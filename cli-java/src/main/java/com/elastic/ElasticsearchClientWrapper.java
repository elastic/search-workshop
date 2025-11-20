package com.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ElasticsearchClientWrapper {
    private final String endpoint;
    private final Logger logger;
    private final ElasticsearchClient client;
    private final RestClient restClient;

    public ElasticsearchClientWrapper(Map<String, Object> config, Logger logger) {
        this.logger = logger;
        
        String endpoint = (String) config.get("endpoint");
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("endpoint is required in the Elasticsearch config");
        }
        this.endpoint = endpoint;

        ClientPair pair = buildClient(config, endpoint);
        this.client = pair.client;
        this.restClient = pair.restClient;
    }
    
    private static class ClientPair {
        final ElasticsearchClient client;
        final RestClient restClient;
        
        ClientPair(ElasticsearchClient client, RestClient restClient) {
            this.client = client;
            this.restClient = restClient;
        }
    }

    public boolean indexExists(String name) throws IOException {
        try {
            ExistsRequest request = ExistsRequest.of(e -> e.index(name));
            return client.indices().exists(request).value();
        } catch (ElasticsearchException e) {
            String message = e.getMessage();
            if (message != null && (message.contains("Connection refused") || message.contains("timeout"))) {
                throw new IOException("Cannot connect to Elasticsearch at " + endpoint + ": " + message + 
                    ". Please check your endpoint configuration and network connectivity.", e);
            }
            throw new IOException("Failed to check index existence: " + message, e);
        }
    }

    public void createIndex(String name, Map<String, Object> mapping) throws IOException {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            String mappingJson = mapper.writeValueAsString(mapping);
            CreateIndexRequest request = CreateIndexRequest.of(i -> i
                .index(name)
                .withJson(new java.io.ByteArrayInputStream(mappingJson.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            );
            client.indices().create(request);
            logger.info("Index '" + name + "' created");
        } catch (ElasticsearchException e) {
            if (e.status() == 400 && e.getMessage() != null && e.getMessage().contains("resource_already_exists")) {
                logger.warning("Index '" + name + "' already exists (conflict)");
            } else {
                String message = e.getMessage();
                if (message != null && (message.contains("Connection refused") || message.contains("timeout"))) {
                    throw new IOException("Cannot connect to Elasticsearch at " + endpoint + ": " + message + 
                        ". Please check your endpoint configuration and network connectivity.", e);
                }
                throw new IOException("Index creation failed: " + message, e);
            }
        }
    }

    public void bulk(String index, String payload, boolean refresh) throws IOException {
        try {
            // Use low-level REST client for bulk operations with raw NDJSON
            org.elasticsearch.client.Request request = new org.elasticsearch.client.Request("POST", "/_bulk");
            
            // Add refresh parameter if needed
            if (refresh) {
                request.addParameter("refresh", "true");
            }
            
            // Set the NDJSON payload as the request body
            request.setEntity(new org.apache.http.entity.StringEntity(
                payload,
                org.apache.http.entity.ContentType.create("application/x-ndjson", java.nio.charset.StandardCharsets.UTF_8)));
            
            org.elasticsearch.client.Response response = restClient.performRequest(request);
            
            // Parse response
            try (java.io.InputStream responseStream = response.getEntity().getContent();
                 java.io.BufferedReader reader = new java.io.BufferedReader(
                     new java.io.InputStreamReader(responseStream, java.nio.charset.StandardCharsets.UTF_8))) {
                
                StringBuilder responseBody = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    responseBody.append(line).append("\n");
                }
                
                // Parse JSON response to check for errors
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> result = mapper.readValue(responseBody.toString(), Map.class);
                
                Boolean errors = (Boolean) result.get("errors");
                if (errors != null && errors) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> items = (List<Map<String, Object>>) result.get("items");
                    if (items != null) {
                        StringBuilder errorMsg = new StringBuilder("Bulk indexing reported errors:");
                        int errorCount = 0;
                        for (Map<String, Object> item : items) {
                            Map<String, Object> indexObj = (Map<String, Object>) item.get("index");
                            if (indexObj != null) {
                                Map<String, Object> error = (Map<String, Object>) indexObj.get("error");
                                if (error != null && errorCount < 5) {
                                    String reason = (String) error.get("reason");
                                    if (reason != null) {
                                        errorMsg.append("\n  ").append(reason);
                                        errorCount++;
                                    }
                                }
                            }
                        }
                        throw new IOException(errorMsg.toString());
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw e;
            }
            throw new IOException("Bulk request failed: " + e.getMessage(), e);
        }
    }

    public HealthResponse clusterHealth() throws IOException {
        try {
            return client.cluster().health();
        } catch (ElasticsearchException e) {
            throw new IOException("Cluster health request failed: " + e.getMessage(), e);
        }
    }

    public boolean deleteIndex(String name) throws IOException {
        try {
            DeleteIndexRequest request = DeleteIndexRequest.of(d -> d.index(name));
            client.indices().delete(request);
            return true;
        } catch (ElasticsearchException e) {
            if (e.status() == 404) {
                return false;
            }
            throw new IOException("Index deletion failed: " + e.getMessage(), e);
        }
    }

    public List<String> listIndices(String pattern) throws IOException {
        try {
            IndicesResponse response = client.cat().indices(i -> i
                .index(pattern)
            );
            
            return response.valueBody().stream()
                .map(idx -> idx.index())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        } catch (ElasticsearchException e) {
            throw new IOException("Failed to list indices: " + e.getMessage(), e);
        }
    }

    public List<String> deleteIndicesByPattern(String pattern) throws IOException {
        List<String> indices = listIndices(pattern);
        if (indices.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> deleted = new ArrayList<>();
        for (String indexName : indices) {
            if (deleteIndex(indexName)) {
                deleted.add(indexName);
            }
        }
        return deleted;
    }

    public ElasticsearchClient getClient() {
        return client;
    }

    public void close() throws IOException {
        if (restClient != null) {
            restClient.close();
        }
    }

    private ClientPair buildClient(Map<String, Object> config, String endpoint) {
        try {
            URI uri = URI.create(endpoint);
            String scheme = uri.getScheme() != null ? uri.getScheme() : "http";
            String host = uri.getHost() != null ? uri.getHost() : "localhost";
            int port = uri.getPort() != -1 ? uri.getPort() : (scheme.equals("https") ? 443 : 9200);

            RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));

            // Handle authentication and SSL in a single callback
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            String apiKey = (String) config.get("api_key");
            String user = (String) config.get("user");
            String password = (String) config.get("password");
            Boolean sslVerify = (Boolean) config.getOrDefault("ssl_verify", true);
            @SuppressWarnings("unchecked")
            Map<String, String> headers = (Map<String, String>) config.get("headers");

            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                // Handle authentication
                if (apiKey != null && !apiKey.trim().isEmpty()) {
                    // API key authentication via header
                    List<org.apache.http.Header> defaultHeaders = new ArrayList<>();
                    defaultHeaders.add(new org.apache.http.message.BasicHeader("Authorization", "ApiKey " + apiKey));
                    httpClientBuilder.setDefaultHeaders(defaultHeaders);
                } else if (user != null && password != null) {
                    credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(user, password)
                    );
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }

                // Handle SSL configuration
                if (scheme.equals("https") && !sslVerify) {
                    try {
                        SSLContext sslContext = SSLContext.getInstance("TLS");
                        sslContext.init(null, new TrustManager[]{
                            new X509TrustManager() {
                                public X509Certificate[] getAcceptedIssuers() { return null; }
                                public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                                public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                            }
                        }, new java.security.SecureRandom());
                        httpClientBuilder.setSSLContext(sslContext);
                        httpClientBuilder.setSSLHostnameVerifier((hostname, session) -> true);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to configure SSL", e);
                    }
                }

                // Handle custom headers
                if (headers != null && !headers.isEmpty()) {
                    List<org.apache.http.Header> defaultHeaders = new ArrayList<>();
                    if (apiKey != null && !apiKey.trim().isEmpty()) {
                        defaultHeaders.add(new org.apache.http.message.BasicHeader("Authorization", "ApiKey " + apiKey));
                    }
                    for (Map.Entry<String, String> entry : headers.entrySet()) {
                        defaultHeaders.add(new org.apache.http.message.BasicHeader(entry.getKey(), entry.getValue()));
                    }
                    httpClientBuilder.setDefaultHeaders(defaultHeaders);
                }

                return httpClientBuilder;
            });

            RestClient restClient = builder.build();
            RestClientTransport transport = new RestClientTransport(restClient, 
                new co.elastic.clients.json.jackson.JacksonJsonpMapper());
            
            ElasticsearchClient esClient = new ElasticsearchClient(transport);
            return new ClientPair(esClient, restClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build Elasticsearch client", e);
        }
    }
}
