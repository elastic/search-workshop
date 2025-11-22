using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Cluster;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace ImportFlights;

public class ElasticsearchClientWrapper
{
    private readonly ElasticsearchClient _client;
    private readonly string _endpoint;
    private readonly ILogger _logger;
    private readonly Dictionary<string, string>? _customHeaders;

    public ElasticsearchClientWrapper(Dictionary<string, object> config, ILogger logger)
    {
        _logger = logger;

        if (!config.TryGetValue("endpoint", out var endpointObj) || endpointObj == null)
        {
            throw new ArgumentException("endpoint is required in the Elasticsearch config");
        }

        _endpoint = endpointObj.ToString()!;

        var settings = new ElasticsearchClientSettings(new Uri(_endpoint));

        // Handle authentication
        if (config.TryGetValue("api_key", out var apiKeyObj) && apiKeyObj != null && !string.IsNullOrWhiteSpace(apiKeyObj.ToString()))
        {
            settings.Authentication(new ApiKey(apiKeyObj.ToString()!));
        }
        else if (config.TryGetValue("user", out var userObj) && config.TryGetValue("password", out var passwordObj))
        {
            var user = userObj?.ToString();
            var password = passwordObj?.ToString();
            if (!string.IsNullOrWhiteSpace(user) && !string.IsNullOrWhiteSpace(password))
            {
                settings.Authentication(new BasicAuthentication(user!, password!));
            }
        }

        // Handle SSL configuration
        var sslVerify = config.TryGetValue("ssl_verify", out var sslVerifyObj) && sslVerifyObj is bool verify ? verify : true;
        if (!sslVerify)
        {
            settings.ServerCertificateValidationCallback((o, certificate, chain, errors) => true);
        }

        if (config.TryGetValue("ca_file", out var caFileObj) && caFileObj != null && !string.IsNullOrWhiteSpace(caFileObj.ToString()))
        {
            // Note: Elasticsearch .NET client doesn't directly support ca_file in the same way
            // This would require custom certificate handling
            _logger.Warn("ca_file configuration is not directly supported by the .NET client");
        }

        if (config.TryGetValue("ca_path", out var caPathObj) && caPathObj != null && !string.IsNullOrWhiteSpace(caPathObj.ToString()))
        {
            _logger.Warn("ca_path configuration is not directly supported by the .NET client");
        }

        // Handle custom headers
        Dictionary<string, string>? defaultHeaders = null;
        if (config.TryGetValue("headers", out var headersObj) && headersObj is Dictionary<object, object> headersDict && headersDict.Count > 0)
        {
            defaultHeaders = new Dictionary<string, string>();
            foreach (var kvp in headersDict)
            {
                var key = kvp.Key?.ToString();
                var value = kvp.Value?.ToString();
                if (!string.IsNullOrWhiteSpace(key) && !string.IsNullOrWhiteSpace(value))
                {
                    defaultHeaders[key] = value;
                }
            }
        }
        _customHeaders = defaultHeaders;

        // Note: Custom headers are parsed and stored, but the Elasticsearch .NET client (v8.15.0)
        // doesn't provide a direct API to configure default headers through ElasticsearchClientSettings.
        // Headers would need to be added per-request using RequestParameters, which requires
        // modifying the low-level transport calls. For now, headers are parsed but not applied.
        // The HeaderHandler class is available for future use if the API supports custom HttpClient configuration.

        _client = new ElasticsearchClient(settings);
    }

    public async Task<bool> IndexExistsAsync(string name)
    {
        try
        {
            var response = await _client.Indices.ExistsAsync(name);
            return response.Exists;
        }
        catch (Exception ex)
        {
            if (ex.Message.Contains("Connection refused") || ex.Message.Contains("timeout"))
            {
                throw new Exception($"Cannot connect to Elasticsearch at {_endpoint}: {ex.Message}. Please check your endpoint configuration and network connectivity.");
            }
            throw new Exception($"Failed to check index existence: {ex.Message}", ex);
        }
    }

    public async Task CreateIndexAsync(string name, Dictionary<string, object> mapping)
    {
        try
        {
            // Serialize mapping to JSON
            var options = new JsonSerializerOptions
            {
                WriteIndented = false,
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            };
            var json = JsonSerializer.Serialize(mapping, options);
            
            // Use the low-level transport API to send raw JSON
            var postData = PostData.String(json);
            
            var response = await _client.Transport.RequestAsync<CreateIndexResponse>(
                Elastic.Transport.HttpMethod.PUT,
                $"/{name}",
                postData);

            if (!response.IsValidResponse)
            {
                if (response.ElasticsearchServerError?.Error?.Type == "resource_already_exists_exception")
                {
                    _logger.Warn($"Index '{name}' already exists (conflict)");
                    return;
                }
                throw new Exception($"Index creation failed: {response.ElasticsearchServerError?.Error?.Reason ?? response.DebugInformation}");
            }

            _logger.Info($"Index '{name}' created");
        }
        catch (Exception ex) when (ex.Message.Contains("resource_already_exists_exception") || ex.Message.Contains("already_exists"))
        {
            _logger.Warn($"Index '{name}' already exists (conflict)");
        }
        catch (Exception ex)
        {
            if (ex.Message.Contains("Connection refused") || ex.Message.Contains("timeout"))
            {
                throw new Exception($"Cannot connect to Elasticsearch at {_endpoint}: {ex.Message}. Please check your endpoint configuration and network connectivity.");
            }
            throw new Exception($"Index creation failed: {ex.Message}", ex);
        }
    }

    public async Task<BulkResponse> BulkAsync(string index, List<Dictionary<string, object?>> documents, bool refresh = false)
    {
        try
        {
            // Build NDJSON payload manually for bulk operations
            var sb = new StringBuilder();
            foreach (var doc in documents)
            {
                // Action line
                sb.AppendLine($"{{\"index\":{{\"_index\":\"{index}\"}}}}");
                // Document line
                var docJson = JsonSerializer.Serialize(doc);
                sb.AppendLine(docJson);
            }

            var payload = sb.ToString();
            var postData = PostData.String(payload);

            // Use the low-level bulk API
            var path = refresh ? "/_bulk?refresh=true" : "/_bulk";
            var response = await _client.Transport.RequestAsync<BulkResponse>(
                Elastic.Transport.HttpMethod.POST,
                path,
                postData);

            if (!response.IsValidResponse)
            {
                throw new Exception($"Bulk request failed: {response.ElasticsearchServerError?.Error?.Reason ?? response.DebugInformation}");
            }

            return response;
        }
        catch (Exception ex)
        {
            throw new Exception($"Bulk request failed: {ex.Message}", ex);
        }
    }

    public async Task<Elastic.Clients.Elasticsearch.Cluster.HealthResponse> GetClusterHealthAsync()
    {
        try
        {
            return await _client.Cluster.HealthAsync();
        }
        catch (Exception ex)
        {
            throw new Exception($"Cluster health request failed: {ex.Message}", ex);
        }
    }

    public async Task<bool> DeleteIndexAsync(string name)
    {
        try
        {
            await _client.Indices.DeleteAsync(name);
            return true;
        }
        catch (Exception ex) when (ex.Message.Contains("index_not_found_exception"))
        {
            return false;
        }
        catch (Exception ex)
        {
            throw new Exception($"Index deletion failed: {ex.Message}", ex);
        }
    }

    public async Task<List<string>> ListIndicesAsync(string pattern = "*")
    {
        try
        {
            var indicesResponse = await _client.Indices.GetAsync(pattern, r => r
                .AllowNoIndices(true)
                .IgnoreUnavailable(true)
                .ExpandWildcards(new[] { ExpandWildcard.All }));

            if (!indicesResponse.IsValidResponse)
            {
                var status = indicesResponse.ApiCallDetails?.HttpStatusCode;
                if (status == 404)
                {
                    return new List<string>();
                }

                throw new Exception(indicesResponse.ElasticsearchServerError?.Error?.Reason ?? indicesResponse.DebugInformation);
            }

            return indicesResponse.Indices.Keys.Select(k => k.ToString()).ToList();
        }
        catch (Exception ex)
        {
            // 404 or no indices found: treat as empty result to match Ruby behavior
            if (ex is TransportException tex && tex.ApiCallDetails?.HttpStatusCode == 404)
            {
                return new List<string>();
            }
            if (ex.Message.Contains("index_not_found_exception") || ex.Message.Contains("404"))
            {
                return new List<string>();
            }

            throw new Exception($"Failed to list indices: {ex.Message}", ex);
        }
    }

    public async Task CreatePipelineAsync(string name, Dictionary<string, object> pipelineConfig)
    {
        var json = JsonSerializer.Serialize(pipelineConfig);
        var postData = PostData.String(json);

        var response = await _client.Transport.RequestAsync<StringResponse>(
            HttpMethod.PUT,
            $"/_ingest/pipeline/{name}",
            postData,
            requestParameters: null,
            openTelemetryData: default,
            cancellationToken: default);

        if (response.ApiCallDetails?.HasSuccessfulStatusCode != true)
        {
            var debug = response.ApiCallDetails?.DebugInformation ?? "Unknown error creating pipeline";
            throw new Exception($"Pipeline creation failed: {debug}");
        }

        _logger.Info($"Pipeline '{name}' created/updated");
    }

    public async Task IndexDocumentAsync(string indexName, Dictionary<string, object> document, string? pipeline = null)
    {
        try
        {
            var response = await _client.IndexAsync(document, r =>
            {
                r.Index(indexName);
                r.Refresh(Refresh.WaitFor);
                if (!string.IsNullOrWhiteSpace(pipeline))
                {
                    r.Pipeline(pipeline);
                }
            });

            if (!response.IsValidResponse)
            {
                throw new Exception(response.ElasticsearchServerError?.Error?.Reason ?? "Unknown indexing error");
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Document indexing failed: {ex.Message}", ex);
        }
    }

    public async Task<List<Dictionary<string, object>>> GetInferenceEndpointsAsync()
    {
        try
        {
            var response = await _client.Transport.RequestAsync<StringResponse>(
                HttpMethod.GET,
                "/_inference/_all",
                postData: null,
                requestParameters: null,
                openTelemetryData: default,
                cancellationToken: default);

            if (response.ApiCallDetails?.HasSuccessfulStatusCode != true)
            {
                var debug = response.ApiCallDetails?.DebugInformation ?? "Unknown error requesting inference endpoints";
                throw new Exception($"Inference endpoints request failed: {debug}");
            }

            var body = response.Body ?? string.Empty;
            var parsed = JsonSerializer.Deserialize<Dictionary<string, object>>(body);
            if (parsed == null)
            {
                return new List<Dictionary<string, object>>();
            }

            if (parsed.TryGetValue("endpoints", out var endpointsObj))
            {
                // endpoints could be list or map
                if (endpointsObj is JsonElement elem)
                {
                    endpointsObj = DeserializeElement(elem);
                }

                if (endpointsObj is List<Dictionary<string, object>> endpointList)
                {
                    return endpointList;
                }

                if (endpointsObj is List<object> endpointObjects)
                {
                    return endpointObjects
                        .Select(obj => obj as Dictionary<string, object> ?? new Dictionary<string, object>())
                        .ToList();
                }

                if (endpointsObj is Dictionary<string, object> endpointMap)
                {
                    return endpointMap
                        .Select(kvp =>
                        {
                            if (kvp.Value is Dictionary<string, object> dict)
                            {
                                dict["inference_id"] = kvp.Key;
                                return dict;
                            }
                            return new Dictionary<string, object>
                            {
                                ["inference_id"] = kvp.Key
                            };
                        })
                        .ToList();
                }
            }

            // Fallback: treat top-level keys (except _shards) as endpoints
            var fallback = new List<Dictionary<string, object>>();
            foreach (var kvp in parsed)
            {
                if (kvp.Key == "_shards")
                {
                    continue;
                }
                if (kvp.Value is Dictionary<string, object> dict)
                {
                    dict["inference_id"] = kvp.Key;
                    fallback.Add(dict);
                }
                else
                {
                    fallback.Add(new Dictionary<string, object> { ["inference_id"] = kvp.Key });
                }
            }

            return fallback;
        }
        catch (Exception ex)
        {
            throw new Exception($"Failed to get inference endpoints: {ex.Message}", ex);
        }
    }

    public async Task<long> CountDocumentsAsync(string indexName)
    {
        try
        {
            var response = await _client.CountAsync(c => c.Index(indexName));
            return response.Count;
        }
        catch (Exception ex)
        {
            _logger.Warn($"Failed to count documents: {ex.Message}");
            return 0;
        }
    }

    private static object? DeserializeElement(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var dict = new Dictionary<string, object>();
                foreach (var prop in element.EnumerateObject())
                {
                    dict[prop.Name] = DeserializeElement(prop.Value)!;
                }
                return dict;
            case JsonValueKind.Array:
                var list = new List<object>();
                foreach (var item in element.EnumerateArray())
                {
                    list.Add(DeserializeElement(item)!);
                }
                return list.Cast<object>().ToList();
            case JsonValueKind.String:
                return element.GetString() ?? string.Empty;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var l))
                {
                    return l;
                }
                if (element.TryGetDouble(out var d))
                {
                    return d;
                }
                return element.GetRawText();
            case JsonValueKind.True:
            case JsonValueKind.False:
                return element.GetBoolean();
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
            default:
                return null;
        }
    }

    public async Task<List<string>> DeleteIndicesByPatternAsync(string pattern)
    {
        var indices = await ListIndicesAsync(pattern);
        if (indices.Count == 0)
        {
            return new List<string>();
        }

        var deleted = new List<string>();
        foreach (var indexName in indices)
        {
            if (await DeleteIndexAsync(indexName))
            {
                deleted.Add(indexName);
            }
        }
        return deleted;
    }
}

// Custom handler to add headers to HTTP requests
internal class HeaderHandler : DelegatingHandler
{
    private readonly Dictionary<string, string> _headers;

    public HeaderHandler(Dictionary<string, string> headers) : base(new HttpClientHandler())
    {
        _headers = headers;
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        foreach (var header in _headers)
        {
            request.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }
        return base.SendAsync(request, cancellationToken);
    }
}
