package com.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.Base64;

public class ContractLoader {
    public static final String ES_INDEX = "contracts";
    public static final String PIPELINE_NAME = "pdf_pipeline";
    public static final String DEFAULT_INFERENCE_ENDPOINT = ".elser-2-elastic";

    private final ElasticsearchClientWrapper client;
    private final Map<String, Object> mapping;
    private final Logger logger;
    private String inferenceEndpoint;
    private int indexedCount;

    public ContractLoader(ElasticsearchClientWrapper client, Map<String, Object> mapping,
                         Logger logger, String inferenceEndpoint) {
        this.client = client;
        this.mapping = mapping;
        this.logger = logger;
        this.inferenceEndpoint = inferenceEndpoint != null ? inferenceEndpoint : DEFAULT_INFERENCE_ENDPOINT;
        this.indexedCount = 0;
    }

    public boolean checkElasticsearch() {
        try {
            co.elastic.clients.elasticsearch.cluster.HealthResponse health = client.clusterHealth();
            String clusterName = health.clusterName() != null ? health.clusterName() : "unknown";
            logger.info("Cluster: " + clusterName);
            logger.info("Status: " + health.status());
            return true;
        } catch (Exception e) {
            logger.severe("Connection error: " + e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public boolean checkInferenceEndpoint() {
        try {
            Map<String, Object> response = client.getInferenceEndpoints();
            List<Map<String, Object>> endpoints = (List<Map<String, Object>>) response.get("endpoints");
            if (endpoints == null) {
                endpoints = new ArrayList<>();
            }

            // First, try to find the specified endpoint
            Optional<Map<String, Object>> foundEndpoint = endpoints.stream()
                .filter(ep -> {
                    String id = (String) ep.get("inference_id");
                    return id != null && id.equals(this.inferenceEndpoint);
                })
                .findFirst();

            if (foundEndpoint.isPresent()) {
                logger.info("Found inference endpoint: " + this.inferenceEndpoint);
                return true;
            }

            // Auto-detect ELSER endpoints
            List<Map<String, Object>> elserEndpoints = new ArrayList<>();
            for (Map<String, Object> ep : endpoints) {
                String id = (String) ep.get("inference_id");
                if (id != null && id.toLowerCase().contains("elser")) {
                    elserEndpoints.add(ep);
                }
            }

            if (!elserEndpoints.isEmpty()) {
                // Prefer endpoints starting with .elser-2- or .elser_model_2
                Optional<Map<String, Object>> preferred = elserEndpoints.stream()
                    .filter(ep -> {
                        String id = (String) ep.get("inference_id");
                        return id != null && (id.contains(".elser-2-") || id.contains(".elser_model_2"));
                    })
                    .findFirst();

                if (preferred.isPresent()) {
                    this.inferenceEndpoint = (String) preferred.get().get("inference_id");
                } else {
                    this.inferenceEndpoint = (String) elserEndpoints.get(0).get("inference_id");
                }

                logger.warning("Specified endpoint not found, using auto-detected: " + this.inferenceEndpoint);
                return true;
            }

            logger.severe("Inference endpoint '" + this.inferenceEndpoint + "' not found");
            logger.info("Available endpoints:");
            for (Map<String, Object> ep : endpoints) {
                String id = (String) ep.get("inference_id");
                if (id != null) {
                    logger.info("  - " + id);
                }
            }
            return false;
        } catch (Exception e) {
            logger.warning("Error checking inference endpoint: " + e.getMessage());
            logger.warning("Continuing anyway...");
            return true;
        }
    }

    public boolean createPipeline() {
        try {
            Map<String, Object> pipelineConfig = new HashMap<>();
            pipelineConfig.put("description", "Extract text from PDF - semantic_text field handles chunking and embeddings");

            List<Map<String, Object>> processors = new ArrayList<>();

            // Attachment processor
            Map<String, Object> attachmentProcessor = new HashMap<>();
            Map<String, Object> attachmentConfig = new HashMap<>();
            attachmentConfig.put("field", "data");
            attachmentConfig.put("target_field", "attachment");
            attachmentConfig.put("remove_binary", true);
            attachmentProcessor.put("attachment", attachmentConfig);
            processors.add(attachmentProcessor);

            // Set processor for semantic_content
            Map<String, Object> setProcessor1 = new HashMap<>();
            Map<String, Object> setConfig1 = new HashMap<>();
            setConfig1.put("field", "semantic_content");
            setConfig1.put("copy_from", "attachment.content");
            setConfig1.put("ignore_empty_value", true);
            setProcessor1.put("set", setConfig1);
            processors.add(setProcessor1);

            // Remove processor for data field
            Map<String, Object> removeProcessor = new HashMap<>();
            Map<String, Object> removeConfig = new HashMap<>();
            removeConfig.put("field", "data");
            removeConfig.put("ignore_missing", true);
            removeProcessor.put("remove", removeConfig);
            processors.add(removeProcessor);

            // Set processor for upload_date
            Map<String, Object> setProcessor2 = new HashMap<>();
            Map<String, Object> setConfig2 = new HashMap<>();
            setConfig2.put("field", "upload_date");
            setConfig2.put("value", "{{ _ingest.timestamp }}");
            setProcessor2.put("set", setConfig2);
            processors.add(setProcessor2);

            pipelineConfig.put("processors", processors);

            client.createPipeline(PIPELINE_NAME, pipelineConfig);
            return true;
        } catch (Exception e) {
            logger.severe("Error creating pipeline: " + e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public boolean createIndex() {
        try {
            // Delete index if it exists before creating a new one
            if (client.indexExists(ES_INDEX)) {
                logger.info("Deleting existing index '" + ES_INDEX + "' before import");
                if (client.deleteIndex(ES_INDEX)) {
                    logger.info("Index '" + ES_INDEX + "' deleted");
                } else {
                    logger.warning("Failed to delete index '" + ES_INDEX + "'");
                }
            }

            // Update mapping with detected inference endpoint
            Map<String, Object> mappingWithInference = deepCopy(mapping);
            if (mappingWithInference.containsKey("mappings")) {
                Map<String, Object> mappings = (Map<String, Object>) mappingWithInference.get("mappings");
                if (mappings != null && mappings.containsKey("properties")) {
                    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
                    if (properties != null && properties.containsKey("semantic_content")) {
                        Map<String, Object> semanticContent = (Map<String, Object>) properties.get("semantic_content");
                        if (semanticContent != null) {
                            semanticContent.put("inference_id", this.inferenceEndpoint);
                        }
                    }
                }
            }

            logger.info("Creating index: " + ES_INDEX);
            client.createIndex(ES_INDEX, mappingWithInference);
            logger.info("Successfully created index: " + ES_INDEX);
            return true;
        } catch (Exception e) {
            logger.severe("Error creating index: " + e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deepCopy(Map<String, Object> original) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String json = mapper.writeValueAsString(original);
            return mapper.readValue(json, Map.class);
        } catch (Exception e) {
            logger.warning("Failed to deep copy mapping, using original: " + e.getMessage());
            return new HashMap<>(original);
        }
    }

    private String extractAirlineName(String filename) {
        String filenameLower = filename.toLowerCase();

        if (filenameLower.contains("american")) {
            return "American Airlines";
        } else if (filenameLower.contains("southwest")) {
            return "Southwest";
        } else if (filenameLower.contains("united")) {
            return "United";
        } else if (filenameLower.contains("delta") || filenameLower.contains("dl-")) {
            return "Delta";
        } else {
            return "Unknown";
        }
    }

    private List<Path> getPdfFiles(String path) throws IOException {
        Path pathObj = Paths.get(path);

        if (!Files.exists(pathObj)) {
            logger.severe("Path '" + path + "' does not exist");
            return Collections.emptyList();
        }

        if (Files.isRegularFile(pathObj)) {
            String filename = pathObj.getFileName().toString().toLowerCase();
            if (filename.endsWith(".pdf")) {
                return Collections.singletonList(pathObj);
            } else {
                logger.severe("'" + path + "' is not a PDF file");
                return Collections.emptyList();
            }
        } else if (Files.isDirectory(pathObj)) {
            List<Path> pdfFiles = new ArrayList<>();
            try (Stream<Path> paths = Files.list(pathObj)) {
                paths.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".pdf"))
                    .sorted()
                    .forEach(pdfFiles::add);
            }
            if (pdfFiles.isEmpty()) {
                logger.warning("No PDF files found in directory '" + path + "'");
            }
            return pdfFiles;
        } else {
            return Collections.emptyList();
        }
    }

    private boolean indexPdf(Path pdfPath) {
        String filename = pdfPath.getFileName().toString();
        String airline = extractAirlineName(filename);

        try {
            // Read and encode the PDF
            byte[] pdfData = Files.readAllBytes(pdfPath);
            String encodedPdf = Base64.getEncoder().encodeToString(pdfData);

            // Index the document
            Map<String, Object> document = new HashMap<>();
            document.put("data", encodedPdf);
            document.put("filename", filename);
            document.put("airline", airline);

            client.indexDocument(ES_INDEX, document, PIPELINE_NAME);

            logger.info("Indexed: " + filename + " (airline: " + airline + ")");
            indexedCount++;
            return true;
        } catch (Exception e) {
            logger.severe("Error processing " + filename + ": " + e.getMessage());
            if (e.getCause() != null) {
                logger.severe("Cause: " + e.getCause().getMessage());
            }
            e.printStackTrace();
            return false;
        }
    }

    public boolean ingestPdfs(String pdfPath) {
        try {
            List<Path> pdfFiles = getPdfFiles(pdfPath);

            if (pdfFiles.isEmpty()) {
                logger.severe("No PDF files to process");
                return false;
            }

            logger.info("Processing " + pdfFiles.size() + " PDF file(s)...");

            int successCount = 0;
            int failedCount = 0;

            for (Path pdfFile : pdfFiles) {
                if (indexPdf(pdfFile)) {
                    successCount++;
                } else {
                    failedCount++;
                }
            }

            logger.info("Indexed " + successCount + " of " + pdfFiles.size() + " file(s)");
            if (failedCount > 0) {
                logger.warning("Failed: " + failedCount);
            }

            return failedCount == 0;
        } catch (Exception e) {
            logger.severe("Error ingesting PDFs: " + e.getMessage());
            return false;
        }
    }

    public boolean verifyIngestion() {
        try {
            // Add a small delay to ensure all documents are searchable
            Thread.sleep(1000);
            
            long count = client.countDocuments(ES_INDEX);
            logger.info("Index '" + ES_INDEX + "' contains " + count + " document(s)");
            
            if (count == 0 && indexedCount > 0) {
                logger.warning("Warning: Expected " + indexedCount + " document(s) but count shows 0. " +
                    "Documents may have failed during pipeline processing. Check Elasticsearch logs for details.");
            }
            
            return true;
        } catch (Exception e) {
            logger.warning("Could not verify document count: " + e.getMessage());
            return true;
        }
    }
}
