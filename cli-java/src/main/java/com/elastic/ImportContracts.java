package com.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ImportContracts {
    private static final Logger logger = Logger.getLogger(ImportContracts.class.getName());
    private static final String DEFAULT_CONFIG = "config/elasticsearch.yml";
    private static final String DEFAULT_MAPPING = "config/mappings-contracts.json";
    private static final String DEFAULT_DATA_DIR = "data";
    private static final String DEFAULT_INDEX = "contracts";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        ElasticsearchClientWrapper client = null;
        try {
            Options options = parseOptions(args);
            logger.setLevel(Level.INFO);

            Map<String, Object> config = loadConfig(options.config);
            client = new ElasticsearchClientWrapper(config, logger);

            if (options.status) {
                reportStatus(client);
                return;
            }

            Map<String, Object> mapping = loadMapping(options.mapping);

            String inferenceEndpoint = options.inferenceEndpoint != null ? 
                options.inferenceEndpoint : ContractLoader.DEFAULT_INFERENCE_ENDPOINT;

            ContractLoader loader = new ContractLoader(
                client, mapping, logger, inferenceEndpoint
            );

            // Check Elasticsearch connection
            if (!loader.checkElasticsearch()) {
                logger.severe("Cannot connect to Elasticsearch. Exiting.");
                System.exit(1);
            }

            // Setup phase
            if (!options.ingestOnly) {
                // Check ELSER endpoint
                if (!loader.checkInferenceEndpoint()) {
                    logger.severe("ELSER inference endpoint not found!");
                    logger.severe("Please deploy ELSER via Kibana or API before continuing.");
                    logger.severe("See: Management → Machine Learning → Trained Models → ELSER → Deploy");
                    System.exit(1);
                }

                // Create pipeline
                if (!loader.createPipeline()) {
                    logger.severe("Failed to create pipeline. Exiting.");
                    System.exit(1);
                }

                // Create index (will delete existing one if present)
                if (!loader.createIndex()) {
                    logger.severe("Failed to create index. Exiting.");
                    System.exit(1);
                }
            }

            // Ingestion phase
            if (!options.setupOnly) {
                long ingestionStartTime = System.currentTimeMillis();

                String pdfPath = options.pdfPath != null ? 
                    resolvePath(options.pdfPath) : resolvePath(DEFAULT_DATA_DIR);

                if (!loader.ingestPdfs(pdfPath)) {
                    logger.severe("PDF ingestion had errors.");
                    System.exit(1);
                }

                long ingestionEndTime = System.currentTimeMillis();
                double elapsedSeconds = (ingestionEndTime - ingestionStartTime) / 1000.0;
                logger.info("Total ingestion time: " + String.format("%.2f", elapsedSeconds) + " seconds");

                // Verify ingestion
                loader.verifyIngestion();
            }
        } catch (Exception e) {
            logger.severe("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            // Close the Elasticsearch client to release resources
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    logger.warning("Failed to close Elasticsearch client: " + e.getMessage());
                }
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            long minutes = duration / 60000;
            double seconds = (duration % 60000) / 1000.0;
            if (minutes > 0) {
                System.out.println("\nTotal time: " + minutes + "m " + String.format("%.2f", seconds) + "s");
            } else {
                System.out.println("\nTotal time: " + String.format("%.2f", seconds) + "s");
            }
        }
    }

    private static Options parseOptions(String[] args) {
        Options options = new Options();
        options.config = DEFAULT_CONFIG;
        options.mapping = DEFAULT_MAPPING;
        options.dataDir = DEFAULT_DATA_DIR;
        options.index = DEFAULT_INDEX;
        options.setupOnly = false;
        options.ingestOnly = false;
        options.inferenceEndpoint = null;
        options.status = false;
        options.pdfPath = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "-c":
                case "--config":
                    if (i + 1 < args.length) {
                        options.config = args[++i];
                    }
                    break;
                case "-m":
                case "--mapping":
                    if (i + 1 < args.length) {
                        options.mapping = args[++i];
                    }
                    break;
                case "--pdf-path":
                    if (i + 1 < args.length) {
                        options.pdfPath = args[++i];
                    }
                    break;
                case "--setup-only":
                    options.setupOnly = true;
                    break;
                case "--ingest-only":
                    options.ingestOnly = true;
                    break;
                case "--inference-endpoint":
                    if (i + 1 < args.length) {
                        options.inferenceEndpoint = args[++i];
                    }
                    break;
                case "--status":
                    options.status = true;
                    break;
                case "-h":
                case "--help":
                    printHelp();
                    System.exit(0);
                    break;
                default:
                    logger.warning("Unknown option: " + arg);
                    break;
            }
        }

        // Validation
        if (options.setupOnly && options.ingestOnly) {
            System.err.println("Cannot use --setup-only and --ingest-only together");
            System.exit(1);
        }

        return options;
    }

    private static void printHelp() {
        System.out.println("Usage: import_contracts [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -c, --config PATH          Path to Elasticsearch config YAML (default: config/elasticsearch.yml)");
        System.out.println("  -m, --mapping PATH         Path to mappings JSON (default: config/mappings-contracts.json)");
        System.out.println("  --pdf-path PATH            Path to PDF file or directory containing PDFs (default: data)");
        System.out.println("  --setup-only               Only setup infrastructure (pipeline and index), skip PDF ingestion");
        System.out.println("  --ingest-only              Skip setup, only ingest PDFs (assumes infrastructure exists)");
        System.out.println("  --inference-endpoint NAME  Inference endpoint ID (default: .elser-2-elastic, will auto-detect if not found)");
        System.out.println("  --status                   Test connection and print cluster health status");
        System.out.println("  -h, --help                 Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Setup and ingest PDFs from default location");
        System.out.println("  java -cp target/import-flights-1.0.0.jar com.elastic.ImportContracts");
        System.out.println();
        System.out.println("  # Setup and ingest PDFs from specific directory");
        System.out.println("  java -cp target/import-flights-1.0.0.jar com.elastic.ImportContracts --pdf-path /path/to/pdfs");
        System.out.println();
        System.out.println("  # Only setup infrastructure (skip PDF ingestion)");
        System.out.println("  java -cp target/import-flights-1.0.0.jar com.elastic.ImportContracts --setup-only");
        System.out.println();
        System.out.println("  # Skip setup and only ingest PDFs");
        System.out.println("  java -cp target/import-flights-1.0.0.jar com.elastic.ImportContracts --ingest-only");
    }

    private static String resolvePath(String path) {
        if (path == null) {
            return null;
        }

        Path pathObj = Paths.get(path);
        
        // If path is absolute, use as-is
        if (pathObj.isAbsolute()) {
            return pathObj.toString();
        }
        
        // Try relative to current directory first (if it exists)
        if (Files.exists(pathObj)) {
            return pathObj.toAbsolutePath().toString();
        }
        
        // Try relative to workspace root (one level up from current directory)
        Path currentDir = Paths.get(System.getProperty("user.dir"));
        Path workspaceRoot = currentDir.getParent();
        if (workspaceRoot != null) {
            Path candidate = workspaceRoot.resolve(path);
            return candidate.toAbsolutePath().toString();
        }

        // Return resolved path even if file doesn't exist (for optional files)
        return pathObj.toAbsolutePath().toString();
    }

    private static Map<String, Object> loadConfig(String path) throws IOException {
        String resolvedPath = resolvePath(path);
        File file = new File(resolvedPath);
        if (!file.exists()) {
            throw new FileNotFoundException("Config file not found: " + path + " (tried: " + resolvedPath + ")");
        }

        Yaml yaml = new Yaml();
        try (FileInputStream inputStream = new FileInputStream(file)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> config = (Map<String, Object>) yaml.load(inputStream);
            return config != null ? config : new HashMap<>();
        }
    }

    private static Map<String, Object> loadMapping(String path) throws IOException {
        String resolvedPath = resolvePath(path);
        File file = new File(resolvedPath);
        if (!file.exists()) {
            throw new FileNotFoundException("Mapping file not found: " + path + " (tried: " + resolvedPath + ")");
        }

        ObjectMapper mapper = new ObjectMapper();
        try (FileInputStream inputStream = new FileInputStream(file)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mapping = mapper.readValue(inputStream, Map.class);
            return mapping;
        }
    }

    private static void reportStatus(ElasticsearchClientWrapper client) throws IOException {
        try {
            co.elastic.clients.elasticsearch.cluster.HealthResponse status = client.clusterHealth();
            logger.info("Cluster status: " + status.status());
            logger.info("Active shards: " + status.activeShards() + ", node count: " + status.numberOfNodes());
        } catch (Exception e) {
            logger.severe("Failed to retrieve cluster status: " + e.getMessage());
            System.exit(1);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.warning("Failed to close Elasticsearch client: " + e.getMessage());
            }
        }
    }

    private static class Options {
        String config;
        String mapping;
        String dataDir;
        String index;
        boolean setupOnly;
        boolean ingestOnly;
        String inferenceEndpoint;
        boolean status;
        String pdfPath;
    }
}
