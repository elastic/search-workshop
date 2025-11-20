package com.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportFlights {
    private static final Logger logger = Logger.getLogger(ImportFlights.class.getName());
    private static final String DEFAULT_CONFIG = "config/elasticsearch.yml";
    private static final String DEFAULT_MAPPING = "config/mappings-flights.json";
    private static final String DEFAULT_DATA_DIR = "data";
    private static final String DEFAULT_INDEX = "flights";
    private static final String DEFAULT_AIRPORTS_FILE = "data/airports.csv.gz";
    private static final String DEFAULT_CANCELLATIONS_FILE = "data/cancellations.csv";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        ElasticsearchClientWrapper client = null;
        try {
            Options options = parseOptions(args);
            logger.setLevel(Level.INFO);

            if (options.sample) {
                sampleDocument(options);
                return;
            }

            Map<String, Object> config = loadConfig(options.config);

            client = new ElasticsearchClientWrapper(config, logger);

            if (options.status) {
                reportStatus(client);
                return;
            }

            if (options.deleteIndex) {
                deleteIndicesByPattern(client, options.index);
                return;
            }

            if (options.deleteAll) {
                deleteIndicesByPattern(client, "flights-*");
                return;
            }

            Map<String, Object> mapping = loadMapping(options.mapping);

            String resolvedAirportsFile = resolvePath(options.airportsFile);
            String resolvedCancellationsFile = resolvePath(options.cancellationsFile);

            FlightLoader loader = new FlightLoader(
                client, mapping, options.index, logger,
                options.batchSize, options.refresh,
                resolvedAirportsFile, resolvedCancellationsFile
            );

            List<String> files = filesToProcess(options);
            loader.importFiles(files);
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
        options.batchSize = FlightLoader.BATCH_SIZE;
        options.refresh = false;
        options.status = false;
        options.deleteIndex = false;
        options.deleteAll = false;
        options.sample = false;
        options.airportsFile = DEFAULT_AIRPORTS_FILE;
        options.cancellationsFile = DEFAULT_CANCELLATIONS_FILE;

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
                case "-d":
                case "--data-dir":
                    if (i + 1 < args.length) {
                        options.dataDir = args[++i];
                    }
                    break;
                case "-f":
                case "--file":
                    if (i + 1 < args.length) {
                        options.file = args[++i];
                    }
                    break;
                case "-a":
                case "--all":
                    options.all = true;
                    break;
                case "-g":
                case "--glob":
                    if (i + 1 < args.length) {
                        options.glob = args[++i];
                    }
                    break;
                case "--index":
                    if (i + 1 < args.length) {
                        options.index = args[++i];
                    }
                    break;
                case "--batch-size":
                    if (i + 1 < args.length) {
                        options.batchSize = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--refresh":
                    options.refresh = true;
                    break;
                case "--status":
                    options.status = true;
                    break;
                case "--delete-index":
                    options.deleteIndex = true;
                    break;
                case "--delete-all":
                    options.deleteAll = true;
                    break;
                case "--sample":
                    options.sample = true;
                    break;
                case "-h":
                case "--help":
                    printHelp();
                    System.exit(0);
                    break;
                default:
                    // Handle glob expansion - remaining args might be file paths
                    if (options.glob != null && options.globFiles == null) {
                        options.globFiles = new ArrayList<>();
                        options.globFiles.add(options.glob);
                    }
                    if (options.globFiles != null) {
                        options.globFiles.add(arg);
                    }
                    break;
            }
        }

        // Validation
        if (options.status && (options.deleteIndex || options.deleteAll)) {
            System.err.println("Cannot use --status with --delete-index or --delete-all");
            System.exit(1);
        }

        if (options.deleteIndex && options.deleteAll) {
            System.err.println("Cannot use --delete-index and --delete-all together");
            System.exit(1);
        }

        if (!options.status && !options.deleteIndex && !options.deleteAll && !options.sample) {
            int selectionCount = 0;
            if (options.file != null) selectionCount++;
            if (options.all) selectionCount++;
            if (options.glob != null || options.globFiles != null) selectionCount++;

            if (selectionCount > 1) {
                System.err.println("Cannot use --file, --all, and --glob together (use only one)");
                System.exit(1);
            }

            if (selectionCount == 0) {
                System.err.println("Please provide either --file PATH, --all, or --glob PATTERN");
                System.exit(1);
            }
        }

        return options;
    }

    private static void printHelp() {
        System.out.println("Usage: import_flights [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -c, --config PATH       Path to Elasticsearch config YAML (default: config/elasticsearch.yml)");
        System.out.println("  -m, --mapping PATH      Path to mappings JSON (default: mappings-flights.json)");
        System.out.println("  -d, --data-dir PATH     Directory containing data files (default: data)");
        System.out.println("  -f, --file PATH         Only import the specified file");
        System.out.println("  -a, --all               Import all files found in the data directory");
        System.out.println("  -g, --glob PATTERN      Import files matching the glob pattern");
        System.out.println("  --index NAME            Override index name (default: flights)");
        System.out.println("  --batch-size N          Number of documents per bulk request (default: 500)");
        System.out.println("  --refresh               Request an index refresh after each bulk request");
        System.out.println("  --status                Test connection and print cluster health status");
        System.out.println("  --delete-index          Delete indices matching the index pattern and exit");
        System.out.println("  --delete-all            Delete all flights-* indices and exit");
        System.out.println("  --sample                Print the first document and exit");
        System.out.println("  -h, --help              Show this help message");
    }

    private static String resolvePath(String path) {
        if (path == null) {
            return null;
        }

        Path pathObj = Paths.get(path);
        if (pathObj.isAbsolute() || Files.exists(pathObj)) {
            return pathObj.toAbsolutePath().toString();
        }

        // Try relative to workspace root (one level up from script directory)
        Path scriptDir = Paths.get(System.getProperty("user.dir"));
        Path workspaceRoot = scriptDir.getParent();
        if (workspaceRoot != null) {
            Path candidate = workspaceRoot.resolve(path);
            return candidate.toAbsolutePath().toString();
        }

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

    private static List<String> filesToProcess(Options options) throws IOException {
        String resolvedDataDir = resolvePath(options.dataDir);

        if (options.file != null) {
            return Arrays.asList(resolveFilePath(options.file, resolvedDataDir));
        } else if (options.globFiles != null) {
            return options.globFiles.stream()
                .map(f -> resolveFilePath(f, resolvedDataDir))
                .filter(f -> new File(f).isFile())
                .sorted()
                .collect(Collectors.toList());
        } else if (options.glob != null) {
            List<String> files;
            Path globPath = Paths.get(options.glob);

            if (globPath.isAbsolute()) {
                files = expandGlob(options.glob);
            } else {
                files = expandGlob(options.glob);
                if (files.isEmpty()) {
                    String expandedPattern = Paths.get(resolvedDataDir, options.glob).toString();
                    files = expandGlob(expandedPattern);
                }
            }

            files = files.stream()
                .filter(f -> new File(f).isFile())
                .sorted()
                .collect(Collectors.toList());

            if (files.isEmpty()) {
                throw new IOException("No files found matching pattern: " + options.glob);
            }
            return files;
        } else {
            // Default: find all .zip, .csv, or .csv.gz files in data directory
            List<String> files = new ArrayList<>();
            Path dataPath = Paths.get(resolvedDataDir);
            if (Files.exists(dataPath)) {
                try (Stream<Path> paths = Files.list(dataPath)) {
                    paths.filter(Files::isRegularFile)
                        .map(Path::toString)
                        .filter(f -> f.toLowerCase().endsWith(".zip") ||
                                    f.toLowerCase().endsWith(".csv") ||
                                    f.toLowerCase().endsWith(".csv.gz"))
                        .sorted()
                        .forEach(files::add);
                }
            }
            if (files.isEmpty()) {
                throw new IOException("No .zip, .csv, or .csv.gz files found in " + resolvedDataDir);
            }
            return files;
        }
    }


    private static List<String> expandGlob(String pattern) throws IOException {
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
        Path patternPath = Paths.get(pattern);
        boolean absolutePattern = patternPath.isAbsolute();

        Path start = determineGlobBase(patternPath);
        if (!Files.exists(start)) {
            return Collections.emptyList();
        }

        try (Stream<Path> paths = Files.walk(start)) {
            return paths
                .filter(Files::isRegularFile)
                .filter(p -> {
                    Path candidate = absolutePattern
                        ? p.toAbsolutePath()
                        : Paths.get("").toAbsolutePath().relativize(p.toAbsolutePath());
                    return matcher.matches(candidate.normalize());
                })
                .map(Path::toString)
                .collect(Collectors.toList());
        }
    }

    private static Path determineGlobBase(Path patternPath) {
        Path root = patternPath.getRoot();
        Path base = (root != null) ? root : Paths.get("");

        for (Path part : patternPath) {
            String segment = part.toString();
            if (containsGlob(segment)) {
                break;
            }
            base = base.resolve(part);
        }

        if (base.toString().isEmpty()) {
            return Paths.get(".");
        }
        return base;
    }

    private static boolean containsGlob(String segment) {
        return segment.contains("*") || segment.contains("?") || segment.contains("[") || segment.contains("{");
    }


    private static String resolveFilePath(String path, String dataDir) {
        Path pathObj = Paths.get(path);
        if (pathObj.isAbsolute() && Files.exists(pathObj)) {
            return pathObj.toString();
        }

        // Try relative to resolved data_dir
        Path candidate = Paths.get(dataDir, path);
        if (Files.exists(candidate)) {
            return candidate.toString();
        }

        // Try relative to current directory
        if (Files.exists(pathObj)) {
            return pathObj.toString();
        }

        throw new RuntimeException("File not found: " + path);
    }

    private static void sampleDocument(Options options) throws Exception {
        Map<String, Object> mapping = loadMapping(options.mapping);

        String resolvedAirportsFile = resolvePath(options.airportsFile);
        String resolvedCancellationsFile = resolvePath(options.cancellationsFile);

        FlightLoader loader = new FlightLoader(
            null, mapping, "flights", logger,
            1, false,
            resolvedAirportsFile, resolvedCancellationsFile
        );

        List<String> files = filesToProcess(options);
        if (files.isEmpty()) {
            logger.severe("No files found to sample");
            System.exit(1);
        }

        Map<String, Object> doc = loader.sampleDocument(files.get(0));
        if (doc == null) {
            logger.severe("No document found in file");
            System.exit(1);
        }

        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(doc));
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

    private static void deleteIndicesByPattern(ElasticsearchClientWrapper client, String pattern) throws IOException {
        try {
            String patternWithWildcard = pattern.endsWith("*") ? pattern : pattern + "-*";
            logger.info("Searching for indices matching pattern: " + patternWithWildcard);

            List<String> deleted = client.deleteIndicesByPattern(patternWithWildcard);

            if (deleted.isEmpty()) {
                logger.warning("No indices found matching pattern: " + patternWithWildcard);
            } else {
                logger.info("Deleted " + deleted.size() + " index(es): " + String.join(", ", deleted));
            }
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
        String file;
        boolean all;
        String glob;
        List<String> globFiles;
        String index;
        int batchSize;
        boolean refresh;
        boolean status;
        boolean deleteIndex;
        boolean deleteAll;
        boolean sample;
        String airportsFile;
        String cancellationsFile;
    }
}
