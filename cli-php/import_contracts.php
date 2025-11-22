#!/usr/bin/env php
<?php

$autoloadPath = __DIR__ . '/vendor/autoload.php';
if (!file_exists($autoloadPath)) {
    fwrite(STDERR, "Error: Composer dependencies not installed. Please run 'composer install' in the cli-php directory.\n");
    exit(1);
}

require_once $autoloadPath;

// Include the ElasticsearchClient class from import_flights.php
require_once __DIR__ . '/import_flights.php';

use Symfony\Component\Yaml\Yaml;
use Http\Discovery\Psr17FactoryDiscovery;

const ES_INDEX = 'contracts';
const PIPELINE_NAME = 'pdf_pipeline';
const DEFAULT_INFERENCE_ENDPOINT = '.elser-2-elastic';

// Extend ElasticsearchClient with contract-specific methods
class ElasticsearchClientContracts extends ElasticsearchClient
{
    public function createPipeline(string $name, array $pipelineConfig): void
    {
        try {
            $this->client->ingest()->putPipeline([
                'id' => $name,
                'body' => $pipelineConfig
            ]);
            $this->logger->info("Pipeline '{$name}' created/updated");
        } catch (\Exception $e) {
            throw new RuntimeException("Pipeline creation failed: " . $e->getMessage());
        }
    }

    public function indexDocument(string $indexName, array $document, ?string $pipeline = null): void
    {
        try {
            $params = [
                'index' => $indexName,
                'body' => $document,
                'refresh' => 'wait_for'
            ];

            if ($pipeline !== null) {
                $params['pipeline'] = $pipeline;
            }

            $response = $this->client->index($params);

            // Check response for errors even if status is OK
            $responseArray = method_exists($response, 'asArray') ? $response->asArray() : (array)$response;
            if (isset($responseArray['error'])) {
                $this->logger->warn("Elasticsearch returned error in response: " . json_encode($responseArray['error']));
                throw new RuntimeException("Document indexing failed: " . json_encode($responseArray['error']));
            }
        } catch (\Exception $e) {
            throw new RuntimeException("Document indexing failed: " . $e->getMessage());
        }
    }

    public function getInferenceEndpoints(): array
    {
        try {
            // Use the official client transport via PSR-7 request (no curl fallback)
            $requestFactory = Psr17FactoryDiscovery::findRequestFactory();
            $request = $requestFactory->createRequest('GET', '/_inference/_all');
            $response = $this->client->sendRequest($request);
            $bodyJson = (string)$response->getBody();
            $body = json_decode($bodyJson, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new RuntimeException('Failed to decode inference endpoints response: ' . json_last_error_msg());
            }

            // Normalize common response shapes to a flat list of endpoint entries
            if (isset($body['endpoints']) && is_array($body['endpoints'])) {
                $endpoints = $body['endpoints'];
                $isList = array_keys($endpoints) === range(0, count($endpoints) - 1);
                if ($isList) {
                    return $endpoints;
                }

                // Map-style { endpoints: { id: { ... }, id2: { ... } } }
                $normalized = [];
                foreach ($endpoints as $key => $value) {
                    $normalized[] = array_merge(['inference_id' => $key], is_array($value) ? $value : []);
                }
                return $normalized;
            }

            // Fallback: treat top-level keys (except _shards) as endpoints
            $normalized = [];
            foreach ($body as $key => $value) {
                if ($key === '_shards') {
                    continue;
                }
                $normalized[] = array_merge(['inference_id' => $key], is_array($value) ? $value : []);
            }
            return $normalized;
        } catch (\Exception $e) {
            // Let the caller decide whether to continue on errors (matches Ruby behavior)
            throw $e;
        }
    }

    public function countDocuments(string $indexName): int
    {
        try {
            $response = $this->client->count(['index' => $indexName]);
            $result = method_exists($response, 'asArray') ? $response->asArray() : (array)$response;
            return (int)($result['count'] ?? 0);
        } catch (\Exception $e) {
            $this->logger->warn("Failed to count documents: " . $e->getMessage());
            return 0;
        }
    }
}

class ContractLoader
{
    private $client;
    private $mapping;
    private $inferenceEndpoint;
    private $logger;
    private $indexedCount = 0;

    public function __construct($client, array $mapping, $logger, ?string $inferenceEndpoint = null)
    {
        $this->client = $client;
        $this->mapping = $mapping;
        $this->logger = $logger;
        $this->inferenceEndpoint = $inferenceEndpoint ?: DEFAULT_INFERENCE_ENDPOINT;
    }

    public function checkElasticsearch(): bool
    {
        try {
            $health = $this->client->clusterHealth();
            $clusterName = $health['cluster_name'] ?? 'unknown';
            $status = $health['status'] ?? 'unknown';
            $this->logger->info("Cluster: {$clusterName}");
            $this->logger->info("Status: {$status}");
            return true;
        } catch (\Exception $e) {
            $this->logger->error("Connection error: " . $e->getMessage());
            return false;
        }
    }

    public function checkInferenceEndpoint(): bool
    {
        try {
            $endpoints = $this->client->getInferenceEndpoints();

            $idFor = function ($ep) {
                foreach (['inference_id', 'endpoint', 'id', 'name'] as $key) {
                    if (isset($ep[$key]) && $ep[$key] !== '') {
                        return $ep[$key];
                    }
                }
                return null;
            };

            // First, try to find the specified endpoint
            foreach ($endpoints as $ep) {
                $id = $idFor($ep);
                if ($id !== null && $id === $this->inferenceEndpoint) {
                    $this->logger->info("Found inference endpoint: {$this->inferenceEndpoint}");
                    return true;
                }
            }

            // Auto-detect ELSER endpoints
            $elserEndpoints = [];
            foreach ($endpoints as $ep) {
                $id = $idFor($ep);
                if ($id !== null && stripos($id, 'elser') !== false) {
                    $elserEndpoints[] = ['id' => $id, 'raw' => $ep];
                }
            }

            if (!empty($elserEndpoints)) {
                // Prefer endpoints starting with .elser-2- or .elser_model_2
                $preferred = [];
                foreach ($elserEndpoints as $ep) {
                    $id = $ep['id'];
                    if (strpos($id, '.elser-2-') !== false || strpos($id, '.elser_model_2') !== false) {
                        $preferred[] = $ep;
                    }
                }

                if (!empty($preferred)) {
                    $this->inferenceEndpoint = $preferred[0]['id'];
                } else {
                    $this->inferenceEndpoint = $elserEndpoints[0]['id'];
                }

                $this->logger->warn("Specified endpoint not found, using auto-detected: {$this->inferenceEndpoint}");
                return true;
            }

            $this->logger->error("Inference endpoint '{$this->inferenceEndpoint}' not found");
            $this->logger->info("Available endpoints:");
            foreach ($endpoints as $ep) {
                $id = $idFor($ep) ?: '<unknown>';
                $this->logger->info("  - {$id}");
            }
            return false;
        } catch (\Exception $e) {
            $this->logger->warn("Error checking inference endpoint: " . $e->getMessage());
            $this->logger->warn("Continuing anyway...");
            return true;
        }
    }

    public function createPipeline(): bool
    {
        $pipelineConfig = [
            'description' => 'Extract text from PDF - semantic_text field handles chunking and embeddings',
            'processors' => [
                [
                    'attachment' => [
                        'field' => 'data',
                        'target_field' => 'attachment',
                        'remove_binary' => true
                    ]
                ],
                [
                    'set' => [
                        'field' => 'semantic_content',
                        'copy_from' => 'attachment.content',
                        'ignore_empty_value' => true
                    ]
                ],
                [
                    'remove' => [
                        'field' => 'data',
                        'ignore_missing' => true
                    ]
                ],
                [
                    'set' => [
                        'field' => 'upload_date',
                        'value' => '{{ _ingest.timestamp }}'
                    ]
                ]
            ]
        ];

        try {
            $this->client->createPipeline(PIPELINE_NAME, $pipelineConfig);
            return true;
        } catch (\Exception $e) {
            $this->logger->error("Error creating pipeline: " . $e->getMessage());
            return false;
        }
    }

    public function createIndex(): bool
    {
        // Delete index if it exists before creating a new one
        try {
            if ($this->client->indexExists(ES_INDEX)) {
                $this->logger->info("Deleting existing index '" . ES_INDEX . "' before import");
                if ($this->client->deleteIndex(ES_INDEX)) {
                    $this->logger->info("Index '" . ES_INDEX . "' deleted");
                } else {
                    $this->logger->warn("Failed to delete index '" . ES_INDEX . "'");
                }
            }
        } catch (\Exception $e) {
            $this->logger->warn("Error checking/deleting index: " . $e->getMessage());
        }

        // Update mapping with detected inference endpoint
        $mappingWithInference = $this->deepCopyArray($this->mapping);
        if (isset($mappingWithInference['mappings']['properties']['semantic_content'])) {
            $mappingWithInference['mappings']['properties']['semantic_content']['inference_id'] = $this->inferenceEndpoint;
        }

        $this->logger->info("Creating index: " . ES_INDEX);
        try {
            $this->client->createIndex(ES_INDEX, $mappingWithInference);
            $this->logger->info("Successfully created index: " . ES_INDEX);
            return true;
        } catch (\Exception $e) {
            $this->logger->error("Error creating index: " . $e->getMessage());
            return false;
        }
    }

    private function deepCopyArray(array $array): array
    {
        return json_decode(json_encode($array), true);
    }

    private function extractAirlineName(string $filename): string
    {
        $filenameLower = strtolower($filename);

        if (strpos($filenameLower, 'american') !== false) {
            return 'American Airlines';
        } elseif (strpos($filenameLower, 'southwest') !== false) {
            return 'Southwest';
        } elseif (strpos($filenameLower, 'united') !== false) {
            return 'United';
        } elseif (strpos($filenameLower, 'delta') !== false || strpos($filenameLower, 'dl-') !== false) {
            return 'Delta';
        }
        return 'Unknown';
    }

    private function getPdfFiles(string $path): array
    {
        if (!file_exists($path)) {
            $this->logger->error("Path '{$path}' does not exist");
            return [];
        }

        if (is_file($path)) {
            if (strtolower(pathinfo($path, PATHINFO_EXTENSION)) === 'pdf') {
                return [$path];
            } else {
                $this->logger->error("'{$path}' is not a PDF file");
                return [];
            }
        } elseif (is_dir($path)) {
            $pdfFiles = [];
            $files = scandir($path);
            foreach ($files as $file) {
                if ($file === '.' || $file === '..') {
                    continue;
                }
                $filePath = $path . '/' . $file;
                if (is_file($filePath) && strtolower(pathinfo($filePath, PATHINFO_EXTENSION)) === 'pdf') {
                    $pdfFiles[] = $filePath;
                }
            }
            sort($pdfFiles);
            if (empty($pdfFiles)) {
                $this->logger->warn("No PDF files found in directory '{$path}'");
            }
            return $pdfFiles;
        }

        return [];
    }

    private function indexPdf(string $pdfPath): bool
    {
        $filename = basename($pdfPath);
        $airline = $this->extractAirlineName($filename);

        try {
            // Read and encode the PDF
            $pdfData = file_get_contents($pdfPath);
            if ($pdfData === false) {
                $this->logger->error("Error reading {$filename}");
                return false;
            }

            $encodedPdf = base64_encode($pdfData);

            // Index the document
            $document = [
                'data' => $encodedPdf,
                'filename' => $filename,
                'airline' => $airline
            ];

            $this->client->indexDocument(ES_INDEX, $document, PIPELINE_NAME);

            $this->logger->info("Indexed: {$filename} (airline: {$airline})");
            $this->indexedCount++;
            return true;
        } catch (\Exception $e) {
            $this->logger->error("Error processing {$filename}: " . $e->getMessage());
            return false;
        }
    }

    public function ingestPdfs(string $pdfPath): bool
    {
        $pdfFiles = $this->getPdfFiles($pdfPath);

        if (empty($pdfFiles)) {
            $this->logger->error('No PDF files to process');
            return false;
        }

        $this->logger->info("Processing " . count($pdfFiles) . " PDF file(s)...");

        $successCount = 0;
        $failedCount = 0;

        foreach ($pdfFiles as $pdfFile) {
            if ($this->indexPdf($pdfFile)) {
                $successCount++;
            } else {
                $failedCount++;
            }
        }

        $this->logger->info("Indexed {$successCount} of " . count($pdfFiles) . " file(s)");
        if ($failedCount > 0) {
            $this->logger->warn("Failed: {$failedCount}");
        }

        return $failedCount === 0;
    }

    public function verifyIngestion(): void
    {
        sleep(1); // Small delay to ensure documents are searchable

        try {
            $count = $this->client->countDocuments(ES_INDEX);
            $this->logger->info("Index '" . ES_INDEX . "' contains {$count} document(s)");

            if ($count === 0 && $this->indexedCount > 0) {
                $this->logger->warn("Warning: Expected {$this->indexedCount} document(s) but count shows 0. Documents may have failed during pipeline processing.");
            }
        } catch (\Exception $e) {
            $this->logger->warn("Could not verify document count: " . $e->getMessage());
        }
    }
}

function parseOptionsContracts(array $argv): array
{
    $options = [
        'config' => 'config/elasticsearch.yml',
        'mapping' => 'config/mappings-contracts.json',
        'pdf_path' => null,
        'setup_only' => false,
        'ingest_only' => false,
        'inference_endpoint' => null,
        'status' => false
    ];

    $i = 1;
    while ($i < count($argv)) {
        $arg = $argv[$i];
        switch ($arg) {
            case '-c':
            case '--config':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --config requires a path\n");
                    exit(1);
                }
                $options['config'] = $argv[$i];
                break;
            case '-m':
            case '--mapping':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --mapping requires a path\n");
                    exit(1);
                }
                $options['mapping'] = $argv[$i];
                break;
            case '--pdf-path':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --pdf-path requires a path\n");
                    exit(1);
                }
                $options['pdf_path'] = $argv[$i];
                break;
            case '--setup-only':
                $options['setup_only'] = true;
                break;
            case '--ingest-only':
                $options['ingest_only'] = true;
                break;
            case '--inference-endpoint':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --inference-endpoint requires a name\n");
                    exit(1);
                }
                $options['inference_endpoint'] = $argv[$i];
                break;
            case '--status':
                $options['status'] = true;
                break;
            case '-h':
            case '--help':
                showHelpContracts();
                exit(0);
            default:
                if (strpos($arg, '-') === 0) {
                    fwrite(STDERR, "Unknown option: {$arg}\n");
                    showHelpContracts();
                    exit(1);
                }
                break;
        }
        $i++;
    }

    // Validation
    if ($options['setup_only'] && $options['ingest_only']) {
        fwrite(STDERR, "Cannot use --setup-only and --ingest-only together\n");
        exit(1);
    }

    return $options;
}

function showHelpContracts(): void
{
    echo "Usage: import_contracts.php [options]\n\n";
    echo "Options:\n";
    echo "  -c, --config PATH          Path to Elasticsearch config YAML (default: config/elasticsearch.yml)\n";
    echo "  -m, --mapping PATH         Path to mappings JSON (default: config/mappings-contracts.json)\n";
    echo "  --pdf-path PATH            Path to PDF file or directory containing PDFs (default: data)\n";
    echo "  --setup-only               Only setup infrastructure (pipeline and index), skip PDF ingestion\n";
    echo "  --ingest-only              Skip setup, only ingest PDFs (assumes infrastructure exists)\n";
    echo "  --inference-endpoint NAME   Inference endpoint ID (default: .elser-2-elastic, will auto-detect if not found)\n";
    echo "  --status                   Test connection and print cluster health status\n";
    echo "  -h, --help                 Show this help message\n";
    echo "\n";
    echo "Examples:\n";
    echo "  # Setup and ingest PDFs from default location\n";
    echo "  php import_contracts.php\n";
    echo "\n";
    echo "  # Setup and ingest PDFs from specific directory\n";
    echo "  php import_contracts.php --pdf-path /path/to/pdfs\n";
    echo "\n";
    echo "  # Only setup infrastructure (skip PDF ingestion)\n";
    echo "  php import_contracts.php --setup-only\n";
    echo "\n";
    echo "  # Skip setup and only ingest PDFs\n";
    echo "  php import_contracts.php --ingest-only\n";
}

function resolvePathContracts(string $path): string
{
    // If path is absolute, use as-is
    if (strpos($path, '/') === 0 || (PHP_OS_FAMILY === 'Windows' && preg_match('/^[A-Z]:/i', $path))) {
        return $path;
    }

    // Try relative to current directory first (if it exists)
    if (file_exists($path)) {
        return $path;
    }

    // Try relative to workspace root (one level up from script directory)
    $scriptDir = dirname(__FILE__);
    $workspaceRoot = dirname($scriptDir);
    $candidate = $workspaceRoot . '/' . $path;

    // Return resolved path even if file doesn't exist (for optional files)
    return $candidate;
}

function loadConfigContracts(string $path): array
{
    $resolvedPath = resolvePathContracts($path);
    if (!file_exists($resolvedPath)) {
        throw new RuntimeException("Config file not found: {$path} (tried: {$resolvedPath})");
    }

    $content = file_get_contents($resolvedPath);
    return Yaml::parse($content) ?: [];
}

function loadMappingContracts(string $path): array
{
    $resolvedPath = resolvePathContracts($path);
    if (!file_exists($resolvedPath)) {
        throw new RuntimeException("Mapping file not found: {$path} (tried: {$resolvedPath})");
    }

    $content = file_get_contents($resolvedPath);
    $mapping = json_decode($content, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new RuntimeException("Invalid JSON in mapping file: " . json_last_error_msg());
    }
    return $mapping;
}

function reportStatusContracts($client, $logger): void
{
    try {
        $status = $client->clusterHealth();
        $logger->info("Cluster status: " . ($status['status'] ?? 'unknown'));
        $logger->info("Active shards: " . ($status['active_shards'] ?? 0) . ", node count: " . ($status['number_of_nodes'] ?? 0));
    } catch (\Exception $e) {
        $logger->error("Failed to retrieve cluster status: " . $e->getMessage());
        exit(1);
    }
}

function mainContracts(array $argv): void
{
    $options = parseOptionsContracts($argv);
    $logger = new SimpleLogger();

    $config = loadConfigContracts($options['config']);
    $client = new ElasticsearchClientContracts($config, $logger);

    if ($options['status']) {
        reportStatusContracts($client, $logger);
        return;
    }

    $mapping = loadMappingContracts($options['mapping']);

    $inferenceEndpoint = $options['inference_endpoint'] ?: DEFAULT_INFERENCE_ENDPOINT;

    $loader = new ContractLoader($client, $mapping, $logger, $inferenceEndpoint);

    // Check Elasticsearch connection
    if (!$loader->checkElasticsearch()) {
        $logger->error("Cannot connect to Elasticsearch. Exiting.");
        exit(1);
    }

    // Setup phase
    if (!$options['ingest_only']) {
        // Check ELSER endpoint
        if (!$loader->checkInferenceEndpoint()) {
            $logger->error("ELSER inference endpoint not found!");
            $logger->error("Please deploy ELSER via Kibana or API before continuing.");
            $logger->error("See: Management → Machine Learning → Trained Models → ELSER → Deploy");
            exit(1);
        }

        // Create pipeline
        if (!$loader->createPipeline()) {
            $logger->error("Failed to create pipeline. Exiting.");
            exit(1);
        }

        // Create index (will delete existing one if present)
        if (!$loader->createIndex()) {
            $logger->error("Failed to create index. Exiting.");
            exit(1);
        }
    }

    // Ingestion phase
    if (!$options['setup_only']) {
        $ingestionStart = microtime(true);

        $pdfPath = $options['pdf_path'] ?: resolvePathContracts('data');

        if (!$loader->ingestPdfs($pdfPath)) {
            $logger->error("PDF ingestion had errors.");
            exit(1);
        }

        $elapsed = microtime(true) - $ingestionStart;
        $logger->info("Total ingestion time: " . number_format($elapsed, 2) . " seconds");

        // Verify ingestion
        $loader->verifyIngestion();
    }
}

if (php_sapi_name() === 'cli') {
    $startTime = microtime(true);
    try {
        mainContracts($argv);
    } finally {
        $endTime = microtime(true);
        $duration = $endTime - $startTime;
        $minutes = (int)floor($duration / 60);
        $seconds = round(fmod($duration, 60), 2);
        if ($minutes > 0) {
            echo "\nTotal time: {$minutes}m {$seconds}s\n";
        } else {
            echo "\nTotal time: {$seconds}s\n";
        }
    }
}
