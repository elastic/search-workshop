#!/usr/bin/env php
<?php

$autoloadPath = __DIR__ . '/vendor/autoload.php';
if (!file_exists($autoloadPath)) {
    fwrite(STDERR, "Error: Composer dependencies not installed. Please run 'composer install' in the cli-php directory.\n");
    exit(1);
}

require_once $autoloadPath;

use Elastic\Elasticsearch\ClientBuilder;
use Elastic\Elasticsearch\Exception\ClientResponseException;
use Symfony\Component\Yaml\Yaml;

class ElasticsearchClient
{
    protected $endpoint;
    protected $logger;
    protected $client;
    protected $config; // Store config for authenticated requests (protected for child classes)

    public function __construct(array $config, $logger)
    {
        if (!isset($config['endpoint'])) {
            throw new InvalidArgumentException('endpoint is required in the Elasticsearch config');
        }

        $this->endpoint = $config['endpoint'];
        $this->logger = $logger;
        $this->config = $config;
        $this->client = $this->buildClient($config, $config['endpoint']);
    }

    public function indexExists(string $name): bool
    {
        try {
            $response = $this->client->indices()->exists(['index' => $name]);
            // PHP client returns a response object; convert to boolean explicitly.
            return method_exists($response, 'asBool') ? $response->asBool() : (bool)$response;
        } catch (ClientResponseException $e) {
            // A 404 from the exists API should be treated as "does not exist"
            if ($e->getResponse()->getStatusCode() === 404) {
                return false;
            }
            throw $e;
        } catch (\Exception $e) {
            $message = $e->getMessage();
            if (strpos($message, 'Connection refused') !== false || strpos($message, 'timeout') !== false) {
                throw new RuntimeException("Cannot connect to Elasticsearch at {$this->endpoint}: {$message}. Please check your endpoint configuration and network connectivity.");
            }
            throw new RuntimeException("Failed to check index existence: {$message}");
        }
    }

    public function createIndex(string $name, array $mapping): void
    {
        try {
            $this->client->indices()->create([
                'index' => $name,
                'body' => $mapping
            ]);
            $this->logger->info("Index '{$name}' created");
        } catch (ClientResponseException $e) {
            $statusCode = $e->getResponse()->getStatusCode();
            if ($statusCode === 409) {
                $this->logger->warn("Index '{$name}' already exists (conflict)");
                return;
            }
            throw $e;
        } catch (\Exception $e) {
            $message = $e->getMessage();
            if (strpos($message, 'Connection refused') !== false || strpos($message, 'timeout') !== false) {
                throw new RuntimeException("Cannot connect to Elasticsearch at {$this->endpoint}: {$message}. Please check your endpoint configuration and network connectivity.");
            }
            throw new RuntimeException("Index creation failed: {$message}");
        }
    }

    public function bulk(string $index, string $payload, bool $refresh = false): array
    {
        try {
            $params = [
                'body' => $payload,
                'refresh' => $refresh
            ];
            $response = $this->client->bulk($params);
            return method_exists($response, 'asArray') ? $response->asArray() : (array)$response;
        } catch (\Exception $e) {
            throw new RuntimeException("Bulk request failed: " . $e->getMessage());
        }
    }

    public function clusterHealth(): array
    {
        try {
            $response = $this->client->cluster()->health();
            return method_exists($response, 'asArray') ? $response->asArray() : (array)$response;
        } catch (\Exception $e) {
            throw new RuntimeException("Cluster health request failed: " . $e->getMessage());
        }
    }

    public function deleteIndex(string $name): bool
    {
        try {
            $this->client->indices()->delete(['index' => $name]);
            return true;
        } catch (ClientResponseException $e) {
            $statusCode = $e->getResponse()->getStatusCode();
            if ($statusCode === 404) {
                return false;
            }
            throw $e;
        } catch (\Exception $e) {
            throw new RuntimeException("Index deletion failed: " . $e->getMessage());
        }
    }

    public function listIndices(string $pattern = '*'): array
    {
        try {
            $response = $this->client->cat()->indices([
                'format' => 'json',
                'index' => $pattern
            ]);
            $data = method_exists($response, 'asArray') ? $response->asArray() : (array)$response;
            return array_map(function ($idx) {
                return $idx['index'];
            }, array_filter($data, function ($idx) {
                return isset($idx['index']);
            }));
        } catch (\Exception $e) {
            throw new RuntimeException("Failed to list indices: " . $e->getMessage());
        }
    }

    public function deleteIndicesByPattern(string $pattern): array
    {
        $indices = $this->listIndices($pattern);
        if (empty($indices)) {
            return [];
        }

        $deleted = [];
        foreach ($indices as $indexName) {
            if ($this->deleteIndex($indexName)) {
                $deleted[] = $indexName;
            }
        }
        return $deleted;
    }

    private function buildClient(array $config, string $endpoint)
    {
        $clientBuilder = ClientBuilder::create();

        // Set endpoint
        $hosts = [$endpoint];
        $clientBuilder->setHosts($hosts);

        // Handle authentication
        if (!empty($config['api_key'])) {
            $clientBuilder->setApiKey($config['api_key']);
        } elseif (!empty($config['user']) && !empty($config['password'])) {
            $clientBuilder->setBasicAuthentication($config['user'], $config['password']);
        }

        // Handle SSL configuration
        if (isset($config['ssl_verify'])) {
            $sslOptions = [
                'verify' => $config['ssl_verify']
            ];
            if (!empty($config['ca_file'])) {
                $sslOptions['cert'] = $config['ca_file'];
            }
            if (!empty($config['ca_path'])) {
                $sslOptions['cert_path'] = $config['ca_path'];
            }
            $clientBuilder->setSSLVerification($config['ssl_verify']);
        }

        // Handle custom headers
        if (!empty($config['headers']) && is_array($config['headers'])) {
            // Note: The PHP client doesn't have direct header support in ClientBuilder
            // We'll need to use a custom handler or set them per request
            // For now, we'll skip this as it's less common
        }

        return $clientBuilder->build();
    }
}

function presence($value)
{
    if ($value === null) {
        return null;
    }
    $trimmed = trim((string)$value);
    return $trimmed === '' ? null : $trimmed;
}

function readCsvRow($handle)
{
    // Specify the escape character to avoid PHP 8.4 deprecation warnings.
    return fgetcsv($handle, 0, ',', '"', '\\');
}

class AirportLookup
{
    private $logger;
    private $airports = [];

    public function __construct(?string $airportsFile, $logger)
    {
        $this->logger = $logger;
        if ($airportsFile && file_exists($airportsFile)) {
            $this->loadAirports($airportsFile);
        }
    }

    public function lookupCoordinates(?string $iataCode): ?string
    {
        if (empty($iataCode)) {
            return null;
        }

        $airport = $this->airports[strtoupper($iataCode)] ?? null;
        if (!$airport) {
            return null;
        }

        return "{$airport['lat']},{$airport['lon']}";
    }

    private function loadAirports(string $filePath): void
    {
        $this->logger->info("Loading airports from {$filePath}");

        $count = 0;
        // Use gzip stream wrapper so we can use fgetcsv directly
        $file = fopen("compress.zlib://{$filePath}", 'r');
        if (!$file) {
            $this->logger->warn("Failed to open airports file: {$filePath}");
            return;
        }

        // Note: airports.csv.gz has no header row
        while (($row = readCsvRow($file)) !== false) {
            // Columns: ID, Name, City, Country, IATA, ICAO, Lat, Lon, ...
            $iata = isset($row[4]) ? trim($row[4]) : null;
            if (empty($iata) || $iata === '\\N') {
                continue;
            }

            $latStr = isset($row[6]) ? trim($row[6]) : null;
            $lonStr = isset($row[7]) ? trim($row[7]) : null;
            if (empty($latStr) || empty($lonStr)) {
                continue;
            }

            $lat = filter_var($latStr, FILTER_VALIDATE_FLOAT);
            $lon = filter_var($lonStr, FILTER_VALIDATE_FLOAT);
            if ($lat === false || $lon === false) {
                continue;
            }

            $this->airports[strtoupper($iata)] = ['lat' => $lat, 'lon' => $lon];
            $count++;
        }

        fclose($file);
        $this->logger->info("Loaded {$count} airports into lookup table");
    }
}

class CancellationLookup
{
    private $logger;
    private $cancellations = [];

    public function __construct(?string $cancellationsFile, $logger)
    {
        $this->logger = $logger;
        if ($cancellationsFile && file_exists($cancellationsFile)) {
            $this->loadCancellations($cancellationsFile);
        }
    }

    public function lookupReason(?string $code): ?string
    {
        if (empty($code)) {
            return null;
        }

        return $this->cancellations[strtoupper($code)] ?? null;
    }

    private function loadCancellations(string $filePath): void
    {
        $this->logger->info("Loading cancellations from {$filePath}");

        $count = 0;
        $file = fopen($filePath, 'r');
        if (!$file) {
            $this->logger->warn("Failed to open cancellations file: {$filePath}");
            return;
        }

        // Read header
        $headers = readCsvRow($file);
        if ($headers === false) {
            fclose($file);
            return;
        }

        $codeIndex = array_search('Code', $headers);
        $descIndex = array_search('Description', $headers);

        if ($codeIndex === false || $descIndex === false) {
            $this->logger->warn("Missing required columns in cancellations file");
            fclose($file);
            return;
        }

        while (($row = readCsvRow($file)) !== false) {
            $code = isset($row[$codeIndex]) ? trim($row[$codeIndex]) : null;
            $description = isset($row[$descIndex]) ? trim($row[$descIndex]) : null;
            if (empty($code) || empty($description)) {
                continue;
            }

            $this->cancellations[strtoupper($code)] = $description;
            $count++;
        }

        fclose($file);
        $this->logger->info("Loaded {$count} cancellation reasons into lookup table");
    }
}

class FlightLoader
{
    const BATCH_SIZE = 500;

    private $client;
    private $mapping;
    private $indexPrefix;
    private $logger;
    private $batchSize;
    private $refresh;
    private $airportLookup;
    private $cancellationLookup;
    private $ensuredIndices = [];
    private $loadedRecords = 0;
    private $totalRecords = 0;

    public function __construct(
        $client,
        array $mapping,
        string $index,
        $logger,
        int $batchSize = self::BATCH_SIZE,
        bool $refresh = false,
        ?string $airportsFile = null,
        ?string $cancellationsFile = null
    ) {
        $this->client = $client;
        $this->mapping = $mapping;
        $this->indexPrefix = $index;
        $this->logger = $logger;
        $this->batchSize = $batchSize;
        $this->refresh = $refresh;
        $this->airportLookup = new AirportLookup($airportsFile, $logger);
        $this->cancellationLookup = new CancellationLookup($cancellationsFile, $logger);
    }

    public function ensureIndex(string $indexName): void
    {
        if (!$this->client) {
            return;
        }

        if (in_array($indexName, $this->ensuredIndices)) {
            $this->logger->debug("Index {$indexName} already ensured in this session");
            return;
        }

        // Delete index if it exists before creating a new one
        if ($this->client->indexExists($indexName)) {
            $this->logger->info("Deleting existing index '{$indexName}' before import");
            if ($this->client->deleteIndex($indexName)) {
                $this->logger->info("Index '{$indexName}' deleted");
            } else {
                $this->logger->warn("Failed to delete index '{$indexName}'");
            }
        }

        $this->logger->info("Creating index: {$indexName}");
        $this->client->createIndex($indexName, $this->mapping);
        $this->ensuredIndices[] = $indexName;
        $this->logger->info("Successfully created index: {$indexName}");
    }

    public function importFiles(array $files): void
    {
        $this->logger->info("Counting records in " . count($files) . " file(s)...");
        $this->totalRecords = $this->countTotalRecordsFast($files);
        $this->logger->info("Total records to import: " . $this->formatNumber($this->totalRecords));
        $this->logger->info("Importing " . count($files) . " file(s)...");

        foreach ($files as $filePath) {
            $this->importFile($filePath);
        }

        // Print newline after progress line
        echo "\n";
        $this->logger->info("Import complete: " . $this->formatNumber($this->loadedRecords) . " of " . $this->formatNumber($this->totalRecords) . " records loaded");
    }

    public function sampleDocument(string $filePath): ?array
    {
        if (!is_file($filePath)) {
            $this->logger->warn("Skipping {$filePath} (not a regular file)");
            return null;
        }

        $this->logger->info("Sampling first document from {$filePath}");

        return $this->withDataIo($filePath, function ($io) {
            $headers = readCsvRow($io);
            if ($headers === false) {
                return null;
            }

            $row = readCsvRow($io);
            if ($row === false) {
                return null;
            }

            $rowData = array_combine($headers, $row);
            return $this->transformRow($rowData);
        });
    }

    private function formatNumber(int $number): string
    {
        return number_format($number);
    }

    private function countTotalRecordsFast(array $files): int
    {
        $total = 0;
        foreach ($files as $filePath) {
            if (!is_file($filePath)) {
                continue;
            }

            $lineCount = $this->countLinesFast($filePath);
            // Subtract 1 for CSV header
            $total += max($lineCount - 1, 0);
        }
        return $total;
    }

    private function countLinesFast(string $filePath): int
    {
        $ext = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));
        $basename = basename($filePath);
        $isGz = strtolower(substr($basename, -3)) === '.gz';

        try {
            if ($ext === 'zip' || ($isGz && strpos($basename, '.zip') !== false)) {
                $zip = new ZipArchive();
                if ($zip->open($filePath) === true) {
                    $entry = $this->csvEntryInZip($zip);
                    if (!$entry) {
                        $zip->close();
                        return 0;
                    }
                    $content = $zip->getFromName($entry);
                    $zip->close();
                    return substr_count($content, "\n") + 1;
                }
            } elseif ($isGz) {
                $file = gzopen($filePath, 'rb');
                if (!$file) {
                    return 0;
                }
                $count = 0;
                while (gzgets($file) !== false) {
                    $count++;
                }
                gzclose($file);
                return $count;
            } else {
                $count = 0;
                $file = fopen($filePath, 'r');
                if ($file) {
                    while (fgets($file) !== false) {
                        $count++;
                    }
                    fclose($file);
                }
                return $count;
            }
        } catch (\Exception $e) {
            $this->logger->warn("Failed to count lines in {$filePath}: " . $e->getMessage());
            return 0;
        }
    }

    private function importFile(string $filePath): void
    {
        if (!is_file($filePath)) {
            $this->logger->warn("Skipping {$filePath} (not a regular file)");
            return;
        }

        $this->logger->info("Importing {$filePath}");

        // Extract year and month from filename if available
        [$fileYear, $fileMonth] = $this->extractYearMonthFromFilename($filePath);

        // Buffer documents by index name (year-month)
        $indexBuffers = []; // [ index_name => [ 'lines' => [], 'count' => 0 ] ]
        $indexedDocs = 0;
        $processedRows = 0;

        $this->withDataIo($filePath, function ($io) use (&$indexBuffers, &$indexedDocs, &$processedRows, $fileYear, $fileMonth) {
            $headers = readCsvRow($io);
            if ($headers === false) {
                return;
            }

            while (($row = readCsvRow($io)) !== false) {
                $processedRows++;

                // Debug: check if we have timestamp source (only log first time)
                if ($processedRows === 1) {
                    $hasTimestamp = in_array('@timestamp', $headers);
                    $hasFlightDate = in_array('FlightDate', $headers);
                    if (!$hasTimestamp && !$hasFlightDate) {
                        $availableHeaders = array_slice($headers, 0, 10);
                        $this->logger->warn("CSV headers don't include '@timestamp' or 'FlightDate'. Available headers: " . implode(', ', $availableHeaders));
                    }
                }

                $rowData = array_combine($headers, $row);
                $doc = $this->transformRow($rowData);
                if (empty($doc)) {
                    continue;
                }

                // Extract index name from timestamp or filename (must be checked before array_filter removes it)
                $timestamp = $doc['@timestamp'] ?? null;
                $indexName = $this->extractIndexName($timestamp, $fileYear, $fileMonth);
                if (!$indexName) {
                    $timestampRaw = $rowData['@timestamp'] ?? $rowData['FlightDate'] ?? null;
                    $this->logger->warn("Skipping document - missing or invalid timestamp. Raw value: " . var_export($timestampRaw, true) . ", parsed timestamp: " . var_export($timestamp, true) . ". Row {$processedRows}: Origin=" . ($rowData['Origin'] ?? '') . ", Dest=" . ($rowData['Dest'] ?? '') . ", Airline=" . ($rowData['Reporting_Airline'] ?? ''));
                    continue;
                }

                // Now filter the document (removing null values) since we've extracted what we need
                $doc = array_filter($doc, function ($value) {
                    return $value !== null;
                });

                // Ensure index exists
                $this->ensureIndex($indexName);

                // Initialize buffer for this index if needed
                if (!isset($indexBuffers[$indexName])) {
                    $indexBuffers[$indexName] = ['lines' => [], 'count' => 0];
                }

                // Add document to buffer
                $buffer = &$indexBuffers[$indexName];
                $buffer['lines'][] = json_encode(['index' => ['_index' => $indexName]]);
                $buffer['lines'][] = json_encode($doc);
                $buffer['count']++;

                // Flush if buffer is full
                if ($buffer['count'] >= $this->batchSize) {
                    $indexedDocs += $this->flushIndex($indexName, $buffer['lines'], $buffer['count']);
                    $buffer['lines'] = [];
                    $buffer['count'] = 0;
                }
            }
        });

        // Flush any remaining buffers
        foreach ($indexBuffers as $indexName => $buffer) {
            if ($buffer['count'] > 0) {
                $indexedDocs += $this->flushIndex($indexName, $buffer['lines'], $buffer['count']);
            }
        }

        $this->logger->info("Finished {$filePath} (rows processed: {$processedRows}, documents indexed: {$indexedDocs})");
    }

    private function flushIndex(string $indexName, array $lines, int $docCount): int
    {
        $payload = implode("\n", $lines) . "\n";
        $result = $this->client->bulk($indexName, $payload, $this->refresh);

        if (isset($result['errors']) && $result['errors']) {
            $errors = [];
            if (isset($result['items'])) {
                foreach ($result['items'] as $item) {
                    if (isset($item['index']['error'])) {
                        $errors[] = $item['index']['error'];
                    }
                }
            }
            $errorCount = min(5, count($errors));
            for ($i = 0; $i < $errorCount; $i++) {
                $this->logger->error("Bulk item error for {$indexName}: " . json_encode($errors[$i]));
            }
            throw new RuntimeException("Bulk indexing reported errors for {$indexName}; aborting");
        }

        $this->loadedRecords += $docCount;
        if ($this->totalRecords > 0) {
            $percentage = round(($this->loadedRecords / $this->totalRecords) * 100, 1);
            echo "\r" . $this->formatNumber($this->loadedRecords) . " of " . $this->formatNumber($this->totalRecords) . " records loaded ({$percentage}%)";
        } else {
            echo "\r" . $this->formatNumber($this->loadedRecords) . " records loaded";
        }
        flush();

        return $docCount;
    }

    private function withDataIo(string $filePath, callable $callback)
    {
        $ext = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));
        $basename = basename($filePath);
        $isGz = strtolower(substr($basename, -3)) === '.gz';

        if ($ext === 'zip' || ($isGz && strpos($basename, '.zip') !== false)) {
            $zip = new ZipArchive();
            if ($zip->open($filePath) !== true) {
                throw new RuntimeException("Failed to open zip file: {$filePath}");
            }

            $entry = $this->csvEntryInZip($zip);
            if (!$entry) {
                $zip->close();
                throw new RuntimeException("No CSV entry found in {$filePath}");
            }

            $content = $zip->getFromName($entry);
            $zip->close();

            if ($content === false) {
                throw new RuntimeException("Failed to read {$entry} from {$filePath}");
            }

            $tempFile = tmpfile();
            fwrite($tempFile, $content);
            rewind($tempFile);
            $result = $callback($tempFile);
            fclose($tempFile);
            return $result;
        } elseif ($isGz) {
            // Use gzip stream wrapper so fgetcsv works correctly
            $file = fopen("compress.zlib://{$filePath}", 'r');
            if (!$file) {
                throw new RuntimeException("Failed to open gzip file: {$filePath}");
            }
            try {
                return $callback($file);
            } finally {
                fclose($file);
            }
        } else {
            $file = fopen($filePath, 'r');
            if (!$file) {
                throw new RuntimeException("Failed to open file: {$filePath}");
            }
            try {
                return $callback($file);
            } finally {
                fclose($file);
            }
        }
    }

    private function csvEntryInZip(ZipArchive $zip): ?string
    {
        for ($i = 0; $i < $zip->numFiles; $i++) {
            $name = $zip->getNameIndex($i);
            if ($name !== false && strtolower(substr($name, -4)) === '.csv') {
                return $name;
            }
        }
        return null;
    }

    private function extractIndexName(?string $timestamp, ?string $fileYear, ?string $fileMonth): ?string
    {
        // If filename specifies month, use that format: flights-<year>-<month>
        if ($fileYear && $fileMonth) {
            return "{$this->indexPrefix}-{$fileYear}-{$fileMonth}";
        }

        // If filename specifies only year, use that format: flights-<year>
        if ($fileYear) {
            return "{$this->indexPrefix}-{$fileYear}";
        }

        // Otherwise, derive from timestamp
        if (!$timestamp) {
            return null;
        }

        // Parse YYYY-MM-DD format and extract YYYY-MM or YYYY
        if (preg_match('/^(\d{4})-(\d{2})-\d{2}/', $timestamp, $matches)) {
            $year = $matches[1];
            // Since filename didn't specify month, use year-only format
            return "{$this->indexPrefix}-{$year}";
        } else {
            $this->logger->warn("Unable to parse timestamp format: {$timestamp}");
            return null;
        }
    }

    private function extractYearMonthFromFilename(string $filePath): array
    {
        $basename = basename($filePath);
        // Remove extensions (.gz, .csv, .zip) - handle multiple extensions like .csv.gz
        $basename = preg_replace('/\.(gz|csv|zip)$/i', '', $basename);

        // Try pattern: flights-YYYY-MM (e.g., flights-2024-07)
        if (preg_match('/-(\d{4})-(\d{2})$/', $basename, $matches)) {
            return [$matches[1], $matches[2]];
        }

        // Try pattern: flights-YYYY (e.g., flights-2019)
        if (preg_match('/-(\d{4})$/', $basename, $matches)) {
            return [$matches[1], null];
        }

        // No pattern matched
        return [null, null];
    }

    private function transformRow(array $row): array
    {
        $doc = [];

        // Get timestamp - prefer @timestamp column if it exists, otherwise use FlightDate
        $timestamp = presence($row['@timestamp'] ?? null) ?: presence($row['FlightDate'] ?? null);

        // Flight ID - construct from date, airline, flight number, origin, and destination
        $flightDate = $timestamp;
        $reportingAirline = presence($row['Reporting_Airline'] ?? null);
        $flightNumber = presence($row['Flight_Number_Reporting_Airline'] ?? null);
        $origin = presence($row['Origin'] ?? null);
        $dest = presence($row['Dest'] ?? null);

        if ($flightDate && $reportingAirline && $flightNumber && $origin && $dest) {
            $doc['FlightID'] = "{$flightDate}_{$reportingAirline}_{$flightNumber}_{$origin}_{$dest}";
        }

        // @timestamp field - use timestamp directly (required for index routing)
        // Store it even if null so we can detect missing dates and skip the document
        $doc['@timestamp'] = $timestamp;

        // Direct mappings from CSV to mapping field names
        $doc['Reporting_Airline'] = $reportingAirline;
        $doc['Tail_Number'] = presence($row['Tail_Number'] ?? null);
        $doc['Flight_Number'] = $flightNumber;
        $doc['Origin'] = $origin;
        $doc['Dest'] = $dest;

        // Time fields - convert to integers (minutes or time in HHMM format)
        $doc['CRSDepTimeLocal'] = $this->toInteger($row['CRSDepTime'] ?? null);
        $doc['DepDelayMin'] = $this->toInteger($row['DepDelay'] ?? null);
        $doc['TaxiOutMin'] = $this->toInteger($row['TaxiOut'] ?? null);
        $doc['TaxiInMin'] = $this->toInteger($row['TaxiIn'] ?? null);
        $doc['CRSArrTimeLocal'] = $this->toInteger($row['CRSArrTime'] ?? null);
        $doc['ArrDelayMin'] = $this->toInteger($row['ArrDelay'] ?? null);

        // Boolean fields
        $doc['Cancelled'] = $this->toBoolean($row['Cancelled'] ?? null);
        $doc['Diverted'] = $this->toBoolean($row['Diverted'] ?? null);

        // Cancellation code
        $cancellationCode = presence($row['CancellationCode'] ?? null);
        $doc['CancellationCode'] = $cancellationCode;

        // Cancellation reason - lookup from cancellations data
        $cancellationReason = $this->cancellationLookup->lookupReason($cancellationCode);
        if ($cancellationReason) {
            $doc['CancellationReason'] = $cancellationReason;
        }

        // Time duration fields (convert to minutes as integers)
        $doc['ActualElapsedTimeMin'] = $this->toInteger($row['ActualElapsedTime'] ?? null);
        $doc['AirTimeMin'] = $this->toInteger($row['AirTime'] ?? null);

        // Count and distance
        $doc['Flights'] = $this->toInteger($row['Flights'] ?? null);
        $doc['DistanceMiles'] = $this->toInteger($row['Distance'] ?? null);

        // Delay fields (with Min suffix to match mapping)
        $doc['CarrierDelayMin'] = $this->toInteger($row['CarrierDelay'] ?? null);
        $doc['WeatherDelayMin'] = $this->toInteger($row['WeatherDelay'] ?? null);
        $doc['NASDelayMin'] = $this->toInteger($row['NASDelay'] ?? null);
        $doc['SecurityDelayMin'] = $this->toInteger($row['SecurityDelay'] ?? null);
        $doc['LateAircraftDelayMin'] = $this->toInteger($row['LateAircraftDelay'] ?? null);

        // Geo point fields - lookup from airports data
        $originLocation = $this->airportLookup->lookupCoordinates($origin);
        if ($originLocation) {
            $doc['OriginLocation'] = $originLocation;
        }

        $destLocation = $this->airportLookup->lookupCoordinates($dest);
        if ($destLocation) {
            $doc['DestLocation'] = $destLocation;
        }

        // Don't filter here - we need @timestamp to stay even if null so we can detect missing dates
        // array_filter will be called in importFile after we extract the index name
        return $doc;
    }

    private function present($value): ?string
    {
        if ($value === null) {
            return null;
        }

        $trimmed = trim((string)$value);
        return $trimmed === '' ? null : $trimmed;
    }

    private function toFloat($value): ?float
    {
        $value = $this->present($value);
        if ($value === null) {
            return null;
        }

        $float = filter_var($value, FILTER_VALIDATE_FLOAT);
        return $float !== false ? $float : null;
    }

    private function toInteger($value): ?int
    {
        $value = $this->present($value);
        if ($value === null) {
            return null;
        }

        $float = filter_var($value, FILTER_VALIDATE_FLOAT);
        return $float !== false ? (int)round($float) : null;
    }

    private function toBoolean($value): ?bool
    {
        $value = $this->present($value);
        if ($value === null) {
            return null;
        }

        $lower = strtolower($value);
        if (in_array($lower, ['true', 't', 'yes', 'y'])) {
            return true;
        }
        if (in_array($lower, ['false', 'f', 'no', 'n'])) {
            return false;
        }

        $numeric = filter_var($value, FILTER_VALIDATE_FLOAT);
        if ($numeric === false) {
            return null;
        }

        return $numeric > 0;
    }
}

class SimpleLogger
{
    private $level;

    const DEBUG = 0;
    const INFO = 1;
    const WARN = 2;
    const ERROR = 3;

    public function __construct(int $level = self::INFO)
    {
        $this->level = $level;
    }

    public function debug(string $message): void
    {
        if ($this->level <= self::DEBUG) {
            $this->log('DEBUG', $message);
        }
    }

    public function info(string $message): void
    {
        if ($this->level <= self::INFO) {
            $this->log('INFO', $message);
        }
    }

    public function warn(string $message): void
    {
        if ($this->level <= self::WARN) {
            $this->log('WARN', $message);
        }
    }

    public function error(string $message): void
    {
        if ($this->level <= self::ERROR) {
            $this->log('ERROR', $message);
        }
    }

    private function log(string $level, string $message): void
    {
        $timestamp = date('Y-m-d H:i:s');
        echo "[{$timestamp}] [{$level}] {$message}\n";
    }
}

function parseOptions(array $argv): array
{
    $options = [
        'config' => 'config/elasticsearch.yml',
        'mapping' => 'config/mappings-flights.json',
        'data_dir' => 'data',
        'index' => 'flights',
        'batch_size' => FlightLoader::BATCH_SIZE,
        'refresh' => false,
        'status' => false,
        'delete_index' => false,
        'delete_all' => false,
        'sample' => false,
        'airports_file' => 'data/airports.csv.gz',
        'cancellations_file' => 'data/cancellations.csv'
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
            case '-d':
            case '--data-dir':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --data-dir requires a path\n");
                    exit(1);
                }
                $options['data_dir'] = $argv[$i];
                break;
            case '-f':
            case '--file':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --file requires a path\n");
                    exit(1);
                }
                $options['file'] = $argv[$i];
                break;
            case '-a':
            case '--all':
                $options['all'] = true;
                break;
            case '-g':
            case '--glob':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --glob requires a pattern\n");
                    exit(1);
                }
                $options['glob'] = $argv[$i];
                break;
            case '--index':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --index requires a name\n");
                    exit(1);
                }
                $options['index'] = $argv[$i];
                break;
            case '--batch-size':
                $i++;
                if ($i >= count($argv)) {
                    fwrite(STDERR, "Error: --batch-size requires a number\n");
                    exit(1);
                }
                $options['batch_size'] = (int)$argv[$i];
                break;
            case '--refresh':
                $options['refresh'] = true;
                break;
            case '--status':
                $options['status'] = true;
                break;
            case '--delete-index':
                $options['delete_index'] = true;
                break;
            case '--delete-all':
                $options['delete_all'] = true;
                break;
            case '--sample':
                $options['sample'] = true;
                break;
            case '-h':
            case '--help':
                showHelp();
                exit(0);
            default:
                if (strpos($arg, '-') === 0) {
                    fwrite(STDERR, "Unknown option: {$arg}\n");
                    showHelp();
                    exit(1);
                }
                // Treat as file path
                $options['glob_files'] = $options['glob_files'] ?? [];
                $options['glob_files'][] = $arg;
                break;
        }
        $i++;
    }

    // If --glob was expanded by the shell, capture the expanded file list
    if (isset($options['glob'])) {
        $globValue = $options['glob'];
        $globFiles = $options['glob_files'] ?? [];
        $hasWildcards = strpbrk($globValue, '*?[]{}') !== false;

        if (!$hasWildcards || !empty($globFiles)) {
            $options['glob_files'] = array_merge([$globValue], $globFiles);
            unset($options['glob']);
        }
    }

    // Validation
    if ($options['status'] && ($options['delete_index'] || $options['delete_all'])) {
        fwrite(STDERR, "Cannot use --status with --delete-index or --delete-all\n");
        exit(1);
    }

    if ($options['delete_index'] && $options['delete_all']) {
        fwrite(STDERR, "Cannot use --delete-index and --delete-all together\n");
        exit(1);
    }

    if (!$options['status'] && !$options['delete_index'] && !$options['delete_all'] && !$options['sample']) {
        $selectionOptions = array_filter([
            $options['file'] ?? null,
            $options['all'] ?? null,
            $options['glob'] ?? null,
            $options['glob_files'] ?? null
        ]);
        if (count($selectionOptions) > 1) {
            fwrite(STDERR, "Cannot use --file, --all, and --glob together (use only one)\n");
            exit(1);
        }

        if (empty($selectionOptions)) {
            fwrite(STDERR, "Please provide either --file PATH, --all, or --glob PATTERN\n");
            exit(1);
        }
    }

    return $options;
}

function showHelp(): void
{
    echo "Usage: import_flights.php [options]\n\n";
    echo "Options:\n";
    echo "  -c, --config PATH        Path to Elasticsearch config YAML (default: config/elasticsearch.yml)\n";
    echo "  -m, --mapping PATH       Path to mappings JSON (default: config/mappings-flights.json)\n";
    echo "  -d, --data-dir PATH      Directory containing data files (default: data)\n";
    echo "  -f, --file PATH          Only import the specified file\n";
    echo "  -a, --all                Import all files found in the data directory\n";
    echo "  -g, --glob PATTERN       Import files matching the glob pattern\n";
    echo "  --index NAME             Override index name (default: flights)\n";
    echo "  --batch-size N           Number of documents per bulk request (default: 500)\n";
    echo "  --refresh                Request an index refresh after each bulk request\n";
    echo "  --status                 Test connection and print cluster health status\n";
    echo "  --delete-index           Delete indices matching the index pattern and exit\n";
    echo "  --delete-all              Delete all flights-* indices and exit\n";
    echo "  --sample                 Print the first document and exit\n";
    echo "  -h, --help               Show this help message\n";
}

function resolvePath(string $path): string
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

function loadConfig(string $path): array
{
    $resolvedPath = resolvePath($path);
    if (!file_exists($resolvedPath)) {
        throw new RuntimeException("Config file not found: {$path} (tried: {$resolvedPath})");
    }

    $content = file_get_contents($resolvedPath);
    return Yaml::parse($content) ?: [];
}

function loadMapping(string $path): array
{
    $resolvedPath = resolvePath($path);
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

function filesToProcess(array $options): array
{
    $resolvedDataDir = resolvePath($options['data_dir']);

    if (isset($options['file'])) {
        return [resolveFilePath($options['file'], $resolvedDataDir)];
    } elseif (!empty($options['glob_files'])) {
        $files = array_map(function ($f) use ($resolvedDataDir) {
            return resolveFilePath($f, $resolvedDataDir);
        }, $options['glob_files']);
        return array_filter($files, 'is_file');
    } elseif (isset($options['glob'])) {
        $globPattern = $options['glob'];

        // If absolute path, use as-is
        if (strpos($globPattern, '/') === 0) {
            $files = glob($globPattern);
        } else {
            // Try the pattern as-is first
            $files = glob($globPattern);
            if (empty($files)) {
                // If no matches, try relative to resolved data_dir
                $expandedPattern = $resolvedDataDir . '/' . $globPattern;
                $files = glob($expandedPattern);
            }
        }

        $files = array_filter($files, 'is_file');
        if (empty($files)) {
            throw new RuntimeException("No files found matching pattern: {$globPattern}");
        }
        return $files;
    } else {
        $patternZip = $resolvedDataDir . '/*.zip';
        $patternCsv = $resolvedDataDir . '/*.csv';
        $patternCsvGz = $resolvedDataDir . '/*.csv.gz';
        $files = array_merge(
            glob($patternZip) ?: [],
            glob($patternCsv) ?: [],
            glob($patternCsvGz) ?: []
        );
        if (empty($files)) {
            throw new RuntimeException("No .zip, .csv, or .csv.gz files found in {$resolvedDataDir}");
        }
        return $files;
    }
}

function resolveFilePath(string $path, string $dataDir): string
{
    // If path is absolute, use as-is
    if (strpos($path, '/') === 0 || (PHP_OS_FAMILY === 'Windows' && preg_match('/^[A-Z]:/i', $path))) {
        if (file_exists($path)) {
            return $path;
        }
    }

    // Try relative to resolved data_dir
    $candidate = $dataDir . '/' . $path;
    if (file_exists($candidate)) {
        return $candidate;
    }

    // Try relative to current directory
    if (file_exists($path)) {
        return $path;
    }

    throw new RuntimeException("File not found: {$path}");
}

function main(array $argv): void
{
    $options = parseOptions($argv);
    $logger = new SimpleLogger();

    if ($options['sample']) {
        sampleDocument($options, $logger);
        return;
    }

    $config = loadConfig($options['config']);
    $client = new ElasticsearchClient($config, $logger);

    if ($options['status']) {
        reportStatus($client, $logger);
        return;
    }

    if ($options['delete_index']) {
        deleteIndicesByPattern($client, $logger, $options['index']);
        return;
    }

    if ($options['delete_all']) {
        deleteIndicesByPattern($client, $logger, 'flights-*');
        return;
    }

    $mapping = loadMapping($options['mapping']);

    // Resolve airports and cancellations file paths
    $resolvedAirportsFile = isset($options['airports_file']) ? resolvePath($options['airports_file']) : null;
    $resolvedCancellationsFile = isset($options['cancellations_file']) ? resolvePath($options['cancellations_file']) : null;

    $loader = new FlightLoader(
        $client,
        $mapping,
        $options['index'],
        $logger,
        $options['batch_size'],
        $options['refresh'],
        $resolvedAirportsFile,
        $resolvedCancellationsFile
    );

    $files = filesToProcess($options);
    $loader->importFiles($files);
}

function sampleDocument(array $options, $logger): void
{
    $mapping = loadMapping($options['mapping']);

    // Resolve airports and cancellations file paths
    $resolvedAirportsFile = isset($options['airports_file']) ? resolvePath($options['airports_file']) : null;
    $resolvedCancellationsFile = isset($options['cancellations_file']) ? resolvePath($options['cancellations_file']) : null;

    $loader = new FlightLoader(
        null,
        $mapping,
        'flights',
        $logger,
        1,
        false,
        $resolvedAirportsFile,
        $resolvedCancellationsFile
    );

    $files = filesToProcess($options);
    if (empty($files)) {
        $logger->error('No files found to sample');
        exit(1);
    }

    $doc = $loader->sampleDocument($files[0]);
    if ($doc === null) {
        $logger->error('No document found in file');
        exit(1);
    }

    echo json_encode($doc, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";
}

function reportStatus($client, $logger): void
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

function deleteIndicesByPattern($client, $logger, string $pattern): void
{
    $patternWithWildcard = strpos($pattern, '*') === false ? $pattern . '-*' : $pattern;
    $logger->info("Searching for indices matching pattern: {$patternWithWildcard}");

    try {
        $deleted = $client->deleteIndicesByPattern($patternWithWildcard);

        if (empty($deleted)) {
            $logger->warn("No indices found matching pattern: {$patternWithWildcard}");
        } else {
            $logger->info("Deleted " . count($deleted) . " index(es): " . implode(', ', $deleted));
        }
    } catch (\Exception $e) {
        $logger->error("Failed to delete indices matching pattern '{$pattern}': " . $e->getMessage());
        exit(1);
    }
}

// Only run main() if this file is being executed directly (not included)
if (php_sapi_name() === 'cli' && basename(__FILE__) === basename($_SERVER['PHP_SELF'])) {
    $startTime = microtime(true);
    try {
        main($argv);
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
