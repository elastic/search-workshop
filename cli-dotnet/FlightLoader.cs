using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;

namespace ImportFlights;

internal class IndexBuffer
{
    public List<Dictionary<string, object?>> Documents { get; } = new();
    public int Count { get; set; }
}

public class FlightLoader
{
    private const int BatchSize = 500;
    private readonly ElasticsearchClientWrapper? _client;
    private readonly Dictionary<string, object> _mapping;
    private readonly string _indexPrefix;
    private readonly ILogger _logger;
    private readonly int _batchSize;
    private readonly bool _refresh;
    private readonly AirportLookup _airportLookup;
    private readonly CancellationLookup _cancellationLookup;
    private readonly HashSet<string> _ensuredIndices = new();
    private int _loadedRecords = 0;
    private int _totalRecords = 0;
    private int _missingTimestampCount = 0;
    private int _missingTimestampWarnings = 0;
    private const int MissingTimestampWarningLimit = 5;

    public FlightLoader(
        ElasticsearchClientWrapper? client,
        Dictionary<string, object> mapping,
        string index,
        ILogger logger,
        int batchSize = BatchSize,
        bool refresh = false,
        string? airportsFile = null,
        string? cancellationsFile = null)
    {
        _client = client;
        _mapping = mapping;
        _indexPrefix = index;
        _logger = logger;
        _batchSize = batchSize;
        _refresh = refresh;
        _airportLookup = new AirportLookup(airportsFile, logger);
        _cancellationLookup = new CancellationLookup(cancellationsFile, logger);
    }

    private async Task EnsureIndexAsync(string indexName)
    {
        if (_client == null)
        {
            return;
        }

        if (_ensuredIndices.Contains(indexName))
            return;

        // Delete index if it exists before creating a new one
        if (await _client.IndexExistsAsync(indexName))
        {
            _logger.Info($"Deleting existing index '{indexName}' before import");
            if (await _client.DeleteIndexAsync(indexName))
            {
                _logger.Info($"Index '{indexName}' deleted");
            }
            else
            {
                _logger.Warn($"Failed to delete index '{indexName}'");
            }
        }

        _logger.Info($"Creating index: {indexName}");
        await _client.CreateIndexAsync(indexName, _mapping);
        _ensuredIndices.Add(indexName);
        _logger.Info($"Successfully created index: {indexName}");
    }

    public async Task ImportFilesAsync(List<string> files)
    {
        _logger.Info($"Counting records in {files.Count} file(s)...");
        _totalRecords = CountTotalRecordsFast(files);
        _logger.Info($"Total records to import: {FormatNumber(_totalRecords)}");
        _logger.Info($"Importing {files.Count} file(s)...");

        foreach (var filePath in files)
        {
            await ImportFileAsync(filePath);
        }

        Console.WriteLine();
        _logger.Info($"Import complete: {FormatNumber(_loadedRecords)} of {FormatNumber(_totalRecords)} records loaded");

        if (_missingTimestampCount > 0)
        {
            _logger.Warn($"Skipped {_missingTimestampCount} document(s) due to missing or invalid timestamps.");
        }
    }

    public async Task<Dictionary<string, object?>?> SampleDocumentAsync(string filePath)
    {
        if (!File.Exists(filePath))
        {
            _logger.Warn($"Skipping {filePath} (not a regular file)");
            return null;
        }

        _logger.Info($"Sampling first document from {filePath}");

        return await WithDataIoAsync<Dictionary<string, object?>?>(filePath, async stream =>
        {
            using var reader = new StreamReader(stream);
            var config = new CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                MissingFieldFound = null // ignore missing fields so non-flight CSVs don't throw
            };
            using var csv = new CsvReader(reader, config);

            if (!await csv.ReadAsync())
            {
                return null;
            }

            csv.ReadHeader();

            if (!await csv.ReadAsync())
            {
                return null;
            }

            _ = csv.GetRecord<dynamic>(); // advance reader; data is accessed via csv in TransformRow
            return TransformRow(csv);
        });
    }

    private string FormatNumber(int number)
    {
        return number.ToString("N0");
    }

    private int CountTotalRecordsFast(List<string> files)
    {
        var total = 0;
        foreach (var filePath in files)
        {
            if (!File.Exists(filePath))
            {
                continue;
            }

            var lineCount = CountLinesFast(filePath);
            total += Math.Max(lineCount - 1, 0); // Subtract 1 for CSV header
        }
        return total;
    }

    private int CountLinesFast(string filePath)
    {
        try
        {
            if (filePath.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
            {
                // For ZIP files, we'd need to extract and count, but for now return 0
                // The actual import will handle it
                return 0;
            }
            else if (filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
            {
                using var fileStream = File.OpenRead(filePath);
                using var gzStream = new GZipStream(fileStream, CompressionMode.Decompress);
                using var reader = new StreamReader(gzStream);
                var count = 0;
                while (reader.ReadLine() != null)
                {
                    count++;
                }
                return count;
            }
            else
            {
                return File.ReadLines(filePath).Count();
            }
        }
        catch (Exception ex)
        {
            _logger.Warn($"Failed to count lines in {filePath}: {ex.Message}");
            return 0;
        }
    }

    private async Task ImportFileAsync(string filePath)
    {
        if (!File.Exists(filePath))
        {
            _logger.Warn($"Skipping {filePath} (not a regular file)");
            return;
        }

        _logger.Info($"Importing {filePath}");

        var (fileYear, fileMonth) = ExtractYearMonthFromFilename(filePath);

        // Buffer documents by index name (year-month)
        var indexBuffers = new Dictionary<string, IndexBuffer>();
        var indexedDocs = 0;
        var processedRows = 0;

        await WithDataIoAsync(filePath, async stream =>
        {
            using var reader = new StreamReader(stream);
            var config = new CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                MissingFieldFound = null // ignore missing fields so sampling non-flight CSVs doesn't throw
            };
            using var csv = new CsvReader(reader, config);

            if (!await csv.ReadAsync())
            {
                return;
            }

            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                processedRows++;

                // Debug: check if we have timestamp source (only log first time)
                if (processedRows == 1)
                {
                    var headers = csv.HeaderRecord;
                    var hasTimestamp = headers?.Contains("@timestamp") ?? false;
                    var hasFlightDate = headers?.Contains("FlightDate") ?? false;
                    if (!hasTimestamp && !hasFlightDate)
                    {
                        var firstHeaders = headers?.Take(10).ToList() ?? new List<string>();
                        _logger.Warn($"CSV headers don't include '@timestamp' or 'FlightDate'. Available headers: {string.Join(", ", firstHeaders)}");
                    }
                }

                var doc = TransformRow(csv);
                if (doc == null || doc.Count == 0)
                {
                    continue;
                }

                // Extract index name from timestamp or filename
                doc.TryGetValue("@timestamp", out var timestampObj);
                var timestamp = timestampObj?.ToString();
                var indexName = ExtractIndexName(timestamp, fileYear, fileMonth);
                if (string.IsNullOrEmpty(indexName))
                {
                    var timestampRaw = csv.GetField("@timestamp") ?? csv.GetField("FlightDate");
                    _missingTimestampCount++;
                    if (_missingTimestampWarnings < MissingTimestampWarningLimit)
                    {
                        _logger.Warn($"Skipping document - missing or invalid timestamp. Raw value: {timestampRaw}, parsed timestamp: {timestamp}. Row {processedRows}: Origin={csv.GetField("Origin")}, Dest={csv.GetField("Dest")}, Airline={csv.GetField("Reporting_Airline")}");
                        _missingTimestampWarnings++;
                        if (_missingTimestampWarnings == MissingTimestampWarningLimit)
                        {
                            _logger.Warn("Further missing timestamp warnings will be suppressed.");
                        }
                    }
                    continue;
                }

                // Remove null values
                doc = doc
                    .Where(kv => kv.Value is not null)
                    .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

                // Ensure index exists
                await EnsureIndexAsync(indexName);

                // Initialize buffer for this index if needed
                if (!indexBuffers.TryGetValue(indexName, out var buffer))
                {
                    buffer = new IndexBuffer();
                    indexBuffers[indexName] = buffer;
                }

                // Add document to buffer
                buffer.Documents.Add(doc);
                buffer.Count++;

                // Flush if buffer is full
                if (buffer.Count >= _batchSize)
                {
                    indexedDocs += await FlushIndexAsync(indexName, buffer.Documents, buffer.Count);
                    buffer.Documents.Clear();
                    buffer.Count = 0;
                }
            }
        });

        // Flush any remaining buffers
        foreach (var kvp in indexBuffers)
        {
            var buffer = kvp.Value;
            if (buffer.Count > 0)
            {
                indexedDocs += await FlushIndexAsync(kvp.Key, buffer.Documents, buffer.Count);
            }
        }

        _logger.Info($"Finished {filePath} (rows processed: {processedRows}, documents indexed: {indexedDocs})");
    }

    private async Task<int> FlushIndexAsync(string indexName, List<Dictionary<string, object?>> documents, int docCount)
    {
        if (_client == null)
        {
            return 0;
        }

        try
        {
            var response = await _client.BulkAsync(indexName, documents, _refresh);

            if (response.Errors)
            {
                var errors = response.ItemsWithErrors.Take(5);
                foreach (var error in errors)
                {
                    _logger.Error($"Bulk item error for {indexName}: {error.Error?.Reason}");
                }
                throw new Exception($"Bulk indexing reported errors for {indexName}; aborting");
            }

            _loadedRecords += docCount;
            if (_totalRecords > 0)
            {
                var percentage = Math.Round((double)_loadedRecords / _totalRecords * 100, 1);
                Console.Write($"\r{FormatNumber(_loadedRecords)} of {FormatNumber(_totalRecords)} records loaded ({percentage}%)");
            }
            else
            {
                Console.Write($"\r{FormatNumber(_loadedRecords)} records loaded");
            }
            Console.Out.Flush();

            return docCount;
        }
        catch (Exception ex)
        {
            _logger.Error($"Bulk flush failed for {indexName}: {ex.Message}");
            throw;
        }
    }

    private async Task<T> WithDataIoAsync<T>(string filePath, Func<Stream, Task<T>> action)
    {
        if (filePath.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
        {
            using var archive = ZipFile.OpenRead(filePath);
            var entry = archive.Entries.FirstOrDefault(e => e.Name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase));
            if (entry == null)
            {
                throw new Exception($"No CSV entry found in {filePath}");
            }

            using var entryStream = entry.Open();
            return await action(entryStream);
        }
        else if (filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
        {
            using var fileStream = File.OpenRead(filePath);
            using var gzStream = new GZipStream(fileStream, CompressionMode.Decompress);
            return await action(gzStream);
        }
        else
        {
            using var fileStream = File.OpenRead(filePath);
            return await action(fileStream);
        }
    }

    private async Task WithDataIoAsync(string filePath, Func<Stream, Task> action)
    {
        if (filePath.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
        {
            using var archive = ZipFile.OpenRead(filePath);
            var entry = archive.Entries.FirstOrDefault(e => e.Name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase));
            if (entry == null)
            {
                throw new Exception($"No CSV entry found in {filePath}");
            }

            using var entryStream = entry.Open();
            await action(entryStream);
        }
        else if (filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
        {
            using var fileStream = File.OpenRead(filePath);
            using var gzStream = new GZipStream(fileStream, CompressionMode.Decompress);
            await action(gzStream);
        }
        else
        {
            using var fileStream = File.OpenRead(filePath);
            await action(fileStream);
        }
    }

    private string? ExtractIndexName(string? timestamp, string? fileYear, string? fileMonth)
    {
        // If filename specifies month, use that format: flights-<year>-<month>
        if (!string.IsNullOrEmpty(fileYear) && !string.IsNullOrEmpty(fileMonth))
        {
            return $"{_indexPrefix}-{fileYear}-{fileMonth}";
        }

        // If filename specifies only year, use that format: flights-<year>
        if (!string.IsNullOrEmpty(fileYear))
        {
            return $"{_indexPrefix}-{fileYear}";
        }

        // Otherwise, derive from timestamp
        if (string.IsNullOrEmpty(timestamp))
        {
            return null;
        }

        // Parse YYYY-MM-DD format and extract YYYY
        var match = Regex.Match(timestamp, @"^(\d{4})-(\d{2})-\d{2}");
        if (match.Success)
        {
            var year = match.Groups[1].Value;
            return $"{_indexPrefix}-{year}";
        }

        _logger.Warn($"Unable to parse timestamp format: {timestamp}");
        return null;
    }

    private (string? Year, string? Month) ExtractYearMonthFromFilename(string filePath)
    {
        var basename = Path.GetFileNameWithoutExtension(filePath);
        // Remove multiple extensions (.csv.gz, .zip, etc.)
        while (true)
        {
            var newBasename = Regex.Replace(basename, @"\.(gz|csv|zip)$", "", RegexOptions.IgnoreCase);
            if (newBasename == basename)
            {
                break;
            }
            basename = newBasename;
        }

        // Try pattern: flights-YYYY-MM (e.g., flights-2024-07)
        var match = Regex.Match(basename, @"-(\d{4})-(\d{2})$");
        if (match.Success)
        {
            return (match.Groups[1].Value, match.Groups[2].Value);
        }

        // Try pattern: flights-YYYY (e.g., flights-2019)
        match = Regex.Match(basename, @"-(\d{4})$");
        if (match.Success)
        {
            return (match.Groups[1].Value, null);
        }

        return (null, null);
    }

    private Dictionary<string, object?> TransformRow(CsvReader csv)
    {
        var doc = new Dictionary<string, object?>();

        // Get timestamp - prefer @timestamp column if it exists, otherwise use FlightDate
        var timestamp = Present(csv.GetField("@timestamp")) ?? Present(csv.GetField("FlightDate"));

        // Flight ID - construct from date, airline, flight number, origin, and destination
        var reportingAirline = Present(csv.GetField("Reporting_Airline"));
        var flightNumber = Present(csv.GetField("Flight_Number_Reporting_Airline"));
        var origin = Present(csv.GetField("Origin"));
        var dest = Present(csv.GetField("Dest"));

        if (!string.IsNullOrEmpty(timestamp) && !string.IsNullOrEmpty(reportingAirline) &&
            !string.IsNullOrEmpty(flightNumber) && !string.IsNullOrEmpty(origin) && !string.IsNullOrEmpty(dest))
        {
            doc["FlightID"] = $"{timestamp}_{reportingAirline}_{flightNumber}_{origin}_{dest}";
        }

        // @timestamp field - use timestamp directly (required for index routing)
        doc["@timestamp"] = timestamp!;

        // Direct mappings from CSV to mapping field names
        doc["Reporting_Airline"] = reportingAirline!;
        doc["Tail_Number"] = Present(csv.GetField("Tail_Number"))!;
        doc["Flight_Number"] = flightNumber!;
        doc["Origin"] = origin!;
        doc["Dest"] = dest!;

        // Time fields - convert to integers (minutes or time in HHMM format)
        doc["CRSDepTimeLocal"] = ToInteger(csv.GetField("CRSDepTime"));
        doc["DepDelayMin"] = ToInteger(csv.GetField("DepDelay"));
        doc["TaxiOutMin"] = ToInteger(csv.GetField("TaxiOut"));
        doc["TaxiInMin"] = ToInteger(csv.GetField("TaxiIn"));
        doc["CRSArrTimeLocal"] = ToInteger(csv.GetField("CRSArrTime"));
        doc["ArrDelayMin"] = ToInteger(csv.GetField("ArrDelay"));

        // Boolean fields
        doc["Cancelled"] = ToBoolean(csv.GetField("Cancelled"));
        doc["Diverted"] = ToBoolean(csv.GetField("Diverted"));

        // Cancellation code
        var cancellationCode = Present(csv.GetField("CancellationCode"));
        doc["CancellationCode"] = cancellationCode!;

        // Cancellation reason - lookup from cancellations data
        var cancellationReason = _cancellationLookup.LookupReason(cancellationCode);
        if (cancellationReason != null)
        {
            doc["CancellationReason"] = cancellationReason;
        }

        // Time duration fields (convert to minutes as integers)
        doc["ActualElapsedTimeMin"] = ToInteger(csv.GetField("ActualElapsedTime"));
        doc["AirTimeMin"] = ToInteger(csv.GetField("AirTime"));

        // Count and distance
        doc["Flights"] = ToInteger(csv.GetField("Flights"));
        doc["DistanceMiles"] = ToInteger(csv.GetField("Distance"));

        // Delay fields (with Min suffix to match mapping)
        doc["CarrierDelayMin"] = ToInteger(csv.GetField("CarrierDelay"));
        doc["WeatherDelayMin"] = ToInteger(csv.GetField("WeatherDelay"));
        doc["NASDelayMin"] = ToInteger(csv.GetField("NASDelay"));
        doc["SecurityDelayMin"] = ToInteger(csv.GetField("SecurityDelay"));
        doc["LateAircraftDelayMin"] = ToInteger(csv.GetField("LateAircraftDelay"));

        // Geo point fields - lookup from airports data
        var originLocation = _airportLookup.LookupCoordinates(origin);
        if (originLocation != null)
        {
            doc["OriginLocation"] = originLocation;
        }

        var destLocation = _airportLookup.LookupCoordinates(dest);
        if (destLocation != null)
        {
            doc["DestLocation"] = destLocation;
        }

        return doc;
    }

    private string? Present(string? value)
    {
        if (value == null)
        {
            return null;
        }

        var trimmed = value.Trim();
        return string.IsNullOrEmpty(trimmed) ? null : trimmed;
    }

    private int? ToInteger(string? value)
    {
        value = Present(value);
        if (value == null)
        {
            return null;
        }

        if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
        {
            return (int)Math.Round(result);
        }

        return null;
    }

    private bool? ToBoolean(string? value)
    {
        value = Present(value);
        if (value == null)
        {
            return null;
        }

        var lower = value.ToLowerInvariant();
        if (new[] { "true", "t", "yes", "y" }.Contains(lower))
        {
            return true;
        }
        if (new[] { "false", "f", "no", "n" }.Contains(lower))
        {
            return false;
        }

        if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var numeric))
        {
            return numeric > 0;
        }

        return null;
    }
}
