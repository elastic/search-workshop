using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using ImportFlights;

namespace ImportContracts;

public class ContractLoader
{
    public const string EsIndex = "contracts";
    public const string PipelineName = "pdf_pipeline";
    public const string DefaultInferenceEndpoint = ".elser-2-elastic";

    private readonly ElasticsearchClientWrapper _client;
    private readonly Dictionary<string, object> _mapping;
    private readonly ILogger _logger;
    private string _inferenceEndpoint;
    private int _indexedCount;

    public ContractLoader(ElasticsearchClientWrapper client, Dictionary<string, object> mapping, ILogger logger, string? inferenceEndpoint = null)
    {
        _client = client;
        _mapping = mapping;
        _logger = logger;
        _inferenceEndpoint = string.IsNullOrWhiteSpace(inferenceEndpoint) ? DefaultInferenceEndpoint : inferenceEndpoint!;
        _indexedCount = 0;
    }

    public async Task<bool> CheckElasticsearchAsync()
    {
        try
        {
            var health = await _client.GetClusterHealthAsync();
            _logger.Info($"Cluster: {health.ClusterName ?? "unknown"}");
            _logger.Info($"Status: {health.Status}");
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error($"Connection error: {ex.Message}");
            return false;
        }
    }

    public async Task<bool> CheckInferenceEndpointAsync()
    {
        try
        {
            var endpoints = await _client.GetInferenceEndpointsAsync();

            // First, try to find the specified endpoint
            var found = endpoints.FirstOrDefault(ep =>
                MatchesEndpoint(ep, _inferenceEndpoint));

            if (found != null)
            {
                _logger.Info($"Found inference endpoint: {_inferenceEndpoint}");
                return true;
            }

            // Auto-detect ELSER endpoints
            var elserEndpoints = endpoints
                .Select(ep => new { Id = EndpointId(ep), Endpoint = ep })
                .Where(ep => ep.Id != null && ep.Id.Contains("elser", StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (elserEndpoints.Count > 0)
            {
                var preferred = elserEndpoints.FirstOrDefault(ep =>
                    ep.Id.Contains(".elser-2-", StringComparison.OrdinalIgnoreCase) ||
                    ep.Id.Contains(".elser_model_2", StringComparison.OrdinalIgnoreCase));

                _inferenceEndpoint = preferred?.Id ?? elserEndpoints.First().Id!;
                _logger.Warn($"Specified endpoint not found, using auto-detected: {_inferenceEndpoint}");
                return true;
            }

            _logger.Error($"Inference endpoint '{_inferenceEndpoint}' not found");
            _logger.Info("Available endpoints:");
            foreach (var ep in endpoints)
            {
                _logger.Info($"  - {EndpointId(ep) ?? "<unknown>"}");
            }
            return false;
        }
        catch (Exception ex)
        {
            _logger.Warn($"Error checking inference endpoint: {ex.Message}");
            _logger.Warn("Continuing anyway...");
            return true;
        }
    }

    public async Task<bool> CreatePipelineAsync()
    {
        var pipelineConfig = new Dictionary<string, object>
        {
            ["description"] = "Extract text from PDF - semantic_text field handles chunking and embeddings",
            ["processors"] = new List<Dictionary<string, object>>
            {
                new()
                {
                    ["attachment"] = new Dictionary<string, object>
                    {
                        ["field"] = "data",
                        ["target_field"] = "attachment",
                        ["remove_binary"] = true
                    }
                },
                new()
                {
                    ["set"] = new Dictionary<string, object>
                    {
                        ["field"] = "semantic_content",
                        ["copy_from"] = "attachment.content",
                        ["ignore_empty_value"] = true
                    }
                },
                new()
                {
                    ["remove"] = new Dictionary<string, object>
                    {
                        ["field"] = "data",
                        ["ignore_missing"] = true
                    }
                },
                new()
                {
                    ["set"] = new Dictionary<string, object>
                    {
                        ["field"] = "upload_date",
                        ["value"] = "{{ _ingest.timestamp }}"
                    }
                }
            }
        };

        try
        {
            await _client.CreatePipelineAsync(PipelineName, pipelineConfig);
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error($"Error creating pipeline: {ex.Message}");
            return false;
        }
    }

    public async Task<bool> CreateIndexAsync()
    {
        try
        {
            if (await _client.IndexExistsAsync(EsIndex))
            {
                _logger.Info($"Deleting existing index '{EsIndex}' before import");
                if (await _client.DeleteIndexAsync(EsIndex))
                {
                    _logger.Info($"Index '{EsIndex}' deleted");
                }
                else
                {
                    _logger.Warn($"Failed to delete index '{EsIndex}'");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Warn($"Error checking/deleting index: {ex.Message}");
        }

        // Update mapping with detected inference endpoint
        var mappingWithInference = NormalizeToDictionary(_mapping);
        var mappingsObj = EnsureDictionary(mappingWithInference, "mappings");
        var propsObj = EnsureDictionary(mappingsObj, "properties");
        var semanticObj = EnsureDictionary(propsObj, "semantic_content");
        semanticObj["inference_id"] = _inferenceEndpoint;

        _logger.Info($"Creating index: {EsIndex}");
        try
        {
            await _client.CreateIndexAsync(EsIndex, mappingWithInference);
            _logger.Info($"Successfully created index: {EsIndex}");
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error($"Error creating index: {ex.Message}");
            return false;
        }
    }

    public async Task<bool> IngestPdfsAsync(string pdfPath)
    {
        var pdfFiles = GetPdfFiles(pdfPath);
        if (pdfFiles.Count == 0)
        {
            _logger.Error("No PDF files to process");
            return false;
        }

        _logger.Info($"Processing {pdfFiles.Count} PDF file(s)...");

        var success = 0;
        var failed = 0;

        foreach (var pdfFile in pdfFiles)
        {
            if (await IndexPdfAsync(pdfFile))
            {
                success++;
            }
            else
            {
                failed++;
            }
        }

        _logger.Info($"Indexed {success} of {pdfFiles.Count} file(s)");
        if (failed > 0)
        {
            _logger.Warn($"Failed: {failed}");
        }

        return failed == 0;
    }

    public async Task VerifyIngestionAsync()
    {
        await Task.Delay(1000);
        try
        {
            var count = await _client.CountDocumentsAsync(EsIndex);
            _logger.Info($"Index '{EsIndex}' contains {count} document(s)");

            if (_indexedCount > 0 && count == 0)
            {
                _logger.Warn($"Warning: Expected {_indexedCount} document(s) but count shows 0. Documents may have failed during pipeline processing.");
            }
        }
        catch (Exception ex)
        {
            _logger.Warn($"Could not verify document count: {ex.Message}");
        }
    }

    private static string? EndpointId(Dictionary<string, object> endpoint)
    {
        foreach (var key in new[] { "inference_id", "endpoint", "id", "name" })
        {
            if (endpoint.TryGetValue(key, out var value) && value is string s && !string.IsNullOrWhiteSpace(s))
            {
                return s;
            }
        }
        return null;
    }

    private static bool MatchesEndpoint(Dictionary<string, object> endpoint, string target)
    {
        var id = EndpointId(endpoint);
        return id != null && id.Equals(target, StringComparison.Ordinal);
    }

    private static Dictionary<string, object> DeepCopy(Dictionary<string, object> source)
    {
        var json = JsonSerializer.Serialize(source);
        return JsonSerializer.Deserialize<Dictionary<string, object>>(json)!;
    }

    private static Dictionary<string, object> NormalizeToDictionary(Dictionary<string, object> source)
    {
        var normalized = new Dictionary<string, object>();
        foreach (var kvp in source)
        {
            normalized[kvp.Key] = NormalizeValue(kvp.Value);
        }
        return normalized;
    }

    private static Dictionary<string, object>? GetDictionary(Dictionary<string, object>? parent, string key)
    {
        if (parent == null) return null;
        if (!parent.TryGetValue(key, out var value) || value == null) return null;
        return NormalizeValue(value) as Dictionary<string, object>;
    }

    private static Dictionary<string, object> EnsureDictionary(Dictionary<string, object> parent, string key)
    {
        var existing = GetDictionary(parent, key);
        if (existing != null)
        {
            parent[key] = existing;
            return existing;
        }

        var created = new Dictionary<string, object>();
        parent[key] = created;
        return created;
    }

    private static object? NormalizeValue(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case Dictionary<string, object> dict:
                return NormalizeToDictionary(dict);
            case List<object> list:
                return list.Select(NormalizeValue).ToList();
            case JsonElement element:
                return ConvertJsonElement(element);
            default:
                return value;
        }
    }

    private static object? ConvertJsonElement(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var dict = new Dictionary<string, object>();
                foreach (var prop in element.EnumerateObject())
                {
                    dict[prop.Name] = ConvertJsonElement(prop.Value)!;
                }
                return dict;
            case JsonValueKind.Array:
                var list = new List<object>();
                foreach (var item in element.EnumerateArray())
                {
                    var converted = ConvertJsonElement(item);
                    if (converted != null)
                    {
                        list.Add(converted);
                    }
                }
                return list;
            case JsonValueKind.String:
                return element.GetString() ?? string.Empty;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var l)) return l;
                if (element.TryGetDouble(out var d)) return d;
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

    private static string ExtractAirlineName(string filename)
    {
        var lower = filename.ToLowerInvariant();
        if (lower.Contains("american"))
        {
            return "American Airlines";
        }
        if (lower.Contains("southwest"))
        {
            return "Southwest";
        }
        if (lower.Contains("united"))
        {
            return "United";
        }
        if (lower.Contains("delta") || lower.Contains("dl-"))
        {
            return "Delta";
        }
        return "Unknown";
    }

    private List<string> GetPdfFiles(string path)
    {
        if (!File.Exists(path) && !Directory.Exists(path))
        {
            _logger.Error($"Path '{path}' does not exist");
            return new List<string>();
        }

        if (File.Exists(path))
        {
            if (string.Equals(Path.GetExtension(path), ".pdf", StringComparison.OrdinalIgnoreCase))
            {
                return new List<string> { path };
            }

            _logger.Error($"'{path}' is not a PDF file");
            return new List<string>();
        }

        var pdfs = Directory.GetFiles(path, "*.pdf").OrderBy(p => p).ToList();
        if (pdfs.Count == 0)
        {
            _logger.Warn($"No PDF files found in directory '{path}'");
        }
        return pdfs;
    }

    private async Task<bool> IndexPdfAsync(string pdfPath)
    {
        var filename = Path.GetFileName(pdfPath);
        var airline = ExtractAirlineName(filename);

        try
        {
            var pdfBytes = await File.ReadAllBytesAsync(pdfPath);
            var encoded = Convert.ToBase64String(pdfBytes);

            var document = new Dictionary<string, object>
            {
                ["data"] = encoded,
                ["filename"] = filename,
                ["airline"] = airline
            };

            await _client.IndexDocumentAsync(EsIndex, document, PipelineName);

            _logger.Info($"Indexed: {filename} (airline: {airline})");
            _indexedCount += 1;
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error($"Error processing {filename}: {ex.Message}");
            return false;
        }
    }
}
