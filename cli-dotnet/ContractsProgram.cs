using System.CommandLine;
using System.Text.Json;
using ImportContracts;
using ImportFlights;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace ImportContractsCli;

public class ContractsProgram
{
    public static async Task<int> Main(string[] args)
    {
        var startTime = DateTime.Now;
        try
        {
            var root = BuildCommand();
            return await root.InvokeAsync(args);
        }
        finally
        {
            var endTime = DateTime.Now;
            var duration = endTime - startTime;
            var minutes = (int)duration.TotalMinutes;
            var seconds = duration.TotalSeconds % 60;

            if (minutes > 0)
            {
                Console.WriteLine($"\nTotal time: {minutes}m {seconds:F2}s");
            }
            else
            {
                Console.WriteLine($"\nTotal time: {seconds:F2}s");
            }
        }
    }

    private static RootCommand BuildCommand()
    {
        var configOption = new Option<string>(
            aliases: new[] { "-c", "--config" },
            getDefaultValue: () => "config/elasticsearch.yml",
            description: "Path to Elasticsearch config YAML (default: config/elasticsearch.yml)");

        var mappingOption = new Option<string>(
            aliases: new[] { "-m", "--mapping" },
            getDefaultValue: () => "config/mappings-contracts.json",
            description: "Path to mappings JSON (default: config/mappings-contracts.json)");

        var pdfPathOption = new Option<string?>(
            aliases: new[] { "--pdf-path" },
            description: "Path to PDF file or directory containing PDFs (default: data)");

        var setupOnlyOption = new Option<bool>(
            aliases: new[] { "--setup-only" },
            description: "Only setup infrastructure (pipeline and index), skip PDF ingestion");

        var ingestOnlyOption = new Option<bool>(
            aliases: new[] { "--ingest-only" },
            description: "Skip setup, only ingest PDFs (assumes infrastructure exists)");

        var inferenceOption = new Option<string?>(
            aliases: new[] { "--inference-endpoint" },
            description: "Inference endpoint ID (default: .elser-2-elastic, will auto-detect if not found)");

        var statusOption = new Option<bool>(
            aliases: new[] { "--status" },
            description: "Test connection and print cluster health status");

        var root = new RootCommand("Import PDF contracts into Elasticsearch");

        root.AddOption(configOption);
        root.AddOption(mappingOption);
        root.AddOption(pdfPathOption);
        root.AddOption(setupOnlyOption);
        root.AddOption(ingestOnlyOption);
        root.AddOption(inferenceOption);
        root.AddOption(statusOption);

        root.SetHandler(async context =>
        {
            var configPath = context.ParseResult.GetValueForOption(configOption)!;
            var mappingPath = context.ParseResult.GetValueForOption(mappingOption)!;
            var pdfPath = context.ParseResult.GetValueForOption(pdfPathOption);
            var setupOnly = context.ParseResult.GetValueForOption(setupOnlyOption);
            var ingestOnly = context.ParseResult.GetValueForOption(ingestOnlyOption);
            var inferenceEndpoint = context.ParseResult.GetValueForOption(inferenceOption);
            var status = context.ParseResult.GetValueForOption(statusOption);

            if (setupOnly && ingestOnly)
            {
                Console.Error.WriteLine("Cannot use --setup-only and --ingest-only together");
                context.ExitCode = 1;
                return;
            }

            var logger = new ConsoleLogger();
            var config = LoadConfig(configPath);
            var client = new ElasticsearchClientWrapper(config, logger);

            if (status)
            {
                await ReportStatusAsync(client, logger);
                return;
            }

            var mapping = LoadMapping(mappingPath);
            var resolvedInference = string.IsNullOrWhiteSpace(inferenceEndpoint)
                ? ContractLoader.DefaultInferenceEndpoint
                : inferenceEndpoint!;

            var loader = new ContractLoader(client, mapping, logger, resolvedInference);

            if (!await loader.CheckElasticsearchAsync())
            {
                logger.Error("Cannot connect to Elasticsearch. Exiting.");
                context.ExitCode = 1;
                return;
            }

            if (!ingestOnly)
            {
                if (!await loader.CheckInferenceEndpointAsync())
                {
                    logger.Error("ELSER inference endpoint not found!");
                    logger.Error("Please deploy ELSER via Kibana or API before continuing.");
                    logger.Error("See: Management → Machine Learning → Trained Models → ELSER → Deploy");
                    context.ExitCode = 1;
                    return;
                }

                if (!await loader.CreatePipelineAsync())
                {
                    logger.Error("Failed to create pipeline. Exiting.");
                    context.ExitCode = 1;
                    return;
                }

                if (!await loader.CreateIndexAsync())
                {
                    logger.Error("Failed to create index. Exiting.");
                    context.ExitCode = 1;
                    return;
                }
            }

            if (!setupOnly)
            {
                var start = DateTime.UtcNow;
                var resolvedPdfPath = pdfPath ?? ResolvePath("data");

                if (!await loader.IngestPdfsAsync(resolvedPdfPath))
                {
                    logger.Error("PDF ingestion had errors.");
                    context.ExitCode = 1;
                    return;
                }

                var elapsed = DateTime.UtcNow - start;
                logger.Info($"Total ingestion time: {elapsed.TotalSeconds:F2} seconds");

                await loader.VerifyIngestionAsync();
            }
        });

        return root;
    }

    private static Dictionary<string, object> LoadConfig(string path)
    {
        var resolvedPath = ResolvePath(path);
        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException($"Config file not found: {path} (tried: {resolvedPath})");
        }

        var content = File.ReadAllText(resolvedPath);
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        var yamlObject = deserializer.Deserialize<Dictionary<object, object>>(content) ?? new Dictionary<object, object>();
        return yamlObject.ToDictionary(k => k.Key.ToString()!, v => v.Value ?? string.Empty);
    }

    private static Dictionary<string, object> LoadMapping(string path)
    {
        var resolvedPath = ResolvePath(path);
        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException($"Mapping file not found: {path} (tried: {resolvedPath})");
        }

        var content = File.ReadAllText(resolvedPath);
        return JsonSerializer.Deserialize<Dictionary<string, object>>(content, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        }) ?? new Dictionary<string, object>();
    }

    private static async Task ReportStatusAsync(ElasticsearchClientWrapper client, ILogger logger)
    {
        try
        {
            var status = await client.GetClusterHealthAsync();
            logger.Info($"Cluster status: {status.Status}");
            logger.Info($"Active shards: {status.ActiveShards}, node count: {status.NumberOfNodes}");
        }
        catch (Exception ex)
        {
            logger.Error($"Failed to retrieve cluster status: {ex.Message}");
            Environment.Exit(1);
        }
    }

    private static string ResolvePath(string path)
    {
        if (Path.IsPathRooted(path))
        {
            return path;
        }

        if (File.Exists(path) || Directory.Exists(path))
        {
            return Path.GetFullPath(path);
        }

        var scriptDir = AppContext.BaseDirectory;
        var workspaceRoot = Path.GetFullPath(Path.Combine(scriptDir, ".."));
        return Path.Combine(workspaceRoot, path);
    }
}
