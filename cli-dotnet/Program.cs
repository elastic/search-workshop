using System;
using System.Collections.Generic;
using System.CommandLine;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using YamlDotNet.Serialization;

namespace ImportFlights;

public class Program
{
    private const int BatchSize = 500;

    public static async Task<int> Main(string[] args)
    {
        var startTime = DateTime.Now;

        try
        {
            var rootCommand = CreateRootCommand();
            return await rootCommand.InvokeAsync(args);
        }
        finally
        {
            var endTime = DateTime.Now;
            var duration = endTime - startTime;
            var minutes = (int)(duration.TotalMinutes);
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

    private static RootCommand CreateRootCommand()
    {
        var configOption = new Option<string>(
            aliases: new[] { "-c", "--config" },
            getDefaultValue: () => "config/elasticsearch.yml",
            description: "Path to Elasticsearch config YAML (default: config/elasticsearch.yml)");

        var mappingOption = new Option<string>(
            aliases: new[] { "-m", "--mapping" },
            getDefaultValue: () => "config/mappings-flights.json",
            description: "Path to mappings JSON (default: config/mappings-flights.json)");

        var dataDirOption = new Option<string>(
            aliases: new[] { "-d", "--data-dir" },
            getDefaultValue: () => "data",
            description: "Directory containing data files (default: data)");

        var fileOption = new Option<string>(
            aliases: new[] { "-f", "--file" },
            description: "Only import the specified file"
        );

        var allOption = new Option<bool>(
            aliases: new[] { "-a", "--all" },
            description: "Import all files found in the data directory"
        );

        var globOption = new Option<string>(
            aliases: new[] { "-g", "--glob" },
            description: "Import files matching the glob pattern"
        );

        var indexOption = new Option<string>(
            aliases: new[] { "--index" },
            getDefaultValue: () => "flights",
            description: "Override index name (default: flights)");

        var batchSizeOption = new Option<int>(
            aliases: new[] { "--batch-size" },
            getDefaultValue: () => BatchSize,
            description: "Number of documents per bulk request (default: 500)");

        var refreshOption = new Option<bool>(
            aliases: new[] { "--refresh" },
            description: "Request an index refresh after each bulk request"
        );

        var statusOption = new Option<bool>(
            aliases: new[] { "--status" },
            description: "Test connection and print cluster health status"
        );

        var deleteIndexOption = new Option<bool>(
            aliases: new[] { "--delete-index" },
            description: "Delete indices matching the index pattern and exit"
        );

        var deleteAllOption = new Option<bool>(
            aliases: new[] { "--delete-all" },
            description: "Delete all flights-* indices and exit"
        );

        var sampleOption = new Option<bool>(
            aliases: new[] { "--sample" },
            description: "Print the first document and exit"
        );

        var airportsFileOption = new Option<string>(
            aliases: new[] { "--airports-file" },
            getDefaultValue: () => "data/airports.csv.gz",
            description: "Path to airports CSV file (default: data/airports.csv.gz)");

        var cancellationsFileOption = new Option<string>(
            aliases: new[] { "--cancellations-file" },
            getDefaultValue: () => "data/cancellations.csv",
            description: "Path to cancellations CSV file (default: data/cancellations.csv)");

        var rootCommand = new RootCommand("Import flight data into Elasticsearch");

        rootCommand.AddOption(configOption);
        rootCommand.AddOption(mappingOption);
        rootCommand.AddOption(dataDirOption);
        rootCommand.AddOption(fileOption);
        rootCommand.AddOption(allOption);
        rootCommand.AddOption(globOption);
        rootCommand.AddOption(indexOption);
        rootCommand.AddOption(batchSizeOption);
        rootCommand.AddOption(refreshOption);
        rootCommand.AddOption(statusOption);
        rootCommand.AddOption(deleteIndexOption);
        rootCommand.AddOption(deleteAllOption);
        rootCommand.AddOption(sampleOption);
        rootCommand.AddOption(airportsFileOption);
        rootCommand.AddOption(cancellationsFileOption);

        rootCommand.SetHandler(async (context) =>
        {
            var configPath = context.ParseResult.GetValueForOption(configOption)!;
            var mappingPath = context.ParseResult.GetValueForOption(mappingOption)!;
            var dataDir = context.ParseResult.GetValueForOption(dataDirOption)!;
            var file = context.ParseResult.GetValueForOption(fileOption);
            var all = context.ParseResult.GetValueForOption(allOption);
            var glob = context.ParseResult.GetValueForOption(globOption);
            var index = context.ParseResult.GetValueForOption(indexOption)!;
            var batchSize = context.ParseResult.GetValueForOption(batchSizeOption);
            var refresh = context.ParseResult.GetValueForOption(refreshOption);
            var status = context.ParseResult.GetValueForOption(statusOption);
            var deleteIndex = context.ParseResult.GetValueForOption(deleteIndexOption);
            var deleteAll = context.ParseResult.GetValueForOption(deleteAllOption);
            var sample = context.ParseResult.GetValueForOption(sampleOption);
            var airportsFile = context.ParseResult.GetValueForOption(airportsFileOption);
            var cancellationsFile = context.ParseResult.GetValueForOption(cancellationsFileOption);

            var logger = new ConsoleLogger();

            // Validate options
            if (status && (deleteIndex || deleteAll))
            {
                logger.Error("Cannot use --status with --delete-index or --delete-all");
                context.ExitCode = 1;
                return;
            }

            if (deleteIndex && deleteAll)
            {
                logger.Error("Cannot use --delete-index and --delete-all together");
                context.ExitCode = 1;
                return;
            }

            if (!status && !deleteIndex && !deleteAll && !sample)
            {
                var selectionCount = new[] { file, all ? "all" : null, glob }.Count(x => x != null);
                if (selectionCount > 1)
                {
                    logger.Error("Cannot use --file, --all, and --glob together (use only one)");
                    context.ExitCode = 1;
                    return;
                }

                if (selectionCount == 0)
                {
                    logger.Error("Please provide either --file PATH, --all, or --glob PATTERN");
                    context.ExitCode = 1;
                    return;
                }
            }

            try
            {
                if (sample)
                {
                    await SampleDocument(mappingPath, dataDir, file, all, glob, airportsFile, cancellationsFile, logger);
                    return;
                }

                var config = LoadConfig(configPath);
                var client = new ElasticsearchClientWrapper(config, logger);

                if (status)
                {
                    await ReportStatus(client, logger);
                    return;
                }

                if (deleteIndex)
                {
                    await DeleteIndicesByPattern(client, logger, index);
                    return;
                }

                if (deleteAll)
                {
                    await DeleteIndicesByPattern(client, logger, "flights-*");
                    return;
                }

                var mapping = LoadMapping(mappingPath);
                var resolvedAirportsFile = ResolvePath(airportsFile!);
                var resolvedCancellationsFile = ResolvePath(cancellationsFile!);

                var loader = new FlightLoader(
                    client: client,
                    mapping: mapping,
                    index: index,
                    logger: logger,
                    batchSize: batchSize,
                    refresh: refresh,
                    airportsFile: resolvedAirportsFile,
                    cancellationsFile: resolvedCancellationsFile
                );

                var files = GetFilesToProcess(dataDir, file, all, glob);
                await loader.ImportFilesAsync(files);
            }
            catch (Exception ex)
            {
                logger.Error($"Error: {ex.Message}");
                if (ex.InnerException != null)
                {
                    logger.Error($"Inner exception: {ex.InnerException.Message}");
                }
                context.ExitCode = 1;
            }
        });

        return rootCommand;
    }

    private static Dictionary<string, object> LoadConfig(string path)
    {
        var resolvedPath = ResolvePath(path);
        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException($"Config file not found: {path} (tried: {resolvedPath})");
        }

        var deserializer = new DeserializerBuilder().Build();
        var yaml = File.ReadAllText(resolvedPath);
        var config = deserializer.Deserialize<Dictionary<string, object>>(yaml) ?? new Dictionary<string, object>();
        return config;
    }

    private static Dictionary<string, object> LoadMapping(string path)
    {
        var resolvedPath = ResolvePath(path);
        if (!File.Exists(resolvedPath))
        {
            throw new FileNotFoundException($"Mapping file not found: {path} (tried: {resolvedPath})");
        }

        var json = File.ReadAllText(resolvedPath);
        var mapping = JsonSerializer.Deserialize<Dictionary<string, object>>(json) ?? new Dictionary<string, object>();
        return mapping;
    }

    private static List<string> GetFilesToProcess(string dataDir, string? file, bool all, string? glob)
    {
        var resolvedDataDir = ResolvePath(dataDir);

        if (file != null)
        {
            return new List<string> { ResolveFilePath(file, resolvedDataDir) };
        }

        if (glob != null)
        {
            var files = Directory.GetFiles(resolvedDataDir, glob, SearchOption.TopDirectoryOnly)
                .Where(f => File.Exists(f))
                .OrderBy(f => f)
                .ToList();

            if (files.Count == 0)
            {
                // Try relative to current directory
                files = Directory.GetFiles(".", glob, SearchOption.TopDirectoryOnly)
                    .Where(f => File.Exists(f))
                    .OrderBy(f => f)
                    .ToList();
            }

            if (files.Count == 0)
            {
                throw new FileNotFoundException($"No files found matching pattern: {glob}");
            }

            return files;
        }

        // Default: all CSV, CSV.GZ, and ZIP files
        var patterns = new[] { "*.csv", "*.csv.gz", "*.zip" };
        var allFiles = patterns
            .SelectMany(p => Directory.GetFiles(resolvedDataDir, p, SearchOption.TopDirectoryOnly))
            .Where(f => File.Exists(f))
            .OrderBy(f => f)
            .ToList();

        if (allFiles.Count == 0)
        {
            throw new FileNotFoundException($"No .csv, .csv.gz, or .zip files found in {resolvedDataDir}");
        }

        return allFiles;
    }

    private static string ResolvePath(string path)
    {
        // Absolute path that already exists
        if (Path.IsPathRooted(path) && (File.Exists(path) || Directory.Exists(path)))
        {
            return path;
        }

        // Relative to current working directory
        var cwdCandidate = Path.GetFullPath(path);
        if (File.Exists(cwdCandidate) || Directory.Exists(cwdCandidate))
        {
            return cwdCandidate;
        }

        // Walk up parent directories to allow running from cli-net or bin directories
        var current = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (current != null)
        {
            var candidate = Path.Combine(current.FullName, path);
            if (File.Exists(candidate) || Directory.Exists(candidate))
            {
                return Path.GetFullPath(candidate);
            }
            current = current.Parent;
        }

        // Fallback to normalized path (will be validated by caller)
        return cwdCandidate;
    }

    private static string ResolveFilePath(string path, string dataDir)
    {
        var expanded = Path.GetFullPath(path);
        if (File.Exists(expanded))
        {
            return expanded;
        }

        var candidate = Path.GetFullPath(Path.Combine(dataDir, path));
        if (File.Exists(candidate))
        {
            return candidate;
        }

        if (File.Exists(path))
        {
            return Path.GetFullPath(path);
        }

        throw new FileNotFoundException($"File not found: {path}");
    }

    private static async Task SampleDocument(
        string mappingPath,
        string dataDir,
        string? file,
        bool all,
        string? glob,
        string? airportsFile,
        string? cancellationsFile,
        ILogger logger)
    {
        var mapping = LoadMapping(mappingPath);
        var resolvedAirportsFile = airportsFile != null ? ResolvePath(airportsFile) : null;
        var resolvedCancellationsFile = cancellationsFile != null ? ResolvePath(cancellationsFile) : null;

        var loader = new FlightLoader(
            client: null,
            mapping: mapping,
            index: "flights",
            logger: logger,
            batchSize: 1,
            refresh: false,
            airportsFile: resolvedAirportsFile,
            cancellationsFile: resolvedCancellationsFile
        );

        var files = GetFilesToProcess(dataDir, file, all, glob);
        if (files.Count == 0)
        {
            logger.Error("No files found to sample");
            Environment.Exit(1);
            return;
        }

        var doc = await loader.SampleDocumentAsync(files[0]);
        if (doc == null)
        {
            logger.Error("No document found in file");
            Environment.Exit(1);
            return;
        }

        var options = new JsonSerializerOptions { WriteIndented = true };
        Console.WriteLine(JsonSerializer.Serialize(doc, options));
    }

    private static async Task ReportStatus(ElasticsearchClientWrapper client, ILogger logger)
    {
        try
        {
            var health = await client.GetClusterHealthAsync();
            logger.Info($"Cluster status: {health.Status}");
            logger.Info($"Active shards: {health.ActiveShards}, node count: {health.NumberOfNodes}");
        }
        catch (Exception ex)
        {
            logger.Error($"Failed to retrieve cluster status: {ex.Message}");
            Environment.Exit(1);
        }
    }

    private static async Task DeleteIndicesByPattern(ElasticsearchClientWrapper client, ILogger logger, string pattern)
    {
        var patternWithWildcard = pattern.EndsWith("*") ? pattern : $"{pattern}-*";
        logger.Info($"Searching for indices matching pattern: {patternWithWildcard}");

        try
        {
            var deleted = await client.DeleteIndicesByPatternAsync(patternWithWildcard);

            if (deleted.Count == 0)
            {
                logger.Warn($"No indices found matching pattern: {patternWithWildcard}");
            }
            else
            {
                logger.Info($"Deleted {deleted.Count} index(es): {string.Join(", ", deleted)}");
            }
        }
        catch (Exception ex)
        {
            logger.Error($"Failed to delete indices matching pattern '{pattern}': {ex.Message}");
            Environment.Exit(1);
        }
    }
}

public interface ILogger
{
    void Info(string message);
    void Warn(string message);
    void Error(string message);
    void Debug(string message);
}

public class ConsoleLogger : ILogger
{
    public void Info(string message) => Console.WriteLine($"[INFO] {message}");
    public void Warn(string message) => Console.WriteLine($"[WARN] {message}");
    public void Error(string message) => Console.Error.WriteLine($"[ERROR] {message}");
    public void Debug(string message) => Console.WriteLine($"[DEBUG] {message}");
}
