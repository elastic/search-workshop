using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;

namespace ImportFlights;

public class AirportLookup
{
    private readonly Dictionary<string, (double Lat, double Lon)> _airports = new();
    private readonly ILogger _logger;

    public AirportLookup(string? airportsFile, ILogger logger)
    {
        _logger = logger;
        if (airportsFile != null && File.Exists(airportsFile))
        {
            LoadAirports(airportsFile);
        }
    }

    public string? LookupCoordinates(string? iataCode)
    {
        if (string.IsNullOrWhiteSpace(iataCode))
        {
            return null;
        }

        var key = iataCode.ToUpperInvariant();
        if (!_airports.TryGetValue(key, out var airport))
        {
            return null;
        }

        return $"{airport.Lat},{airport.Lon}";
    }

    private void LoadAirports(string filePath)
    {
        _logger.Info($"Loading airports from {filePath}");

        var count = 0;
        using var fileStream = File.OpenRead(filePath);
        Stream stream = filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)
            ? new GZipStream(fileStream, CompressionMode.Decompress)
            : fileStream;

        using var reader = new StreamReader(stream);
        var config = new CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false
        };
        using var csv = new CsvReader(reader, config);

        while (csv.Read())
        {
            var iata = csv.GetField(4)?.Trim();
            if (string.IsNullOrWhiteSpace(iata) || iata == "\\N")
            {
                continue;
            }

            var latStr = csv.GetField(6)?.Trim();
            var lonStr = csv.GetField(7)?.Trim();
            if (string.IsNullOrWhiteSpace(latStr) || string.IsNullOrWhiteSpace(lonStr))
            {
                continue;
            }

            if (double.TryParse(latStr, out var lat) && double.TryParse(lonStr, out var lon))
            {
                _airports[iata.ToUpperInvariant()] = (lat, lon);
                count++;
            }
        }

        _logger.Info($"Loaded {count} airports into lookup table");
    }
}

public class CancellationLookup
{
    private readonly Dictionary<string, string> _cancellations = new();
    private readonly ILogger _logger;

    public CancellationLookup(string? cancellationsFile, ILogger logger)
    {
        _logger = logger;
        if (cancellationsFile != null && File.Exists(cancellationsFile))
        {
            LoadCancellations(cancellationsFile);
        }
    }

    public string? LookupReason(string? code)
    {
        if (string.IsNullOrWhiteSpace(code))
        {
            return null;
        }

        var key = code.ToUpperInvariant();
        return _cancellations.TryGetValue(key, out var reason) ? reason : null;
    }

    private void LoadCancellations(string filePath)
    {
        _logger.Info($"Loading cancellations from {filePath}");

        var count = 0;
        using var reader = new StreamReader(filePath);
        var config = new CsvConfiguration(System.Globalization.CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true
        };
        using var csv = new CsvReader(reader, config);

        if (csv.Read())
        {
            csv.ReadHeader();
        }

        while (csv.Read())
        {
            var code = csv.GetField("Code")?.Trim();
            var description = csv.GetField("Description")?.Trim();
            if (string.IsNullOrWhiteSpace(code) || string.IsNullOrWhiteSpace(description))
            {
                continue;
            }

            _cancellations[code.ToUpperInvariant()] = description;
            count++;
        }

        _logger.Info($"Loaded {count} cancellation reasons into lookup table");
    }
}
