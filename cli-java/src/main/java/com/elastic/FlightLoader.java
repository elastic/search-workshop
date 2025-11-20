package com.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class FlightLoader {
    public static final int BATCH_SIZE = 500;

    private final ElasticsearchClientWrapper client;
    private final Map<String, Object> mapping;
    private final String indexPrefix;
    private final Logger logger;
    private final int batchSize;
    private final boolean refresh;
    private final AirportLookup airportLookup;
    private final CancellationLookup cancellationLookup;
    private final Set<String> ensuredIndices;
    private long loadedRecords;
    private long totalRecords;
    private final ObjectMapper objectMapper;

    public FlightLoader(ElasticsearchClientWrapper client, Map<String, Object> mapping, String index,
                       Logger logger, int batchSize, boolean refresh,
                       String airportsFile, String cancellationsFile) {
        this.client = client;
        this.mapping = mapping;
        this.indexPrefix = index;
        this.logger = logger;
        this.batchSize = batchSize;
        this.refresh = refresh;
        this.airportLookup = new AirportLookup(airportsFile, logger);
        this.cancellationLookup = new CancellationLookup(cancellationsFile, logger);
        this.ensuredIndices = new HashSet<>();
        this.loadedRecords = 0;
        this.totalRecords = 0;
        this.objectMapper = new ObjectMapper();
    }

    public void ensureIndex(String indexName) throws IOException {
        if (client == null) {
            return;
        }

        if (ensuredIndices.contains(indexName)) {
            logger.fine("Index " + indexName + " already ensured in this session");
            return;
        }

        // Delete index if it exists before creating a new one
        if (client.indexExists(indexName)) {
            logger.info("Deleting existing index '" + indexName + "' before import");
            if (client.deleteIndex(indexName)) {
                logger.info("Index '" + indexName + "' deleted");
            } else {
                logger.warning("Failed to delete index '" + indexName + "'");
            }
        }

        logger.info("Creating index: " + indexName);
        client.createIndex(indexName, mapping);
        ensuredIndices.add(indexName);
        logger.info("Successfully created index: " + indexName);
    }

    public void importFiles(List<String> files) throws IOException {
        logger.info("Counting records in " + files.size() + " file(s)...");
        totalRecords = countTotalRecordsFast(files);
        logger.info("Total records to import: " + formatNumber(totalRecords));
        logger.info("Importing " + files.size() + " file(s)...");

        for (String filePath : files) {
            importFile(filePath);
        }

        System.out.println();
        logger.info("Import complete: " + formatNumber(loadedRecords) + " of " + 
                   formatNumber(totalRecords) + " records loaded");
    }

    public Map<String, Object> sampleDocument(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.isFile()) {
            logger.warning("Skipping " + filePath + " (not a regular file)");
            return null;
        }

        logger.info("Sampling first document from " + filePath);

        try (Reader reader = getDataReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            Iterator<CSVRecord> iterator = parser.iterator();
            if (!iterator.hasNext()) {
                return null;
            }

            return transformRow(iterator.next());
        }
    }

    private String formatNumber(long number) {
        return String.format("%,d", number);
    }

    private long countTotalRecordsFast(List<String> files) {
        long total = 0;
        for (String filePath : files) {
            File file = new File(filePath);
            if (!file.isFile()) {
                continue;
            }

            long lineCount = countLinesFast(filePath);
            // Subtract 1 for CSV header
            total += Math.max(lineCount - 1, 0);
        }
        return total;
    }

    private long countLinesFast(String filePath) {
        try {
            if (filePath.toLowerCase().endsWith(".zip")) {
                String entry = csvEntryInZip(filePath);
                if (entry == null) {
                    return 0;
                }
                return countLinesInZipEntry(filePath, entry);
            } else if (filePath.toLowerCase().endsWith(".gz")) {
                return countLinesInGzip(filePath);
            } else {
                try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
                    return lines.count();
                }
            }
        } catch (Exception e) {
            logger.warning("Failed to count lines in " + filePath + ": " + e.getMessage());
            return 0;
        }
    }

    private long countLinesInZipEntry(String zipPath, String entry) throws IOException {
        try (ZipFile zipFile = new ZipFile(zipPath)) {
            ZipEntry zipEntry = zipFile.getEntry(entry);
            if (zipEntry == null) {
                return 0;
            }
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(zipFile.getInputStream(zipEntry), StandardCharsets.UTF_8))) {
                return reader.lines().count();
            }
        }
    }

    private long countLinesInGzip(String filePath) throws IOException {
        try (InputStream fileStream = new FileInputStream(filePath);
             InputStream gzStream = new GZIPInputStream(fileStream);
             BufferedReader reader = new BufferedReader(
                 new InputStreamReader(gzStream, StandardCharsets.UTF_8))) {
            return reader.lines().count();
        }
    }

    private void importFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.isFile()) {
            logger.warning("Skipping " + filePath + " (not a regular file)");
            return;
        }

        logger.info("Importing " + filePath);

        // Extract year and month from filename if available
        String[] yearMonth = extractYearMonthFromFilename(filePath);
        String fileYear = yearMonth[0];
        String fileMonth = yearMonth[1];

        // Buffer documents by index name (year-month)
        Map<String, IndexBuffer> indexBuffers = new HashMap<>();
        long indexedDocs = 0;
        long processedRows = 0;

        try (Reader reader = getDataReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord row : parser) {
                processedRows++;

                Map<String, Object> doc = transformRow(row);
                if (doc == null || doc.isEmpty()) {
                    continue;
                }

                // Extract index name from timestamp or filename
                String timestamp = (String) doc.get("@timestamp");
                String indexName = extractIndexName(timestamp, fileYear, fileMonth);
                if (indexName == null) {
                    String timestampRaw = row.get("@timestamp") != null ? row.get("@timestamp") : row.get("FlightDate");
                    logger.warning("Skipping document - missing or invalid timestamp. Raw value: " + 
                                 timestampRaw + ", parsed timestamp: " + timestamp + 
                                 ". Row " + processedRows + ": Origin=" + row.get("Origin") + 
                                 ", Dest=" + row.get("Dest") + ", Airline=" + row.get("Reporting_Airline"));
                    continue;
                }

                // Remove null values
                doc.values().removeIf(Objects::isNull);

                // Ensure index exists
                ensureIndex(indexName);

                // Initialize buffer for this index if needed
                IndexBuffer buffer = indexBuffers.computeIfAbsent(indexName, k -> new IndexBuffer());

                // Add document to buffer
                try {
                    buffer.lines.add(objectMapper.writeValueAsString(
                        Map.of("index", Map.of("_index", indexName))));
                    buffer.lines.add(objectMapper.writeValueAsString(doc));
                    buffer.count++;
                } catch (Exception e) {
                    throw new IOException("Failed to serialize document", e);
                }

                // Flush if buffer is full
                if (buffer.count >= batchSize) {
                    indexedDocs += flushIndex(indexName, buffer.lines, buffer.count);
                    buffer.lines.clear();
                    buffer.count = 0;
                }
            }
        }

        // Flush any remaining buffers
        for (Map.Entry<String, IndexBuffer> entry : indexBuffers.entrySet()) {
            IndexBuffer buffer = entry.getValue();
            if (buffer.count > 0) {
                indexedDocs += flushIndex(entry.getKey(), buffer.lines, buffer.count);
            }
        }

        logger.info("Finished " + filePath + " (rows processed: " + processedRows + 
                   ", documents indexed: " + indexedDocs + ")");
    }

    private int flushIndex(String indexName, List<String> lines, int docCount) throws IOException {
        String payload = String.join("\n", lines) + "\n";
        client.bulk(indexName, payload, refresh);

        loadedRecords += docCount;
        if (totalRecords > 0) {
            double percentage = (loadedRecords * 100.0 / totalRecords);
            System.out.print("\r" + formatNumber(loadedRecords) + " of " + 
                           formatNumber(totalRecords) + " records loaded (" + 
                           String.format("%.1f", percentage) + "%)");
        } else {
            System.out.print("\r" + formatNumber(loadedRecords) + " records loaded");
        }
        System.out.flush();

        return docCount;
    }

    private Reader getDataReader(String filePath) throws IOException {
        File file = new File(filePath);
        String lowerPath = filePath.toLowerCase();

        if (lowerPath.endsWith(".zip")) {
            String entry = csvEntryInZip(filePath);
            if (entry == null) {
                throw new IOException("No CSV entry found in " + filePath);
            }

            ZipFile zipFile = new ZipFile(filePath);
            ZipEntry zipEntry = zipFile.getEntry(entry);
            if (zipEntry == null) {
                zipFile.close();
                throw new IOException("Entry " + entry + " not found in " + filePath);
            }

            return new InputStreamReader(zipFile.getInputStream(zipEntry), StandardCharsets.UTF_8) {
                @Override
                public void close() throws IOException {
                    super.close();
                    zipFile.close();
                }
            };
        } else if (lowerPath.endsWith(".gz")) {
            final InputStream fileStream = new FileInputStream(filePath);
            final InputStream gzStream = new GZIPInputStream(fileStream);
            return new InputStreamReader(gzStream, StandardCharsets.UTF_8) {
                @Override
                public void close() throws IOException {
                    super.close();
                    gzStream.close();
                    fileStream.close();
                }
            };
        } else {
            return new FileReader(filePath, StandardCharsets.UTF_8);
        }
    }

    private String csvEntryInZip(String zipPath) throws IOException {
        try (ZipFile zipFile = new ZipFile(zipPath)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String name = entry.getName().toLowerCase();
                if (name.endsWith(".csv")) {
                    return entry.getName();
                }
            }
        }
        return null;
    }

    private String extractIndexName(String timestamp, String fileYear, String fileMonth) {
        // If filename specifies month, use that format: flights-<year>-<month>
        if (fileYear != null && fileMonth != null) {
            return indexPrefix + "-" + fileYear + "-" + fileMonth;
        }

        // If filename specifies only year, use that format: flights-<year>
        if (fileYear != null) {
            return indexPrefix + "-" + fileYear;
        }

        // Otherwise, derive from timestamp
        if (timestamp == null) {
            return null;
        }

        // Parse YYYY-MM-DD format and extract YYYY-MM or YYYY
        Pattern pattern = Pattern.compile("^(\\d{4})-(\\d{2})-\\d{2}");
        Matcher matcher = pattern.matcher(timestamp);
        if (matcher.matches()) {
            String year = matcher.group(1);
            // Since filename didn't specify month, use year-only format
            return indexPrefix + "-" + year;
        } else {
            logger.warning("Unable to parse timestamp format: " + timestamp);
            return null;
        }
    }

    private String[] extractYearMonthFromFilename(String filePath) {
        Path path = Paths.get(filePath);
        String basename = path.getFileName().toString();

        // Remove extensions (.gz, .csv, .zip) - handle multiple extensions
        while (true) {
            String newBasename = basename.replaceAll("(?i)\\.(gz|csv|zip)$", "");
            if (newBasename.equals(basename)) {
                break;
            }
            basename = newBasename;
        }

        // Try pattern: flights-YYYY-MM (e.g., flights-2024-07)
        Pattern pattern1 = Pattern.compile("-?(\\d{4})-(\\d{2})$");
        Matcher matcher1 = pattern1.matcher(basename);
        if (matcher1.find()) {
            return new String[]{matcher1.group(1), matcher1.group(2)};
        }

        // Try pattern: flights-YYYY (e.g., flights-2019)
        Pattern pattern2 = Pattern.compile("-?(\\d{4})$");
        Matcher matcher2 = pattern2.matcher(basename);
        if (matcher2.find()) {
            return new String[]{matcher2.group(1), null};
        }

        // No pattern matched
        return new String[]{null, null};
    }

    private Map<String, Object> transformRow(CSVRecord row) {
        Map<String, Object> doc = new HashMap<>();

        // Get timestamp - prefer @timestamp column if it exists, otherwise use FlightDate
        String timestamp = present(row.get("@timestamp"));
        if (timestamp == null) {
            timestamp = present(row.get("FlightDate"));
        }

        // Flight ID - construct from date, airline, flight number, origin, and destination
        String flightDate = timestamp;
        String reportingAirline = present(row.get("Reporting_Airline"));
        String flightNumber = present(row.get("Flight_Number_Reporting_Airline"));
        String origin = present(row.get("Origin"));
        String dest = present(row.get("Dest"));

        if (flightDate != null && reportingAirline != null && flightNumber != null && 
            origin != null && dest != null) {
            doc.put("FlightID", flightDate + "_" + reportingAirline + "_" + 
                   flightNumber + "_" + origin + "_" + dest);
        }

        // @timestamp field - use timestamp directly (required for index routing)
        doc.put("@timestamp", timestamp);

        // Direct mappings from CSV to mapping field names
        doc.put("Reporting_Airline", reportingAirline);
        doc.put("Tail_Number", present(row.get("Tail_Number")));
        doc.put("Flight_Number", flightNumber);
        doc.put("Origin", origin);
        doc.put("Dest", dest);

        // Time fields - convert to integers (minutes or time in HHMM format)
        doc.put("CRSDepTimeLocal", toInteger(row.get("CRSDepTime")));
        doc.put("DepDelayMin", toInteger(row.get("DepDelay")));
        doc.put("TaxiOutMin", toInteger(row.get("TaxiOut")));
        doc.put("TaxiInMin", toInteger(row.get("TaxiIn")));
        doc.put("CRSArrTimeLocal", toInteger(row.get("CRSArrTime")));
        doc.put("ArrDelayMin", toInteger(row.get("ArrDelay")));

        // Boolean fields
        doc.put("Cancelled", toBoolean(row.get("Cancelled")));
        doc.put("Diverted", toBoolean(row.get("Diverted")));

        // Cancellation code
        String cancellationCode = present(row.get("CancellationCode"));
        doc.put("CancellationCode", cancellationCode);

        // Cancellation reason - lookup from cancellations data
        String cancellationReason = cancellationLookup.lookupReason(cancellationCode);
        if (cancellationReason != null) {
            doc.put("CancellationReason", cancellationReason);
        }

        // Time duration fields (convert to minutes as integers)
        doc.put("ActualElapsedTimeMin", toInteger(row.get("ActualElapsedTime")));
        doc.put("AirTimeMin", toInteger(row.get("AirTime")));

        // Count and distance
        doc.put("Flights", toInteger(row.get("Flights")));
        doc.put("DistanceMiles", toInteger(row.get("Distance")));

        // Delay fields (with Min suffix to match mapping)
        doc.put("CarrierDelayMin", toInteger(row.get("CarrierDelay")));
        doc.put("WeatherDelayMin", toInteger(row.get("WeatherDelay")));
        doc.put("NASDelayMin", toInteger(row.get("NASDelay")));
        doc.put("SecurityDelayMin", toInteger(row.get("SecurityDelay")));
        doc.put("LateAircraftDelayMin", toInteger(row.get("LateAircraftDelay")));

        // Geo point fields - lookup from airports data
        String originLocation = airportLookup.lookupCoordinates(origin);
        if (originLocation != null) {
            doc.put("OriginLocation", originLocation);
        }

        String destLocation = airportLookup.lookupCoordinates(dest);
        if (destLocation != null) {
            doc.put("DestLocation", destLocation);
        }

        return doc;
    }

    private String present(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private Integer toInteger(String value) {
        value = present(value);
        if (value == null) {
            return null;
        }

        try {
            return (int) Math.round(Double.parseDouble(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Boolean toBoolean(String value) {
        value = present(value);
        if (value == null) {
            return null;
        }

        String lower = value.toLowerCase();
        if (lower.equals("true") || lower.equals("t") || lower.equals("yes") || lower.equals("y")) {
            return true;
        }
        if (lower.equals("false") || lower.equals("f") || lower.equals("no") || lower.equals("n")) {
            return false;
        }

        try {
            double numeric = Double.parseDouble(value);
            return numeric > 0;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static class IndexBuffer {
        List<String> lines = new ArrayList<>();
        int count = 0;
    }
}
