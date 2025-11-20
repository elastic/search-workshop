package com.elastic;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class AirportLookup {
    private final Logger logger;
    private final Map<String, Airport> airports;

    public AirportLookup(String airportsFile, Logger logger) {
        this.logger = logger;
        this.airports = new HashMap<>();
        
        if (airportsFile != null && new File(airportsFile).exists()) {
            loadAirports(airportsFile);
        }
    }

    public String lookupCoordinates(String iataCode) {
        if (iataCode == null || iataCode.trim().isEmpty()) {
            return null;
        }

        Airport airport = airports.get(iataCode.toUpperCase());
        if (airport == null) {
            return null;
        }

        return airport.lat + "," + airport.lon;
    }

    private void loadAirports(String filePath) {
        logger.info("Loading airports from " + filePath);

        int count = 0;
        try (InputStream fileStream = new FileInputStream(filePath);
             InputStream gzStream = filePath.toLowerCase().endsWith(".gz") 
                 ? new GZIPInputStream(fileStream) 
                 : fileStream;
             Reader reader = new InputStreamReader(gzStream, "UTF-8");
             CSVParser parser = CSVFormat.DEFAULT.parse(reader)) {

            for (CSVRecord record : parser) {
                if (record.size() < 8) {
                    continue;
                }

                String iata = record.get(4) != null ? record.get(4).trim() : null;
                if (iata == null || iata.isEmpty() || iata.equals("\\N")) {
                    continue;
                }

                String latStr = record.get(6) != null ? record.get(6).trim() : null;
                String lonStr = record.get(7) != null ? record.get(7).trim() : null;
                
                if (latStr == null || latStr.isEmpty() || lonStr == null || lonStr.isEmpty()) {
                    continue;
                }

                try {
                    double lat = Double.parseDouble(latStr);
                    double lon = Double.parseDouble(lonStr);
                    airports.put(iata.toUpperCase(), new Airport(lat, lon));
                    count++;
                } catch (NumberFormatException e) {
                    // Skip invalid coordinates
                    continue;
                }
            }
        } catch (IOException e) {
            logger.warning("Failed to load airports from " + filePath + ": " + e.getMessage());
        }

        logger.info("Loaded " + count + " airports into lookup table");
    }

    private static class Airport {
        final double lat;
        final double lon;

        Airport(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }
}
