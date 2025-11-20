package com.elastic;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CancellationLookup {
    private final Logger logger;
    private final Map<String, String> cancellations;

    public CancellationLookup(String cancellationsFile, Logger logger) {
        this.logger = logger;
        this.cancellations = new HashMap<>();
        
        if (cancellationsFile != null && new File(cancellationsFile).exists()) {
            loadCancellations(cancellationsFile);
        }
    }

    public String lookupReason(String code) {
        if (code == null || code.trim().isEmpty()) {
            return null;
        }

        return cancellations.get(code.toUpperCase());
    }

    private void loadCancellations(String filePath) {
        logger.info("Loading cancellations from " + filePath);

        int count = 0;
        try (Reader reader = new FileReader(filePath, java.nio.charset.StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            for (CSVRecord record : parser) {
                String code = record.get("Code");
                String description = record.get("Description");
                
                if (code != null && description != null) {
                    code = code.trim();
                    description = description.trim();
                    
                    if (!code.isEmpty() && !description.isEmpty()) {
                        cancellations.put(code.toUpperCase(), description);
                        count++;
                    }
                }
            }
        } catch (IOException e) {
            logger.warning("Failed to load cancellations from " + filePath + ": " + e.getMessage());
        }

        logger.info("Loaded " + count + " cancellation reasons into lookup table");
    }
}
