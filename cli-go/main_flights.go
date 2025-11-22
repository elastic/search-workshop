//go:build !contracts

package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

// main_flights.go - Entry point for flights import
// This file provides the main() function for the import_flights binary.
// Build with: go build -o import_flights

func main() {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		minutes := int(duration.Minutes())
		seconds := duration.Seconds() - float64(minutes*60)
		if minutes > 0 {
			fmt.Printf("\nTotal time: %dm %.2fs\n", minutes, seconds)
		} else {
			fmt.Printf("\nTotal time: %.2fs\n", seconds)
		}
	}()

	options := parseOptions()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	if options.Sample {
		if err := sampleDocument(options, logger); err != nil {
			logger.Fatalf("Error: %v", err)
		}
		return
	}

	config, err := loadConfig(options.Config)
	if err != nil {
		logger.Fatalf("Error loading config: %v", err)
	}

	client, err := NewElasticsearchClient(config, logger)
	if err != nil {
		logger.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	if options.Status {
		if err := reportStatus(client, logger); err != nil {
			logger.Fatalf("Error: %v", err)
		}
		return
	}

	if options.DeleteIndex {
		if err := deleteIndicesByPattern(client, logger, options.Index); err != nil {
			logger.Fatalf("Error: %v", err)
		}
		return
	}

	if options.DeleteAll {
		if err := deleteIndicesByPattern(client, logger, "flights-*"); err != nil {
			logger.Fatalf("Error: %v", err)
		}
		return
	}

	mapping, err := loadMapping(options.Mapping)
	if err != nil {
		logger.Fatalf("Error loading mapping: %v", err)
	}

	resolvedAirportsFile := ""
	if options.AirportsFile != "" {
		resolvedAirportsFile = resolvePath(options.AirportsFile)
	}

	resolvedCancellationsFile := ""
	if options.CancellationsFile != "" {
		resolvedCancellationsFile = resolvePath(options.CancellationsFile)
	}

	loader, err := NewFlightLoader(client, mapping, options.Index, logger, options.BatchSize, options.Refresh, resolvedAirportsFile, resolvedCancellationsFile)
	if err != nil {
		logger.Fatalf("Error creating flight loader: %v", err)
	}

	files, err := filesToProcess(options)
	if err != nil {
		logger.Fatalf("Error determining files to process: %v", err)
	}

	if err := loader.ImportFiles(files); err != nil {
		logger.Fatalf("Error importing files: %v", err)
	}
}
