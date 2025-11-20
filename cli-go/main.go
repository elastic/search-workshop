package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"gopkg.in/yaml.v3"
)

const (
	defaultBatchSize = 500
)

type Config struct {
	Endpoint  string            `yaml:"endpoint"`
	User      string            `yaml:"user"`
	Password  string            `yaml:"password"`
	APIKey    string            `yaml:"api_key"`
	Headers   map[string]string `yaml:"headers"`
	SSLVerify bool              `yaml:"ssl_verify"`
	KibanaEP  string            `yaml:"kibana_endpoint"`
}

type ElasticsearchClient struct {
	client   *elasticsearch.Client
	endpoint string
	logger   *log.Logger
}

func NewElasticsearchClient(config Config, logger *log.Logger) (*ElasticsearchClient, error) {
	if config.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required in the Elasticsearch config")
	}

	cfg := elasticsearch.Config{
		Addresses: []string{config.Endpoint},
	}

	// Handle authentication
	if config.APIKey != "" {
		cfg.APIKey = config.APIKey
	} else if config.User != "" && config.Password != "" {
		cfg.Username = config.User
		cfg.Password = config.Password
	}

	// Note: SSL verification is handled by the transport layer
	// The official client uses the default HTTP client which respects SSL settings
	// For custom SSL configuration, you would need to set up a custom http.Transport

	// Handle custom headers
	if len(config.Headers) > 0 {
		cfg.Header = make(map[string][]string)
		for k, v := range config.Headers {
			cfg.Header[k] = []string{v}
		}
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	return &ElasticsearchClient{
		client:   client,
		endpoint: config.Endpoint,
		logger:   logger,
	}, nil
}

func (c *ElasticsearchClient) IndexExists(name string) (bool, error) {
	res, err := c.client.Indices.Exists([]string{name})
	if err != nil {
		if strings.Contains(err.Error(), "Connection refused") || strings.Contains(err.Error(), "timeout") {
			return false, fmt.Errorf("cannot connect to Elasticsearch at %s: %v. Please check your endpoint configuration and network connectivity", c.endpoint, err)
		}
		return false, fmt.Errorf("failed to check index existence: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("error checking index: %s", res.String())
	}

	return true, nil
}

func (c *ElasticsearchClient) CreateIndex(name string, mapping map[string]interface{}) error {
	mappingJSON, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("failed to marshal mapping: %w", err)
	}

	res, err := c.client.Indices.Create(name, c.client.Indices.Create.WithBody(strings.NewReader(string(mappingJSON))))
	if err != nil {
		if strings.Contains(err.Error(), "Connection refused") || strings.Contains(err.Error(), "timeout") {
			return fmt.Errorf("cannot connect to Elasticsearch at %s: %v. Please check your endpoint configuration and network connectivity", c.endpoint, err)
		}
		return fmt.Errorf("index creation failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 400 {
			// Index already exists
			c.logger.Printf("Index '%s' already exists (conflict)", name)
			return nil
		}
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("index creation failed: %s", string(body))
	}

	c.logger.Printf("Index '%s' created", name)
	return nil
}

func (c *ElasticsearchClient) Bulk(payload string, refresh bool) (*esapi.Response, error) {
	res, err := c.client.Bulk(strings.NewReader(payload), c.client.Bulk.WithRefresh(strconv.FormatBool(refresh)))
	if err != nil {
		return nil, fmt.Errorf("bulk request failed: %w", err)
	}
	return res, nil
}

func (c *ElasticsearchClient) ClusterHealth() (map[string]interface{}, error) {
	res, err := c.client.Cluster.Health()
	if err != nil {
		return nil, fmt.Errorf("cluster health request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("cluster health request failed: %s", string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode cluster health response: %w", err)
	}

	return result, nil
}

func (c *ElasticsearchClient) DeleteIndex(name string) (bool, error) {
	res, err := c.client.Indices.Delete([]string{name})
	if err != nil {
		return false, fmt.Errorf("index deletion failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return false, nil
		}
		body, _ := io.ReadAll(res.Body)
		return false, fmt.Errorf("index deletion failed: %s", string(body))
	}

	return true, nil
}

func (c *ElasticsearchClient) ListIndices(pattern string) ([]string, error) {
	res, err := c.client.Cat.Indices(c.client.Cat.Indices.WithIndex(pattern), c.client.Cat.Indices.WithFormat("json"))
	if err != nil {
		return nil, fmt.Errorf("failed to list indices: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("failed to list indices: %s", string(body))
	}

	var indices []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		return nil, fmt.Errorf("failed to decode indices response: %w", err)
	}

	var result []string
	for _, idx := range indices {
		if name, ok := idx["index"].(string); ok && name != "" {
			result = append(result, name)
		}
	}

	return result, nil
}

func (c *ElasticsearchClient) DeleteIndicesByPattern(pattern string) ([]string, error) {
	indices, err := c.ListIndices(pattern)
	if err != nil {
		return nil, err
	}

	if len(indices) == 0 {
		return []string{}, nil
	}

	var deleted []string
	for _, indexName := range indices {
		ok, err := c.DeleteIndex(indexName)
		if err != nil {
			return deleted, err
		}
		if ok {
			deleted = append(deleted, indexName)
		}
	}

	return deleted, nil
}

type AirportLookup struct {
	airports map[string]struct {
		Lat float64
		Lon float64
	}
	logger *log.Logger
}

func NewAirportLookup(airportsFile string, logger *log.Logger) (*AirportLookup, error) {
	lookup := &AirportLookup{
		airports: make(map[string]struct {
			Lat float64
			Lon float64
		}),
		logger: logger,
	}

	if airportsFile != "" {
		if _, err := os.Stat(airportsFile); err == nil {
			if err := lookup.loadAirports(airportsFile); err != nil {
				return nil, err
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}

	return lookup, nil
}

func (a *AirportLookup) LookupCoordinates(iataCode string) string {
	if iataCode == "" {
		return ""
	}

	airport, ok := a.airports[strings.ToUpper(iataCode)]
	if !ok {
		return ""
	}

	return fmt.Sprintf("%.6f,%.6f", airport.Lat, airport.Lon)
}

func (a *AirportLookup) loadAirports(filePath string) error {
	a.logger.Printf("Loading airports from %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.HasSuffix(strings.ToLower(filePath), ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	}

	csvReader := csv.NewReader(reader)
	count := 0

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Columns: ID, Name, City, Country, IATA, ICAO, Lat, Lon, ...
		if len(row) < 8 {
			continue
		}

		iata := strings.TrimSpace(row[4])
		if iata == "" || iata == "\\N" {
			continue
		}

		latStr := strings.TrimSpace(row[6])
		lonStr := strings.TrimSpace(row[7])
		if latStr == "" || lonStr == "" {
			continue
		}

		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			continue
		}

		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			continue
		}

		a.airports[strings.ToUpper(iata)] = struct {
			Lat float64
			Lon float64
		}{Lat: lat, Lon: lon}
		count++
	}

	a.logger.Printf("Loaded %d airports into lookup table", count)
	return nil
}

type CancellationLookup struct {
	cancellations map[string]string
	logger        *log.Logger
}

func NewCancellationLookup(cancellationsFile string, logger *log.Logger) (*CancellationLookup, error) {
	lookup := &CancellationLookup{
		cancellations: make(map[string]string),
		logger:        logger,
	}

	if cancellationsFile != "" {
		if _, err := os.Stat(cancellationsFile); err == nil {
			if err := lookup.loadCancellations(cancellationsFile); err != nil {
				return nil, err
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}

	return lookup, nil
}

func (c *CancellationLookup) LookupReason(code string) string {
	if code == "" {
		return ""
	}

	return c.cancellations[strings.ToUpper(code)]
}

func (c *CancellationLookup) loadCancellations(filePath string) error {
	c.logger.Printf("Loading cancellations from %s", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	csvReader.TrimLeadingSpace = true

	headers, err := csvReader.Read()
	if err != nil {
		return err
	}

	codeIdx := -1
	descIdx := -1
	for i, h := range headers {
		h = strings.TrimSpace(h)
		if h == "Code" {
			codeIdx = i
		} else if h == "Description" {
			descIdx = i
		}
	}

	if codeIdx == -1 || descIdx == -1 {
		return fmt.Errorf("CSV must have 'Code' and 'Description' columns")
	}

	count := 0
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if len(row) <= codeIdx || len(row) <= descIdx {
			continue
		}

		code := strings.TrimSpace(row[codeIdx])
		description := strings.TrimSpace(row[descIdx])
		if code == "" || description == "" {
			continue
		}

		c.cancellations[strings.ToUpper(code)] = description
		count++
	}

	c.logger.Printf("Loaded %d cancellation reasons into lookup table", count)
	return nil
}

type FlightLoader struct {
	client             *ElasticsearchClient
	mapping            map[string]interface{}
	indexPrefix        string
	logger             *log.Logger
	batchSize          int
	refresh            bool
	airportLookup      *AirportLookup
	cancellationLookup *CancellationLookup
	ensuredIndices     map[string]bool
	mu                 sync.Mutex
	loadedRecords      int64
	totalRecords       int64
}

func NewFlightLoader(client *ElasticsearchClient, mapping map[string]interface{}, indexPrefix string, logger *log.Logger, batchSize int, refresh bool, airportsFile, cancellationsFile string) (*FlightLoader, error) {
	airportLookup, err := NewAirportLookup(airportsFile, logger)
	if err != nil {
		return nil, err
	}

	cancellationLookup, err := NewCancellationLookup(cancellationsFile, logger)
	if err != nil {
		return nil, err
	}

	return &FlightLoader{
		client:             client,
		mapping:            mapping,
		indexPrefix:        indexPrefix,
		logger:             logger,
		batchSize:          batchSize,
		refresh:            refresh,
		airportLookup:      airportLookup,
		cancellationLookup: cancellationLookup,
		ensuredIndices:     make(map[string]bool),
	}, nil
}

func (f *FlightLoader) EnsureIndex(indexName string) error {
	if f.client == nil {
		return nil
	}

	f.mu.Lock()
	if f.ensuredIndices[indexName] {
		f.mu.Unlock()
		return nil
	}
	// Mark as in-progress to prevent concurrent ensures
	f.ensuredIndices[indexName] = true
	f.mu.Unlock()

	exists, err := f.client.IndexExists(indexName)
	if err != nil {
		// If error, unmark so we can retry
		f.mu.Lock()
		delete(f.ensuredIndices, indexName)
		f.mu.Unlock()
		return err
	}

	if exists {
		f.logger.Printf("Deleting existing index '%s' before import", indexName)
		deleted, err := f.client.DeleteIndex(indexName)
		if err != nil {
			// If error, unmark so we can retry
			f.mu.Lock()
			delete(f.ensuredIndices, indexName)
			f.mu.Unlock()
			return err
		}
		if deleted {
			f.logger.Printf("Index '%s' deleted", indexName)
		} else {
			f.logger.Printf("Failed to delete index '%s'", indexName)
		}
	}

	f.logger.Printf("Creating index: %s", indexName)
	if err := f.client.CreateIndex(indexName, f.mapping); err != nil {
		// If error, unmark so we can retry
		f.mu.Lock()
		delete(f.ensuredIndices, indexName)
		f.mu.Unlock()
		return err
	}

	f.logger.Printf("Successfully created index: %s", indexName)
	return nil
}

func (f *FlightLoader) ImportFiles(files []string) error {
	f.logger.Printf("Counting records in %d file(s)...", len(files))
	total, err := f.countTotalRecordsFast(files)
	if err != nil {
		return err
	}
	f.totalRecords = total
	f.logger.Printf("Total records to import: %s", formatNumber(total))
	f.logger.Printf("Importing %d file(s)...", len(files))

	for _, filePath := range files {
		if err := f.importFile(filePath); err != nil {
			return err
		}
	}

	fmt.Println()
	f.logger.Printf("Import complete: %s of %s records loaded", formatNumber(f.loadedRecords), formatNumber(f.totalRecords))
	return nil
}

func (f *FlightLoader) SampleDocument(filePath string) (map[string]interface{}, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		f.logger.Printf("Skipping %s (not a regular file)", filePath)
		return nil, nil
	}

	f.logger.Printf("Sampling first document from %s", filePath)

	reader, cleanup, err := f.openDataReader(filePath)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	csvReader := csv.NewReader(reader)
	headers, err := csvReader.Read()
	if err != nil {
		return nil, err
	}

	row, err := csvReader.Read()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rowMap := make(map[string]string)
	for i, h := range headers {
		if i < len(row) {
			rowMap[h] = row[i]
		}
	}

	doc := f.transformRow(rowMap)
	return doc, nil
}

func (f *FlightLoader) countTotalRecordsFast(files []string) (int64, error) {
	var total int64
	for _, filePath := range files {
		info, err := os.Stat(filePath)
		if err != nil || info.IsDir() {
			continue
		}

		count, err := f.countLinesFast(filePath)
		if err != nil {
			f.logger.Printf("Failed to count lines in %s: %v", filePath, err)
			continue
		}
		// Subtract 1 for CSV header
		if count > 0 {
			total += count - 1
		}
	}
	return total, nil
}

func (f *FlightLoader) countLinesFast(filePath string) (int64, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	baseName := strings.ToLower(filepath.Base(filePath))

	if ext == ".zip" {
		entry, err := f.csvEntryInZip(filePath)
		if err != nil || entry == "" {
			return 0, err
		}
		cmd1 := exec.Command("unzip", "-p", filePath, entry)
		cmd2 := exec.Command("wc", "-l")

		pipe, err := cmd1.StdoutPipe()
		if err != nil {
			return 0, err
		}
		cmd2.Stdin = pipe

		if err := cmd1.Start(); err != nil {
			return 0, err
		}

		output, err := cmd2.CombinedOutput()
		if err != nil {
			cmd1.Wait()
			return 0, err
		}

		if err := cmd1.Wait(); err != nil {
			return 0, err
		}

		lines := strings.TrimSpace(string(output))
		count, err := strconv.ParseInt(lines, 10, 64)
		return count, err
	} else if strings.HasSuffix(baseName, ".gz") {
		cmd1 := exec.Command("gunzip", "-c", filePath)
		cmd2 := exec.Command("wc", "-l")

		pipe, err := cmd1.StdoutPipe()
		if err != nil {
			return 0, err
		}
		cmd2.Stdin = pipe

		if err := cmd1.Start(); err != nil {
			return 0, err
		}

		output, err := cmd2.CombinedOutput()
		if err != nil {
			cmd1.Wait()
			return 0, err
		}

		if err := cmd1.Wait(); err != nil {
			return 0, err
		}

		lines := strings.TrimSpace(string(output))
		count, err := strconv.ParseInt(lines, 10, 64)
		return count, err
	} else {
		cmd := exec.Command("wc", "-l", filePath)
		output, err := cmd.Output()
		if err != nil {
			return 0, err
		}
		parts := strings.Fields(string(output))
		if len(parts) > 0 {
			count, err := strconv.ParseInt(parts[0], 10, 64)
			return count, err
		}
		return 0, nil
	}
}

func (f *FlightLoader) csvEntryInZip(zipPath string) (string, error) {
	cmd := exec.Command("unzip", "-Z1", zipPath)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if strings.HasSuffix(strings.ToLower(line), ".csv") {
			return line, nil
		}
	}

	return "", nil
}

func (f *FlightLoader) importFile(filePath string) error {
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if info.IsDir() {
		f.logger.Printf("Skipping %s (not a regular file)", filePath)
		return nil
	}

	f.logger.Printf("Importing %s", filePath)

	fileYear, fileMonth := f.extractYearMonthFromFilename(filePath)

	indexBuffers := make(map[string]*struct {
		lines []string
		count int
	})
	ensuredInThisFile := make(map[string]bool) // Track which indices we've ensured in this file
	indexedDocs := 0
	processedRows := 0

	reader, cleanup, err := f.openDataReader(filePath)
	if err != nil {
		return err
	}
	defer cleanup()

	csvReader := csv.NewReader(reader)
	headers, err := csvReader.Read()
	if err != nil {
		return err
	}

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		processedRows++

		rowMap := make(map[string]string)
		for i, h := range headers {
			if i < len(row) {
				rowMap[h] = row[i]
			}
		}

		if processedRows == 1 {
			hasTimestamp := false
			hasFlightDate := false
			for h := range rowMap {
				if h == "@timestamp" {
					hasTimestamp = true
				}
				if h == "FlightDate" {
					hasFlightDate = true
				}
			}
			if !hasTimestamp && !hasFlightDate {
				headerList := make([]string, 0, 10)
				for h := range rowMap {
					headerList = append(headerList, h)
					if len(headerList) >= 10 {
						break
					}
				}
				f.logger.Printf("CSV headers don't include '@timestamp' or 'FlightDate'. Available headers: %s", strings.Join(headerList, ", "))
			}
		}

		doc := f.transformRow(rowMap)
		if doc == nil || len(doc) == 0 {
			continue
		}

		timestamp, _ := doc["@timestamp"].(string)
		indexName := f.extractIndexName(timestamp, fileYear, fileMonth)
		if indexName == "" {
			timestampRaw := rowMap["@timestamp"]
			if timestampRaw == "" {
				timestampRaw = rowMap["FlightDate"]
			}
			f.logger.Printf("Skipping document - missing or invalid timestamp. Raw value: %q, parsed timestamp: %q. Row %d: Origin=%s, Dest=%s, Airline=%s",
				timestampRaw, timestamp, processedRows, rowMap["Origin"], rowMap["Dest"], rowMap["Reporting_Airline"])
			continue
		}

		// Remove nil values
		doc = compactMap(doc)

		// Only ensure index once per unique index name in this file
		if !ensuredInThisFile[indexName] {
			if err := f.EnsureIndex(indexName); err != nil {
				return err
			}
			ensuredInThisFile[indexName] = true
		}

		if indexBuffers[indexName] == nil {
			indexBuffers[indexName] = &struct {
				lines []string
				count int
			}{lines: make([]string, 0), count: 0}
		}

		buffer := indexBuffers[indexName]
		indexAction := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
			},
		}
		indexJSON, _ := json.Marshal(indexAction)
		docJSON, _ := json.Marshal(doc)

		buffer.lines = append(buffer.lines, string(indexJSON), string(docJSON))
		buffer.count++

		if buffer.count >= f.batchSize {
			flushed, err := f.flushIndex(indexName, buffer.lines, buffer.count)
			if err != nil {
				return err
			}
			indexedDocs += flushed
			buffer.lines = buffer.lines[:0]
			buffer.count = 0
		}
	}

	for indexName, buffer := range indexBuffers {
		if buffer.count > 0 {
			flushed, err := f.flushIndex(indexName, buffer.lines, buffer.count)
			if err != nil {
				return err
			}
			indexedDocs += flushed
		}
	}

	f.logger.Printf("Finished %s (rows processed: %d, documents indexed: %d)", filePath, processedRows, indexedDocs)
	return nil
}

func (f *FlightLoader) flushIndex(indexName string, lines []string, docCount int) (int, error) {
	payload := strings.Join(lines, "\n") + "\n"

	res, err := f.client.Bulk(payload, f.refresh)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return 0, fmt.Errorf("bulk request failed: %s", string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode bulk response: %w", err)
	}

	if errors, ok := result["errors"].(bool); ok && errors {
		items, _ := result["items"].([]interface{})
		errorCount := 0
		for _, item := range items {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if index, ok := itemMap["index"].(map[string]interface{}); ok {
					if err, ok := index["error"].(map[string]interface{}); ok {
						if errorCount < 5 {
							f.logger.Printf("Bulk item error for %s: %v", indexName, err)
						}
						errorCount++
					}
				}
			}
		}
		if errorCount > 0 {
			return 0, fmt.Errorf("bulk indexing reported errors for %s; aborting", indexName)
		}
	}

	f.mu.Lock()
	f.loadedRecords += int64(docCount)
	loaded := f.loadedRecords
	total := f.totalRecords
	f.mu.Unlock()

	if total > 0 {
		percentage := float64(loaded) / float64(total) * 100
		fmt.Printf("\r%s of %s records loaded (%.1f%%)", formatNumber(loaded), formatNumber(total), percentage)
	} else {
		fmt.Printf("\r%s records loaded", formatNumber(loaded))
	}
	os.Stdout.Sync()

	return docCount, nil
}

func (f *FlightLoader) openDataReader(filePath string) (io.Reader, func(), error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	baseName := strings.ToLower(filepath.Base(filePath))

	if ext == ".zip" {
		entry, err := f.csvEntryInZip(filePath)
		if err != nil || entry == "" {
			return nil, nil, fmt.Errorf("no CSV entry found in %s", filePath)
		}

		cmd := exec.Command("unzip", "-p", filePath, entry)
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, nil, err
		}

		if err := cmd.Start(); err != nil {
			return nil, nil, err
		}

		var cmdErr error
		cleanup := func() {
			cmdErr = cmd.Wait()
			if cmdErr != nil {
				f.logger.Printf("Warning: unzip command failed: %v", cmdErr)
			}
		}

		return pipe, cleanup, nil
	} else if strings.HasSuffix(baseName, ".gz") {
		file, err := os.Open(filePath)
		if err != nil {
			return nil, nil, err
		}

		gz, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, err
		}

		cleanup := func() {
			gz.Close()
			file.Close()
		}

		return gz, cleanup, nil
	} else {
		file, err := os.Open(filePath)
		if err != nil {
			return nil, nil, err
		}

		cleanup := func() {
			file.Close()
		}

		return file, cleanup, nil
	}
}

func (f *FlightLoader) extractIndexName(timestamp string, fileYear, fileMonth string) string {
	if fileYear != "" && fileMonth != "" {
		return fmt.Sprintf("%s-%s-%s", f.indexPrefix, fileYear, fileMonth)
	}

	if fileYear != "" {
		return fmt.Sprintf("%s-%s", f.indexPrefix, fileYear)
	}

	if timestamp == "" {
		return ""
	}

	re := regexp.MustCompile(`^(\d{4})-(\d{2})-\d{2}`)
	matches := re.FindStringSubmatch(timestamp)
	if len(matches) >= 3 {
		year := matches[1]
		return fmt.Sprintf("%s-%s", f.indexPrefix, year)
	}

	f.logger.Printf("Unable to parse timestamp format: %s", timestamp)
	return ""
}

func (f *FlightLoader) extractYearMonthFromFilename(filePath string) (string, string) {
	basename := filepath.Base(filePath)

	// Remove extensions (.gz, .csv, .zip) - handle multiple extensions
	for {
		oldBasename := basename
		basename = regexp.MustCompile(`\.(gz|csv|zip)$`).ReplaceAllString(strings.ToLower(basename), "")
		if basename == oldBasename {
			break
		}
	}

	// Try pattern: flights-YYYY-MM (e.g., flights-2024-07)
	re := regexp.MustCompile(`-(\d{4})-(\d{2})$`)
	matches := re.FindStringSubmatch(basename)
	if len(matches) >= 3 {
		return matches[1], matches[2]
	}

	// Try pattern: flights-YYYY (e.g., flights-2019)
	re = regexp.MustCompile(`-(\d{4})$`)
	matches = re.FindStringSubmatch(basename)
	if len(matches) >= 2 {
		return matches[1], ""
	}

	return "", ""
}

func (f *FlightLoader) transformRow(row map[string]string) map[string]interface{} {
	doc := make(map[string]interface{})

	timestamp := present(row["@timestamp"])
	if timestamp == "" {
		timestamp = present(row["FlightDate"])
	}

	flightDate := timestamp
	reportingAirline := present(row["Reporting_Airline"])
	flightNumber := present(row["Flight_Number_Reporting_Airline"])
	origin := present(row["Origin"])
	dest := present(row["Dest"])

	if flightDate != "" && reportingAirline != "" && flightNumber != "" && origin != "" && dest != "" {
		doc["FlightID"] = fmt.Sprintf("%s_%s_%s_%s_%s", flightDate, reportingAirline, flightNumber, origin, dest)
	}

	doc["@timestamp"] = timestamp

	doc["Reporting_Airline"] = reportingAirline
	doc["Tail_Number"] = present(row["Tail_Number"])
	doc["Flight_Number"] = flightNumber
	doc["Origin"] = origin
	doc["Dest"] = dest

	doc["CRSDepTimeLocal"] = toInteger(row["CRSDepTime"])
	doc["DepDelayMin"] = toInteger(row["DepDelay"])
	doc["TaxiOutMin"] = toInteger(row["TaxiOut"])
	doc["TaxiInMin"] = toInteger(row["TaxiIn"])
	doc["CRSArrTimeLocal"] = toInteger(row["CRSArrTime"])
	doc["ArrDelayMin"] = toInteger(row["ArrDelay"])

	doc["Cancelled"] = toBoolean(row["Cancelled"])
	doc["Diverted"] = toBoolean(row["Diverted"])

	cancellationCode := present(row["CancellationCode"])
	doc["CancellationCode"] = cancellationCode

	cancellationReason := f.cancellationLookup.LookupReason(cancellationCode)
	if cancellationReason != "" {
		doc["CancellationReason"] = cancellationReason
	}

	doc["ActualElapsedTimeMin"] = toInteger(row["ActualElapsedTime"])
	doc["AirTimeMin"] = toInteger(row["AirTime"])

	doc["Flights"] = toInteger(row["Flights"])
	doc["DistanceMiles"] = toInteger(row["Distance"])

	doc["CarrierDelayMin"] = toInteger(row["CarrierDelay"])
	doc["WeatherDelayMin"] = toInteger(row["WeatherDelay"])
	doc["NASDelayMin"] = toInteger(row["NASDelay"])
	doc["SecurityDelayMin"] = toInteger(row["SecurityDelay"])
	doc["LateAircraftDelayMin"] = toInteger(row["LateAircraftDelay"])

	originLocation := f.airportLookup.LookupCoordinates(origin)
	if originLocation != "" {
		doc["OriginLocation"] = originLocation
	}

	destLocation := f.airportLookup.LookupCoordinates(dest)
	if destLocation != "" {
		doc["DestLocation"] = destLocation
	}

	return doc
}

func present(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	return trimmed
}

func toInteger(value string) interface{} {
	value = present(value)
	if value == "" {
		return nil
	}

	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil
	}

	return int(f + 0.5)
}

func toBoolean(value string) interface{} {
	value = present(value)
	if value == "" {
		return nil
	}

	lower := strings.ToLower(value)
	switch lower {
	case "true", "t", "yes", "y":
		return true
	case "false", "f", "no", "n":
		return false
	}

	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f > 0
	}

	return nil
}

func compactMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		if v != nil {
			result[k] = v
		}
	}
	return result
}

func formatNumber(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(r)
	}
	return result.String()
}

func resolvePath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}

	if abs, err := filepath.Abs(path); err == nil {
		if _, statErr := os.Stat(abs); statErr == nil {
			return abs
		}
	}

	if cwd, err := os.Getwd(); err == nil {
		if candidate := resolveRelativePath(cwd, path); candidate != "" {
			return candidate
		}
		parent := filepath.Dir(cwd)
		if parent != "" && parent != cwd {
			if candidate := resolveRelativePath(parent, path); candidate != "" {
				return candidate
			}
			// If the file doesn't exist, still return the parent-relative path as best-effort
			return filepath.Join(parent, path)
		}
	}

	// Try relative to workspace root (one level up from executable)
	execPath, err := os.Executable()
	if err != nil {
		// Fallback: try relative to current working directory
		return path
	}
	execDir := filepath.Dir(execPath)
	if candidate := resolveRelativePath(execDir, path); candidate != "" {
		return candidate
	}

	workspaceRoot := filepath.Join(execDir, "..")
	if candidate := resolveRelativePath(workspaceRoot, path); candidate != "" {
		return candidate
	}

	// Return resolved path even if file doesn't exist (for optional files)
	// The caller will check existence if needed
	return filepath.Join(workspaceRoot, path)
}

func resolveRelativePath(basePath, relPath string) string {
	candidate := filepath.Join(basePath, relPath)
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	return ""
}

func loadConfig(path string) (Config, error) {
	resolvedPath := resolvePath(path)
	data, err := os.ReadFile(resolvedPath)
	if err != nil {
		return Config{}, fmt.Errorf("config file not found: %s (tried: %s)", path, resolvedPath)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("failed to parse config: %w", err)
	}

	return config, nil
}

func loadMapping(path string) (map[string]interface{}, error) {
	resolvedPath := resolvePath(path)
	data, err := os.ReadFile(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("mapping file not found: %s (tried: %s)", path, resolvedPath)
	}

	var mapping map[string]interface{}
	if err := json.Unmarshal(data, &mapping); err != nil {
		return nil, fmt.Errorf("failed to parse mapping: %w", err)
	}

	return mapping, nil
}

func filesToProcess(options *Options) ([]string, error) {
	resolvedDataDir := resolvePath(options.DataDir)

	if options.File != "" {
		return []string{resolveFilePath(options.File, resolvedDataDir)}, nil
	}

	if len(options.GlobFiles) > 0 {
		var files []string
		for _, f := range options.GlobFiles {
			files = append(files, resolveFilePath(f, resolvedDataDir))
		}
		return files, nil
	}

	if options.Glob != "" {
		var files []string
		if filepath.IsAbs(options.Glob) {
			matches, err := filepath.Glob(options.Glob)
			if err != nil {
				return nil, err
			}
			for _, m := range matches {
				if info, err := os.Stat(m); err == nil && !info.IsDir() {
					files = append(files, m)
				}
			}
		} else {
			matches, err := filepath.Glob(options.Glob)
			if err != nil {
				return nil, err
			}
			for _, m := range matches {
				if info, err := os.Stat(m); err == nil && !info.IsDir() {
					files = append(files, m)
				}
			}
			if len(files) == 0 {
				expandedPattern := filepath.Join(resolvedDataDir, options.Glob)
				matches, err := filepath.Glob(expandedPattern)
				if err != nil {
					return nil, err
				}
				for _, m := range matches {
					if info, err := os.Stat(m); err == nil && !info.IsDir() {
						files = append(files, m)
					}
				}
			}
		}

		if len(files) == 0 {
			return nil, fmt.Errorf("no files found matching pattern: %s", options.Glob)
		}
		return files, nil
	}

	// Default: all files in data directory (when --all is set or no specific option)
	if options.All || (options.File == "" && options.Glob == "" && len(options.GlobFiles) == 0) {
		patternZip := filepath.Join(resolvedDataDir, "*.zip")
		patternCSV := filepath.Join(resolvedDataDir, "*.csv")
		patternCSVGz := filepath.Join(resolvedDataDir, "*.csv.gz")

		var allFiles []string
		allFiles = append(allFiles, globFiles(patternZip)...)
		allFiles = append(allFiles, globFiles(patternCSV)...)
		allFiles = append(allFiles, globFiles(patternCSVGz)...)

		if len(allFiles) == 0 {
			return nil, fmt.Errorf("no .zip, .csv, or .csv.gz files found in %s", resolvedDataDir)
		}

		return allFiles, nil
	}

	return nil, fmt.Errorf("please provide either --file PATH, --all, or --glob PATTERN")
}

func globFiles(pattern string) []string {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil
	}
	var files []string
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && !info.IsDir() {
			files = append(files, m)
		}
	}
	return files
}

func resolveFilePath(path, dataDir string) string {
	expanded := filepath.Clean(path)
	if filepath.IsAbs(expanded) {
		if _, err := os.Stat(expanded); err == nil {
			return expanded
		}
	}

	resolvedDataDir := resolvePath(dataDir)
	candidate := filepath.Join(resolvedDataDir, path)
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}

	if _, err := os.Stat(path); err == nil {
		return path
	}

	return path
}

type Options struct {
	Config            string
	Mapping           string
	DataDir           string
	File              string
	All               bool
	Glob              string
	GlobFiles         []string
	Index             string
	BatchSize         int
	Refresh           bool
	Status            bool
	DeleteIndex       bool
	DeleteAll         bool
	Sample            bool
	AirportsFile      string
	CancellationsFile string
}

func parseOptions() *Options {
	options := &Options{
		Config:            "config/elasticsearch.yml",
		Mapping:           "config/mappings-flights.json",
		DataDir:           "data",
		Index:             "flights",
		BatchSize:         defaultBatchSize,
		Refresh:           false,
		Status:            false,
		DeleteIndex:       false,
		DeleteAll:         false,
		Sample:            false,
		AirportsFile:      "data/airports.csv.gz",
		CancellationsFile: "data/cancellations.csv",
	}

	flag.StringVar(&options.Config, "c", options.Config, "Path to Elasticsearch config YAML")
	flag.StringVar(&options.Config, "config", options.Config, "Path to Elasticsearch config YAML")
	flag.StringVar(&options.Mapping, "m", options.Mapping, "Path to mappings JSON")
	flag.StringVar(&options.Mapping, "mapping", options.Mapping, "Path to mappings JSON")
	flag.StringVar(&options.DataDir, "d", options.DataDir, "Directory containing data files")
	flag.StringVar(&options.DataDir, "data-dir", options.DataDir, "Directory containing data files")
	flag.StringVar(&options.File, "f", options.File, "Only import the specified file")
	flag.StringVar(&options.File, "file", options.File, "Only import the specified file")
	flag.BoolVar(&options.All, "a", false, "Import all files found in the data directory")
	flag.BoolVar(&options.All, "all", false, "Import all files found in the data directory")
	flag.StringVar(&options.Glob, "g", options.Glob, "Import files matching the glob pattern")
	flag.StringVar(&options.Glob, "glob", options.Glob, "Import files matching the glob pattern")
	flag.StringVar(&options.Index, "index", options.Index, "Override index name")
	flag.IntVar(&options.BatchSize, "batch-size", options.BatchSize, "Number of documents per bulk request")
	flag.BoolVar(&options.Refresh, "refresh", options.Refresh, "Request an index refresh after each bulk request")
	flag.BoolVar(&options.Status, "status", options.Status, "Test connection and print cluster health status")
	flag.BoolVar(&options.DeleteIndex, "delete-index", options.DeleteIndex, "Delete indices matching the index pattern and exit")
	flag.BoolVar(&options.DeleteAll, "delete-all", options.DeleteAll, "Delete all flights-* indices and exit")
	flag.BoolVar(&options.Sample, "sample", options.Sample, "Print the first document and exit")
	flag.StringVar(&options.AirportsFile, "airports-file", options.AirportsFile, "Path to airports CSV file")
	flag.StringVar(&options.CancellationsFile, "cancellations-file", options.CancellationsFile, "Path to cancellations CSV file")

	flag.Parse()

	// Handle glob expansion from shell
	if options.Glob != "" && len(flag.Args()) > 0 {
		if !strings.Contains(options.Glob, "*") && !strings.Contains(options.Glob, "?") {
			options.GlobFiles = append([]string{options.Glob}, flag.Args()...)
			options.Glob = ""
		}
	}

	// Validate options
	if options.Status && (options.DeleteIndex || options.DeleteAll) {
		fmt.Fprintf(os.Stderr, "Cannot use --status with --delete-index or --delete-all\n")
		os.Exit(1)
	}

	if options.DeleteIndex && options.DeleteAll {
		fmt.Fprintf(os.Stderr, "Cannot use --delete-index and --delete-all together\n")
		os.Exit(1)
	}

	if !options.Status && !options.DeleteIndex && !options.DeleteAll && !options.Sample {
		selectionCount := 0
		if options.File != "" {
			selectionCount++
		}
		if options.All {
			selectionCount++
		}
		if options.Glob != "" {
			selectionCount++
		}
		if len(options.GlobFiles) > 0 {
			selectionCount++
		}

		if selectionCount > 1 {
			fmt.Fprintf(os.Stderr, "Cannot use --file, --all, and --glob together (use only one)\n")
			os.Exit(1)
		}
	}

	return options
}

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

func sampleDocument(options *Options, logger *log.Logger) error {
	mapping, err := loadMapping(options.Mapping)
	if err != nil {
		return err
	}

	resolvedAirportsFile := ""
	if options.AirportsFile != "" {
		resolvedAirportsFile = resolvePath(options.AirportsFile)
	}

	resolvedCancellationsFile := ""
	if options.CancellationsFile != "" {
		resolvedCancellationsFile = resolvePath(options.CancellationsFile)
	}

	loader, err := NewFlightLoader(nil, mapping, "flights", logger, 1, false, resolvedAirportsFile, resolvedCancellationsFile)
	if err != nil {
		return err
	}

	files, err := filesToProcess(options)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return fmt.Errorf("no files found to sample")
	}

	doc, err := loader.SampleDocument(files[0])
	if err != nil {
		return err
	}

	if doc == nil {
		return fmt.Errorf("no document found in file")
	}

	jsonData, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(jsonData))
	return nil
}

func reportStatus(client *ElasticsearchClient, logger *log.Logger) error {
	status, err := client.ClusterHealth()
	if err != nil {
		return fmt.Errorf("failed to retrieve cluster status: %w", err)
	}

	statusStr, _ := status["status"].(string)
	logger.Printf("Cluster status: %s", statusStr)

	activeShards, _ := status["active_shards"].(float64)
	nodeCount, _ := status["number_of_nodes"].(float64)
	logger.Printf("Active shards: %.0f, node count: %.0f", activeShards, nodeCount)

	return nil
}

func deleteIndicesByPattern(client *ElasticsearchClient, logger *log.Logger, pattern string) error {
	patternWithWildcard := pattern
	if !strings.HasSuffix(pattern, "*") {
		patternWithWildcard = pattern + "-*"
	}

	logger.Printf("Searching for indices matching pattern: %s", patternWithWildcard)

	deleted, err := client.DeleteIndicesByPattern(patternWithWildcard)
	if err != nil {
		return fmt.Errorf("failed to delete indices matching pattern '%s': %w", pattern, err)
	}

	if len(deleted) == 0 {
		logger.Printf("No indices found matching pattern: %s", patternWithWildcard)
	} else {
		logger.Printf("Deleted %d index(es): %s", len(deleted), strings.Join(deleted, ", "))
	}

	return nil
}
