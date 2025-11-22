package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

const (
	esIndex                = "contracts"
	pipelineName           = "pdf_pipeline"
	defaultInferenceEndpoint = ".elser-2-elastic"
)

// Extend ElasticsearchClient with contract-specific methods
func (c *ElasticsearchClient) CreatePipeline(name string, pipelineConfig map[string]interface{}) error {
	configJSON, err := json.Marshal(pipelineConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal pipeline config: %w", err)
	}

	req := esapi.IngestPutPipelineRequest{
		PipelineID: name,
		Body:       strings.NewReader(string(configJSON)),
	}

	res, err := req.Do(context.Background(), c.client)
	if err != nil {
		return fmt.Errorf("pipeline creation failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("pipeline creation failed: %s", string(body))
	}

	c.logger.Printf("Pipeline '%s' created/updated", name)
	return nil
}

func (c *ElasticsearchClient) IndexDocument(indexName string, document map[string]interface{}, pipeline string) error {
	docJSON, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      indexName,
		Body:       strings.NewReader(string(docJSON)),
		Pipeline:   pipeline,
		Refresh:    "wait_for",
	}

	res, err := req.Do(context.Background(), c.client)
	if err != nil {
		return fmt.Errorf("document indexing failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("document indexing failed: %s", string(body))
	}

	// Check response body for errors even if status is OK
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err == nil {
		if errVal, ok := result["error"]; ok {
			c.logger.Printf("Elasticsearch returned error in response: %v", errVal)
			return fmt.Errorf("document indexing failed: %v", errVal)
		}
	}

	return nil
}

func (c *ElasticsearchClient) GetInferenceEndpoints() (map[string]interface{}, error) {
	// Use low-level HTTP request since inference endpoints API may not be in typed client
	url := strings.TrimSuffix(c.endpoint, "/") + "/_inference/_all"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		c.logger.Printf("Failed to create request for inference endpoints: %v", err)
		return map[string]interface{}{"endpoints": []interface{}{}}, nil
	}

	// Add authentication headers from config
	if c.config != nil {
		if c.config.APIKey != "" {
			req.Header.Set("Authorization", "ApiKey "+c.config.APIKey)
		} else if c.config.User != "" && c.config.Password != "" {
			auth := c.config.User + ":" + c.config.Password
			encoded := base64.StdEncoding.EncodeToString([]byte(auth))
			req.Header.Set("Authorization", "Basic "+encoded)
		}

		// Add custom headers
		for k, v := range c.config.Headers {
			req.Header.Set(k, v)
		}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.logger.Printf("Failed to get inference endpoints: %v", err)
		return map[string]interface{}{"endpoints": []interface{}{}}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Printf("Failed to get inference endpoints: HTTP %d", resp.StatusCode)
		return map[string]interface{}{"endpoints": []interface{}{}}, nil
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.logger.Printf("Failed to decode inference endpoints response: %v", err)
		return map[string]interface{}{"endpoints": []interface{}{}}, nil
	}

	return result, nil
}

func (c *ElasticsearchClient) CountDocuments(indexName string) (int64, error) {
	res, err := c.client.Count(c.client.Count.WithIndex(indexName))
	if err != nil {
		c.logger.Printf("Failed to count documents: %v", err)
		return 0, nil
	}
	defer res.Body.Close()

	if res.IsError() {
		c.logger.Printf("Failed to count documents: HTTP %d", res.StatusCode)
		return 0, nil
	}

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, err
	}

	if count, ok := result["count"].(float64); ok {
		return int64(count), nil
	}

	return 0, nil
}

type ContractLoader struct {
	client           *ElasticsearchClient
	mapping          map[string]interface{}
	inferenceEndpoint string
	logger           *log.Logger
	indexedCount     int
}

func NewContractLoader(client *ElasticsearchClient, mapping map[string]interface{}, logger *log.Logger, inferenceEndpoint string) *ContractLoader {
	if inferenceEndpoint == "" {
		inferenceEndpoint = defaultInferenceEndpoint
	}
	return &ContractLoader{
		client:            client,
		mapping:           mapping,
		inferenceEndpoint: inferenceEndpoint,
		logger:            logger,
		indexedCount:      0,
	}
}

func (l *ContractLoader) CheckElasticsearch() bool {
	health, err := l.client.ClusterHealth()
	if err != nil {
		l.logger.Printf("Connection error: %v", err)
		return false
	}

	clusterName, _ := health["cluster_name"].(string)
	if clusterName == "" {
		clusterName = "unknown"
	}
	status, _ := health["status"].(string)
	if status == "" {
		status = "unknown"
	}

	l.logger.Printf("Cluster: %s", clusterName)
	l.logger.Printf("Status: %s", status)
	return true
}

func (l *ContractLoader) CheckInferenceEndpoint() bool {
	response, err := l.client.GetInferenceEndpoints()
	if err != nil {
		l.logger.Printf("Error checking inference endpoint: %v", err)
		l.logger.Printf("Continuing anyway...")
		return true
	}

	endpoints, _ := response["endpoints"].([]interface{})
	if endpoints == nil {
		endpoints = []interface{}{}
	}

	// First, try to find the specified endpoint
	for _, ep := range endpoints {
		if epMap, ok := ep.(map[string]interface{}); ok {
			if id, ok := epMap["inference_id"].(string); ok && id == l.inferenceEndpoint {
				l.logger.Printf("Found inference endpoint: %s", l.inferenceEndpoint)
				return true
			}
		}
	}

	// Auto-detect ELSER endpoints
	var elserEndpoints []map[string]interface{}
	for _, ep := range endpoints {
		if epMap, ok := ep.(map[string]interface{}); ok {
			if id, ok := epMap["inference_id"].(string); ok {
				if strings.Contains(strings.ToLower(id), "elser") {
					elserEndpoints = append(elserEndpoints, epMap)
				}
			}
		}
	}

	if len(elserEndpoints) > 0 {
		// Prefer endpoints starting with .elser-2- or .elser_model_2
		var preferred []map[string]interface{}
		for _, ep := range elserEndpoints {
			if id, ok := ep["inference_id"].(string); ok {
				if strings.Contains(id, ".elser-2-") || strings.Contains(id, ".elser_model_2") {
					preferred = append(preferred, ep)
				}
			}
		}

		if len(preferred) > 0 {
			if id, ok := preferred[0]["inference_id"].(string); ok {
				l.inferenceEndpoint = id
			}
		} else if len(elserEndpoints) > 0 {
			if id, ok := elserEndpoints[0]["inference_id"].(string); ok {
				l.inferenceEndpoint = id
			}
		}

		l.logger.Printf("Specified endpoint not found, using auto-detected: %s", l.inferenceEndpoint)
		return true
	}

	l.logger.Printf("Inference endpoint '%s' not found", l.inferenceEndpoint)
	l.logger.Printf("Available endpoints:")
	for _, ep := range endpoints {
		if epMap, ok := ep.(map[string]interface{}); ok {
			if id, ok := epMap["inference_id"].(string); ok {
				l.logger.Printf("  - %s", id)
			}
		}
	}
	return false
}

func (l *ContractLoader) CreatePipeline() bool {
	pipelineConfig := map[string]interface{}{
		"description": "Extract text from PDF - semantic_text field handles chunking and embeddings",
		"processors": []map[string]interface{}{
			{
				"attachment": map[string]interface{}{
					"field":         "data",
					"target_field":  "attachment",
					"remove_binary": true,
				},
			},
			{
				"set": map[string]interface{}{
					"field":             "semantic_content",
					"copy_from":         "attachment.content",
					"ignore_empty_value": true,
				},
			},
			{
				"remove": map[string]interface{}{
					"field":          "data",
					"ignore_missing": true,
				},
			},
			{
				"set": map[string]interface{}{
					"field": "upload_date",
					"value": "{{ _ingest.timestamp }}",
				},
			},
		},
	}

	if err := l.client.CreatePipeline(pipelineName, pipelineConfig); err != nil {
		l.logger.Printf("Error creating pipeline: %v", err)
		return false
	}
	return true
}

func (l *ContractLoader) CreateIndex() bool {
	// Delete index if it exists before creating a new one
	exists, err := l.client.IndexExists(esIndex)
	if err != nil {
		l.logger.Printf("Error checking index existence: %v", err)
		return false
	}

	if exists {
		l.logger.Printf("Deleting existing index '%s' before import", esIndex)
		deleted, err := l.client.DeleteIndex(esIndex)
		if err != nil {
			l.logger.Printf("Failed to delete index '%s': %v", esIndex, err)
		} else if deleted {
			l.logger.Printf("Index '%s' deleted", esIndex)
		} else {
			l.logger.Printf("Failed to delete index '%s'", esIndex)
		}
	}

	// Update mapping with detected inference endpoint
	mappingWithInference := deepCopyMap(l.mapping)
	if mappings, ok := mappingWithInference["mappings"].(map[string]interface{}); ok {
		if properties, ok := mappings["properties"].(map[string]interface{}); ok {
			if semanticContent, ok := properties["semantic_content"].(map[string]interface{}); ok {
				semanticContent["inference_id"] = l.inferenceEndpoint
			}
		}
	}

	l.logger.Printf("Creating index: %s", esIndex)
	if err := l.client.CreateIndex(esIndex, mappingWithInference); err != nil {
		l.logger.Printf("Error creating index: %v", err)
		return false
	}
	l.logger.Printf("Successfully created index: %s", esIndex)
	return true
}

func deepCopyMap(m map[string]interface{}) map[string]interface{} {
	jsonData, err := json.Marshal(m)
	if err != nil {
		return m
	}
	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return m
	}
	return result
}

func (l *ContractLoader) ExtractAirlineName(filename string) string {
	filenameLower := strings.ToLower(filename)

	if strings.Contains(filenameLower, "american") {
		return "American Airlines"
	} else if strings.Contains(filenameLower, "southwest") {
		return "Southwest"
	} else if strings.Contains(filenameLower, "united") {
		return "United"
	} else if strings.Contains(filenameLower, "delta") || strings.Contains(filenameLower, "dl-") {
		return "Delta"
	}
	return "Unknown"
}

func (l *ContractLoader) GetPdfFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		l.logger.Printf("Path '%s' does not exist", path)
		return []string{}, nil
	}

	if info.IsDir() {
		var pdfFiles []string
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				name := entry.Name()
				if strings.HasSuffix(strings.ToLower(name), ".pdf") {
					pdfFiles = append(pdfFiles, filepath.Join(path, name))
				}
			}
		}

		if len(pdfFiles) == 0 {
			l.logger.Printf("No PDF files found in directory '%s'", path)
		}
		return pdfFiles, nil
	}

	// Single file
	if strings.HasSuffix(strings.ToLower(path), ".pdf") {
		return []string{path}, nil
	}

	l.logger.Printf("'%s' is not a PDF file", path)
	return []string{}, nil
}

func (l *ContractLoader) IndexPdf(pdfPath string) bool {
	filename := filepath.Base(pdfPath)
	airline := l.ExtractAirlineName(filename)

	pdfData, err := os.ReadFile(pdfPath)
	if err != nil {
		l.logger.Printf("Error reading %s: %v", filename, err)
		return false
	}

	encodedPdf := base64.StdEncoding.EncodeToString(pdfData)

	document := map[string]interface{}{
		"data":     encodedPdf,
		"filename": filename,
		"airline":  airline,
	}

	if err := l.client.IndexDocument(esIndex, document, pipelineName); err != nil {
		l.logger.Printf("\nError processing %s: %v", filename, err)
		return false
	}

	// Don't log here - progress is handled in IngestPdfs()
	l.indexedCount++
	return true
}

func (l *ContractLoader) IngestPdfs(pdfPath string) bool {
	pdfFiles, err := l.GetPdfFiles(pdfPath)
	if err != nil {
		l.logger.Printf("Error getting PDF files: %v", err)
		return false
	}

	if len(pdfFiles) == 0 {
		l.logger.Printf("No PDF files to process")
		return false
	}

	totalFiles := len(pdfFiles)
	l.logger.Printf("Processing %d PDF file(s)...", totalFiles)

	successCount := 0
	failedCount := 0
	processedCount := 0

	for _, pdfFile := range pdfFiles {
		if l.IndexPdf(pdfFile) {
			successCount++
		} else {
			failedCount++
		}
		
		processedCount++
		
		// Update progress
		percentage := float64(processedCount) / float64(totalFiles) * 100
		fmt.Printf("\r%d of %d files processed (%.1f%%)", processedCount, totalFiles, percentage)
	}

	// Print newline after progress line
	fmt.Println()

	l.logger.Printf("Indexed %d of %d file(s)", successCount, totalFiles)
	if failedCount > 0 {
		l.logger.Printf("Failed: %d", failedCount)
	}

	return failedCount == 0
}

func (l *ContractLoader) VerifyIngestion() {
	time.Sleep(1 * time.Second) // Small delay to ensure documents are searchable

	count, err := l.client.CountDocuments(esIndex)
	if err != nil {
		l.logger.Printf("Could not verify document count: %v", err)
		return
	}

	l.logger.Printf("Index '%s' contains %d document(s)", esIndex, count)

	if count == 0 && l.indexedCount > 0 {
		l.logger.Printf("Warning: Expected %d document(s) but count shows 0. Documents may have failed during pipeline processing.", l.indexedCount)
	}
}

func mainContracts() {
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

	var (
		config            = flag.String("c", "config/elasticsearch.yml", "Path to Elasticsearch config YAML")
		configLong        = flag.String("config", "config/elasticsearch.yml", "Path to Elasticsearch config YAML")
		mapping           = flag.String("m", "config/mappings-contracts.json", "Path to mappings JSON")
		mappingLong       = flag.String("mapping", "config/mappings-contracts.json", "Path to mappings JSON")
		pdfPath           = flag.String("pdf-path", "", "Path to PDF file or directory containing PDFs (default: data)")
		setupOnly         = flag.Bool("setup-only", false, "Only setup infrastructure (pipeline and index), skip PDF ingestion")
		ingestOnly        = flag.Bool("ingest-only", false, "Skip setup, only ingest PDFs (assumes infrastructure exists)")
		inferenceEndpoint = flag.String("inference-endpoint", "", "Inference endpoint ID (default: .elser-2-elastic, will auto-detect if not found)")
		status            = flag.Bool("status", false, "Test connection and print cluster health status")
	)

	flag.Parse()

	// Use long form if provided, otherwise use short form
	if *configLong != "config/elasticsearch.yml" {
		config = configLong
	}
	if *mappingLong != "config/mappings-contracts.json" {
		mapping = mappingLong
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)

	esConfig, err := loadConfig(*config)
	if err != nil {
		logger.Fatalf("Error loading config: %v", err)
	}

	client, err := NewElasticsearchClient(esConfig, logger)
	if err != nil {
		logger.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	if *status {
		if err := reportStatus(client, logger); err != nil {
			logger.Fatalf("Error: %v", err)
		}
		return
	}

	mappingData, err := loadMapping(*mapping)
	if err != nil {
		logger.Fatalf("Error loading mapping: %v", err)
	}

	endpoint := *inferenceEndpoint
	if endpoint == "" {
		endpoint = defaultInferenceEndpoint
	}

	loader := NewContractLoader(client, mappingData, logger, endpoint)

	// Check Elasticsearch connection
	if !loader.CheckElasticsearch() {
		logger.Fatalf("Cannot connect to Elasticsearch. Exiting.")
	}

	// Setup phase
	if !*ingestOnly {
		// Check ELSER endpoint
		if !loader.CheckInferenceEndpoint() {
			logger.Fatalf("ELSER inference endpoint not found!")
			logger.Fatalf("Please deploy ELSER via Kibana or API before continuing.")
			logger.Fatalf("See: Management → Machine Learning → Trained Models → ELSER → Deploy")
		}

		// Create pipeline
		if !loader.CreatePipeline() {
			logger.Fatalf("Failed to create pipeline. Exiting.")
		}

		// Create index (will delete existing one if present)
		if !loader.CreateIndex() {
			logger.Fatalf("Failed to create index. Exiting.")
		}
	}

	// Ingestion phase
	if !*setupOnly {
		ingestionStart := time.Now()

		pdfPathValue := *pdfPath
		if pdfPathValue == "" {
			pdfPathValue = resolvePath("data")
		}

		if !loader.IngestPdfs(pdfPathValue) {
			logger.Fatalf("PDF ingestion had errors.")
		}

		elapsed := time.Since(ingestionStart)
		logger.Printf("Total ingestion time: %.2f seconds", elapsed.Seconds())

		// Verify ingestion
		loader.VerifyIngestion()
	}
}
