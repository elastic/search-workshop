#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import yaml from 'js-yaml';
import { Command } from 'commander';
import { ElasticsearchClient } from './import_flights.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Extend ElasticsearchClient with contract-specific methods
class ElasticsearchClientContracts extends ElasticsearchClient {
  constructor(config, logger) {
    super(config, logger);
    this.config = config;
  }

  async createPipeline(name, pipelineConfig) {
    try {
      await this.client.ingest.putPipeline({
        id: name,
        body: pipelineConfig,
      });
      this.logger.info(`Pipeline '${name}' created/updated`);
    } catch (error) {
      throw new Error(`Pipeline creation failed: ${error.message}`);
    }
  }

  async indexDocument(indexName, document, pipeline = null) {
    try {
      const options = {
        index: indexName,
        body: document,
        refresh: 'wait_for',
      };
      if (pipeline) {
        options.pipeline = pipeline;
      }
      const response = await this.client.index(options);
      
      // Check for errors in the response (pipeline failures might return 200 but with errors)
      if (response.body && response.body.error) {
        throw new Error(`Document indexing failed: ${JSON.stringify(response.body.error)}`);
      }
      
      return response;
    } catch (error) {
      // Enhanced error logging
      if (error.meta && error.meta.body) {
        throw new Error(`Document indexing failed: ${error.message}. Response: ${JSON.stringify(error.meta.body)}`);
      }
      throw new Error(`Document indexing failed: ${error.message}`);
    }
  }

  async getInferenceEndpoints() {
    try {
      const response = await this.client.transport.request({
        method: 'GET',
        path: '/_inference/_all',
      });

      const body = response?.body ?? response ?? {};

      if (Array.isArray(body.endpoints)) {
        return body.endpoints;
      }

      if (body.endpoints && typeof body.endpoints === 'object') {
        return Object.entries(body.endpoints).map(([key, value]) => ({
          inference_id: key,
          ...value,
        }));
      }

      const fallbackKeys = Object.keys(body).filter((k) => k !== '_shards');
      if (fallbackKeys.length > 0) {
        return fallbackKeys.map((key) => ({
          inference_id: key,
          ...(typeof body[key] === 'object' ? body[key] : {}),
        }));
      }

      return [];
    } catch (error) {
      this.logger.warn(`Failed to get inference endpoints: ${error.message}`);
      return [];
    }
  }

  async countDocuments(indexName) {
    try {
      const response = await this.client.count({ index: indexName });
      return response?.body?.count ?? response?.count ?? 0;
    } catch (error) {
      this.logger.warn(`Failed to count documents: ${error.message}`);
      return 0;
    }
  }
}

class ContractLoader {
  static ES_INDEX = 'contracts';
  static PIPELINE_NAME = 'pdf_pipeline';
  static DEFAULT_INFERENCE_ENDPOINT = '.elser-2-elastic';

  constructor({ client, mapping, logger, inferenceEndpoint = null }) {
    this.client = client;
    this.mapping = mapping;
    this.logger = logger;
    this.inferenceEndpoint = inferenceEndpoint || ContractLoader.DEFAULT_INFERENCE_ENDPOINT;
    this.indexedCount = 0;
  }

  async checkElasticsearch() {
    try {
      const health = await this.client.clusterHealth();
      this.logger.info(`Cluster: ${health.cluster_name || 'unknown'}`);
      this.logger.info(`Status: ${health.status}`);
      return true;
    } catch (error) {
      this.logger.error(`Connection error: ${error.message}`);
      return false;
    }
  }

  async checkInferenceEndpoint() {
    try {
      const endpoints = await this.client.getInferenceEndpoints();

      // First, try to find the specified endpoint
      const foundEndpoint = endpoints.find(
        (ep) =>
          ep.inference_id === this.inferenceEndpoint ||
          ep.endpoint === this.inferenceEndpoint ||
          ep.id === this.inferenceEndpoint ||
          ep.name === this.inferenceEndpoint
      );

      if (foundEndpoint) {
        this.logger.info(`Found inference endpoint: ${this.inferenceEndpoint}`);
        return true;
      }

      // Auto-detect ELSER endpoints
      const elserEndpoints = endpoints.filter((ep) =>
        ep.inference_id?.toLowerCase().includes('elser') ||
        ep.endpoint?.toLowerCase().includes('elser') ||
        ep.id?.toLowerCase().includes('elser') ||
        ep.name?.toLowerCase().includes('elser')
      );

      if (elserEndpoints.length > 0) {
        // Prefer endpoints starting with .elser-2- or .elser_model_2
        const preferred = elserEndpoints.filter((ep) => {
          const id = ep.inference_id || ep.endpoint || ep.id || ep.name;
          return id?.includes('.elser-2-') || id?.includes('.elser_model_2');
        });

        if (preferred.length > 0) {
          this.inferenceEndpoint =
            preferred[0].inference_id ||
            preferred[0].endpoint ||
            preferred[0].id ||
            preferred[0].name;
        } else {
          this.inferenceEndpoint =
            elserEndpoints[0].inference_id ||
            elserEndpoints[0].endpoint ||
            elserEndpoints[0].id ||
            elserEndpoints[0].name;
        }

        this.logger.warn(`Specified endpoint not found, using auto-detected: ${this.inferenceEndpoint}`);
        return true;
      }

      this.logger.error(`Inference endpoint '${this.inferenceEndpoint}' not found`);
      this.logger.info('Available endpoints:');
      endpoints.forEach((ep) => {
        const id = ep.inference_id || ep.endpoint || ep.id || ep.name || '<unknown>';
        this.logger.info(`  - ${id}`);
      });
      return false;
    } catch (error) {
      this.logger.warn(`Error checking inference endpoint: ${error.message}`);
      this.logger.warn('Continuing anyway...');
      return true;
    }
  }

  async createPipeline() {
    const pipelineConfig = {
      description: 'Extract text from PDF - semantic_text field handles chunking and embeddings',
      processors: [
        {
          attachment: {
            field: 'data',
            target_field: 'attachment',
            remove_binary: true,
          },
        },
        {
          set: {
            field: 'semantic_content',
            copy_from: 'attachment.content',
            ignore_empty_value: true,
          },
        },
        {
          remove: {
            field: 'data',
            ignore_missing: true,
          },
        },
        {
          set: {
            field: 'upload_date',
            value: '{{ _ingest.timestamp }}',
          },
        },
      ],
    };

    try {
      await this.client.createPipeline(ContractLoader.PIPELINE_NAME, pipelineConfig);
      return true;
    } catch (error) {
      this.logger.error(`Error creating pipeline: ${error.message}`);
      return false;
    }
  }

  async createIndex() {
    // Delete index if it exists before creating a new one
    if (await this.client.indexExists(ContractLoader.ES_INDEX)) {
      this.logger.info(`Deleting existing index '${ContractLoader.ES_INDEX}' before import`);
      if (await this.client.deleteIndex(ContractLoader.ES_INDEX)) {
        this.logger.info(`Index '${ContractLoader.ES_INDEX}' deleted`);
      } else {
        this.logger.warn(`Failed to delete index '${ContractLoader.ES_INDEX}'`);
      }
    }

    // Update mapping with detected inference endpoint
    const mappingWithInference = JSON.parse(JSON.stringify(this.mapping));
    if (
      mappingWithInference.mappings &&
      mappingWithInference.mappings.properties &&
      mappingWithInference.mappings.properties.semantic_content
    ) {
      mappingWithInference.mappings.properties.semantic_content.inference_id = this.inferenceEndpoint;
    }

    this.logger.info(`Creating index: ${ContractLoader.ES_INDEX}`);
    try {
      await this.client.createIndex(ContractLoader.ES_INDEX, mappingWithInference);
      this.logger.info(`Successfully created index: ${ContractLoader.ES_INDEX}`);
      return true;
    } catch (error) {
      this.logger.error(`Error creating index: ${error.message}`);
      return false;
    }
  }

  extractAirlineName(filename) {
    const filenameLower = filename.toLowerCase();

    if (filenameLower.includes('american')) {
      return 'American Airlines';
    } else if (filenameLower.includes('southwest')) {
      return 'Southwest';
    } else if (filenameLower.includes('united')) {
      return 'United';
    } else if (filenameLower.includes('delta') || filenameLower.includes('dl-')) {
      return 'Delta';
    } else {
      return 'Unknown';
    }
  }

  getPdfFiles(pdfPath) {
    const resolvedPath = resolvePath(pdfPath);

    if (!fs.existsSync(resolvedPath)) {
      this.logger.error(`Path '${pdfPath}' does not exist`);
      return [];
    }

    const stats = fs.statSync(resolvedPath);

    if (stats.isFile()) {
      if (path.extname(resolvedPath).toLowerCase() === '.pdf') {
        return [resolvedPath];
      } else {
        this.logger.error(`'${pdfPath}' is not a PDF file`);
        return [];
      }
    } else if (stats.isDirectory()) {
      const files = fs.readdirSync(resolvedPath);
      const pdfFiles = files
        .filter((file) => path.extname(file).toLowerCase() === '.pdf')
        .map((file) => path.join(resolvedPath, file))
        .sort();

      if (pdfFiles.length === 0) {
        this.logger.warn(`No PDF files found in directory '${pdfPath}'`);
      }
      return pdfFiles;
    } else {
      return [];
    }
  }

  async indexPdf(pdfPath) {
    const filename = path.basename(pdfPath);
    const airline = this.extractAirlineName(filename);

    try {
      // Read and encode the PDF
      const pdfData = fs.readFileSync(pdfPath);
      const encodedPdf = pdfData.toString('base64');

      // Index the document
      const document = {
        data: encodedPdf,
        filename: filename,
        airline: airline,
      };

      await this.client.indexDocument(
        ContractLoader.ES_INDEX,
        document,
        ContractLoader.PIPELINE_NAME
      );

      this.logger.info(`Indexed: ${filename} (airline: ${airline})`);
      this.indexedCount += 1;
      return true;
    } catch (error) {
      this.logger.error(`Error processing ${filename}: ${error.message}`);
      if (error.stack) {
        this.logger.error(error.stack);
      }
      return false;
    }
  }

  async ingestPdfs(pdfPath) {
    const pdfFiles = this.getPdfFiles(pdfPath);

    if (pdfFiles.length === 0) {
      this.logger.error('No PDF files to process');
      return false;
    }

    this.logger.info(`Processing ${pdfFiles.length} PDF file(s)...`);

    let successCount = 0;
    let failedCount = 0;

    for (const pdfFile of pdfFiles) {
      if (await this.indexPdf(pdfFile)) {
        successCount += 1;
      } else {
        failedCount += 1;
      }
    }

    this.logger.info(`Indexed ${successCount} of ${pdfFiles.length} file(s)`);
    if (failedCount > 0) {
      this.logger.warn(`Failed: ${failedCount}`);
    }

    return failedCount === 0;
  }

  async verifyIngestion() {
    try {
      // Wait a bit for pipeline processing
      await new Promise((resolve) => setTimeout(resolve, 1000));
      
      const count = await this.client.countDocuments(ContractLoader.ES_INDEX);
      this.logger.info(`Index '${ContractLoader.ES_INDEX}' contains ${count} document(s)`);
      
      if (this.indexedCount > 0 && count === 0) {
        this.logger.warn(
          `Warning: Expected ${this.indexedCount} document(s) but count shows 0. ` +
          `Documents may have failed during pipeline processing.`
        );
      }
      
      return true;
    } catch (error) {
      this.logger.warn(`Could not verify document count: ${error.message}`);
      return true;
    }
  }
}

// Logger implementation
class Logger {
  info(message) {
    console.log(`[INFO] ${message}`);
  }

  warn(message) {
    console.warn(`[WARN] ${message}`);
  }

  error(message) {
    console.error(`[ERROR] ${message}`);
  }

  debug(message) {
    if (process.env.DEBUG) {
      console.log(`[DEBUG] ${message}`);
    }
  }
}

function resolvePath(filePath) {
  // If path is absolute, use as-is
  if (path.isAbsolute(filePath)) {
    return filePath;
  }

  // Try relative to current directory first (if it exists)
  if (fs.existsSync(filePath)) {
    return path.resolve(filePath);
  }

  // Try relative to workspace root (one level up from script directory)
  const workspaceRoot = path.resolve(__dirname, '..');
  const candidate = path.resolve(workspaceRoot, filePath);

  // Return resolved path even if file doesn't exist (for optional files)
  return candidate;
}

function loadConfig(configPath) {
  try {
    const resolvedPath = resolvePath(configPath);
    const content = fs.readFileSync(resolvedPath, 'utf-8');
    return yaml.load(content) || {};
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error(`Config file not found: ${configPath} (tried: ${resolvePath(configPath)})`);
    }
    throw error;
  }
}

function loadMapping(mappingPath) {
  try {
    const resolvedPath = resolvePath(mappingPath);
    const content = fs.readFileSync(resolvedPath, 'utf-8');
    return JSON.parse(content);
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error(`Mapping file not found: ${mappingPath} (tried: ${resolvePath(mappingPath)})`);
    }
    throw error;
  }
}

async function reportStatus(client, logger) {
  try {
    const status = await client.clusterHealth();
    logger.info(`Cluster status: ${status.status}`);
    logger.info(`Active shards: ${status.active_shards}, node count: ${status.number_of_nodes}`);
  } catch (error) {
    logger.error(`Failed to retrieve cluster status: ${error.message}`);
    process.exit(1);
  }
}

async function main() {
  const program = new Command();
  program
    .name('import_contracts.js')
    .description('Import PDF contracts into Elasticsearch')
    .option('-c, --config <path>', 'Path to Elasticsearch config YAML', 'config/elasticsearch.yml')
    .option('-m, --mapping <path>', 'Path to mappings JSON', 'config/mappings-contracts.json')
    .option('--pdf-path <path>', 'Path to PDF file or directory containing PDFs (default: data)')
    .option('--setup-only', 'Only setup infrastructure (pipeline and index), skip PDF ingestion')
    .option('--ingest-only', 'Skip setup, only ingest PDFs (assumes infrastructure exists)')
    .option('--inference-endpoint <name>', 'Inference endpoint ID (default: .elser-2-elastic, will auto-detect if not found)')
    .option('--status', 'Test connection and print cluster health status')
    .parse(process.argv);

  const options = program.opts();
  const logger = new Logger();

  // Execute based on options
  if (options.status) {
    const config = loadConfig(options.config);
    const client = new ElasticsearchClientContracts(config, logger);
    await reportStatus(client, logger);
    return;
  }

  const config = loadConfig(options.config);
  const client = new ElasticsearchClientContracts(config, logger);
  const mapping = loadMapping(options.mapping);

  const inferenceEndpoint = options.inferenceEndpoint || ContractLoader.DEFAULT_INFERENCE_ENDPOINT;

  const loader = new ContractLoader({
    client,
    mapping,
    logger,
    inferenceEndpoint,
  });

  // Check Elasticsearch connection
  if (!(await loader.checkElasticsearch())) {
    logger.error('Cannot connect to Elasticsearch. Exiting.');
    process.exit(1);
  }

  // Setup phase
  if (!options.ingestOnly) {
    // Check ELSER endpoint
    if (!(await loader.checkInferenceEndpoint())) {
      logger.error('ELSER inference endpoint not found!');
      logger.error('Please deploy ELSER via Kibana or API before continuing.');
      logger.error('See: Management → Machine Learning → Trained Models → ELSER → Deploy');
      process.exit(1);
    }

    // Create pipeline
    if (!(await loader.createPipeline())) {
      logger.error('Failed to create pipeline. Exiting.');
      process.exit(1);
    }

    // Create index (will delete existing one if present)
    if (!(await loader.createIndex())) {
      logger.error('Failed to create index. Exiting.');
      process.exit(1);
    }
  }

  // Ingestion phase
  if (!options.setupOnly) {
    const startTime = Date.now();

    const pdfPath = options.pdfPath || resolvePath('data');

    if (!(await loader.ingestPdfs(pdfPath))) {
      logger.error('PDF ingestion had errors.');
      process.exit(1);
    }

    const elapsedTime = ((Date.now() - startTime) / 1000).toFixed(2);
    logger.info(`Total ingestion time: ${elapsedTime} seconds`);

    // Verify ingestion
    await loader.verifyIngestion();
  }
}

// Run main if this file is executed directly
(async () => {
  const startTime = Date.now();
  try {
    await main();
  } catch (error) {
    console.error('Error:', error.message);
    if (error.stack) {
      console.error(error.stack);
    }
    process.exit(1);
  } finally {
    const endTime = Date.now();
    const durationMs = endTime - startTime;
    const minutes = Math.floor(durationMs / 60000);
    const seconds = ((durationMs % 60000) / 1000).toFixed(2);
    if (minutes > 0) {
      console.log(`\nTotal time: ${minutes}m ${seconds}s`);
    } else {
      console.log(`\nTotal time: ${seconds}s`);
    }
  }
})();
