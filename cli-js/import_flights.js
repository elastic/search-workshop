#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { gunzipSync } from 'zlib';
import { execSync } from 'child_process';
import { parse as parseCSV } from 'csv-parse/sync';
import yaml from 'js-yaml';
import { Client } from '@elastic/elasticsearch';
import { Command } from 'commander';

const BATCH_SIZE = 500;

export class ElasticsearchClient {
  constructor(config, logger) {
    const endpoint = config.endpoint;
    if (!endpoint) {
      throw new Error('endpoint is required in the Elasticsearch config');
    }

    this.logger = logger;
    this.endpoint = endpoint;

    // Build client configuration
    const clientConfig = {
      node: endpoint,
    };

    // Handle authentication
    if (config.api_key) {
      clientConfig.auth = {
        apiKey: config.api_key,
      };
    } else if (config.user && config.password) {
      clientConfig.auth = {
        username: config.user,
        password: config.password,
      };
    }

    // Handle custom headers
    if (config.headers && Object.keys(config.headers).length > 0) {
      clientConfig.headers = config.headers;
    }

    // Handle SSL verification
    if (config.ssl_verify === false) {
      clientConfig.tls = {
        rejectUnauthorized: false,
      };
    } else if (config.ca_file || config.ca_path) {
      // Note: The official client handles CA files through the tls option
      // This is a simplified implementation - for production, you may need
      // to use https.Agent with custom CA certificates
      clientConfig.tls = {};
      if (config.ca_file) {
        clientConfig.tls.ca = fs.readFileSync(config.ca_file);
      }
    }

    this.client = new Client(clientConfig);
  }

  async indexExists(name) {
    try {
      const response = await this.client.indices.exists({ index: name });
      // Node client returns either a boolean or a response with a boolean body depending on version/config
      return typeof response === 'boolean' ? response : Boolean(response?.body);
    } catch (error) {
      if (error.message && (error.message.includes('Connection refused') || error.message.includes('timeout'))) {
        throw new Error(
          `Cannot connect to Elasticsearch at ${this.endpoint}: ${error.message}. ` +
          'Please check your endpoint configuration and network connectivity.'
        );
      }
      throw new Error(`Failed to check index existence: ${error.message}`);
    }
  }

  async createIndex(name, mapping) {
    try {
      await this.client.indices.create({
        index: name,
        body: mapping,
      });
      this.logger.info(`Index '${name}' created`);
    } catch (error) {
      if (error.statusCode === 400 && error.body?.error?.type === 'resource_already_exists_exception') {
        this.logger.warn(`Index '${name}' already exists (conflict)`);
      } else if (error.message && (error.message.includes('Connection refused') || error.message.includes('timeout'))) {
        throw new Error(
          `Cannot connect to Elasticsearch at ${this.endpoint}: ${error.message}. ` +
          'Please check your endpoint configuration and network connectivity.'
        );
      } else {
        throw new Error(`Index creation failed: ${error.message}`);
      }
    }
  }

  async bulk(index, payload, refresh = false) {
    try {
      // The official client expects body as an array of operations or NDJSON string
      // Since payload is already NDJSON format (string), we pass it directly
      const response = await this.client.bulk({
        body: payload, // NDJSON string
        refresh: refresh ? 'wait_for' : false,
      });
      // The official client returns the response with body property containing errors and items
      return response.body || response;
    } catch (error) {
      throw new Error(`Bulk request failed: ${error.message}`);
    }
  }

  async clusterHealth() {
    try {
      const response = await this.client.cluster.health();
      // Newer clients wrap the payload in a body property
      return response?.body ?? response;
    } catch (error) {
      throw new Error(`Cluster health request failed: ${error.message}`);
    }
  }

  async deleteIndex(name) {
    try {
      await this.client.indices.delete({ index: name });
      return true;
    } catch (error) {
      if (error.statusCode === 404) {
        return false;
      }
      throw new Error(`Index deletion failed: ${error.message}`);
    }
  }

  async listIndices(pattern = '*') {
    try {
      const response = await this.client.cat.indices({
        index: pattern,
        format: 'json',
      });
      // The cat API returns an array directly in the response body
      const indices = Array.isArray(response.body) ? response.body : response.body.split('\n').filter(Boolean).map(line => JSON.parse(line));
      return indices.map((idx) => idx.index).filter(Boolean);
    } catch (error) {
      throw new Error(`Failed to list indices: ${error.message}`);
    }
  }

  async deleteIndicesByPattern(pattern) {
    const indices = await this.listIndices(pattern);
    if (indices.length === 0) {
      return [];
    }

    const deleted = [];
    for (const indexName of indices) {
      if (await this.deleteIndex(indexName)) {
        deleted.push(indexName);
      }
    }
    return deleted;
  }
}

function presence(value) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  return trimmed === '' ? null : trimmed;
}

class AirportLookup {
  constructor({ airportsFile, logger }) {
    this.logger = logger;
    this.airports = {};
    if (airportsFile && fs.existsSync(airportsFile)) {
      // Load synchronously for constructor
      this.loadAirportsSync(airportsFile);
    }
  }

  lookupCoordinates(iataCode) {
    if (!iataCode) return null;

    const airport = this.airports[iataCode.toUpperCase()];
    if (!airport) return null;

    return `${airport.lat},${airport.lon}`;
  }

  loadAirportsSync(filePath) {
    this.logger.info(`Loading airports from ${filePath}`);

    let count = 0;
    const data = fs.readFileSync(filePath);
    const decompressed = gunzipSync(data);
    const content = decompressed.toString('utf-8');
    
    const records = parseCSV(content, {
      columns: false,
      skip_empty_lines: true,
    });

    for (const row of records) {
      const iata = row[4]?.trim();
      if (!iata || iata === '\\N') continue;

      const latStr = row[6]?.trim();
      const lonStr = row[7]?.trim();
      if (!latStr || !lonStr) continue;

      const lat = parseFloat(latStr);
      const lon = parseFloat(lonStr);
      if (isNaN(lat) || isNaN(lon)) continue;

      this.airports[iata.toUpperCase()] = { lat, lon };
      count++;
    }

    this.logger.info(`Loaded ${count} airports into lookup table`);
  }
}

class CancellationLookup {
  constructor({ cancellationsFile, logger }) {
    this.logger = logger;
    this.cancellations = {};
    if (cancellationsFile && fs.existsSync(cancellationsFile)) {
      this.loadCancellations(cancellationsFile);
    }
  }

  lookupReason(code) {
    if (!code) return null;
    return this.cancellations[code.toUpperCase()] || null;
  }

  loadCancellations(filePath) {
    this.logger.info(`Loading cancellations from ${filePath}`);

    const content = fs.readFileSync(filePath, 'utf-8');
    const records = parseCSV(content, {
      columns: true,
      skip_empty_lines: true,
    });

    let count = 0;
    for (const row of records) {
      const code = row.Code?.trim();
      const description = row.Description?.trim();
      if (!code || !description) continue;

      this.cancellations[code.toUpperCase()] = description;
      count++;
    }

    this.logger.info(`Loaded ${count} cancellation reasons into lookup table`);
  }
}

class FlightLoader {
  constructor({
    client = null,
    mapping,
    index,
    logger,
    batchSize = BATCH_SIZE,
    refresh = false,
    airportsFile = null,
    cancellationsFile = null,
  }) {
    this.client = client;
    this.mapping = mapping;
    this.indexPrefix = index;
    this.logger = logger;
    this.batchSize = batchSize;
    this.refresh = refresh;
    this.airportLookup = new AirportLookup({ airportsFile, logger });
    this.cancellationLookup = new CancellationLookup({ cancellationsFile, logger });
    this.ensuredIndices = new Set();
    this.loadedRecords = 0;
    this.totalRecords = 0;
  }

  async ensureIndex(indexName) {
    if (!this.client) return;

    if (this.ensuredIndices.has(indexName)) {
      this.logger.debug(`Index ${indexName} already ensured in this session`);
      return;
    }

    // Delete index if it exists before creating a new one
    if (await this.client.indexExists(indexName)) {
      this.logger.info(`Deleting existing index '${indexName}' before import`);
      if (await this.client.deleteIndex(indexName)) {
        this.logger.info(`Index '${indexName}' deleted`);
      } else {
        this.logger.warn(`Failed to delete index '${indexName}'`);
      }
    }

    this.logger.info(`Creating index: ${indexName}`);
    await this.client.createIndex(indexName, this.mapping);
    this.ensuredIndices.add(indexName);
    this.logger.info(`Successfully created index: ${indexName}`);
  }

  async importFiles(files) {
    this.logger.info(`Counting records in ${files.length} file(s)...`);
    this.totalRecords = await this.countTotalRecordsFast(files);
    this.logger.info(`Total records to import: ${this.formatNumber(this.totalRecords)}`);
    this.logger.info(`Importing ${files.length} file(s)...`);

    for (const filePath of files) {
      await this.importFile(filePath);
    }

    process.stdout.write('\n');
    this.logger.info(
      `Import complete: ${this.formatNumber(this.loadedRecords)} of ` +
      `${this.formatNumber(this.totalRecords)} records loaded`
    );
  }

  async sampleDocument(filePath) {
    if (!fs.statSync(filePath).isFile()) {
      this.logger.warn(`Skipping ${filePath} (not a regular file)`);
      return null;
    }

    this.logger.info(`Sampling first document from ${filePath}`);

    const rows = await this.readCSVRows(filePath, 1);
    if (rows.length === 0) return null;

    return this.transformRow(rows[0]);
  }

  formatNumber(number) {
    return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  }

  async countTotalRecordsFast(files) {
    let total = 0;
    for (const filePath of files) {
      if (!fs.statSync(filePath).isFile()) continue;
      const lineCount = this.countLinesFast(filePath);
      total += Math.max(lineCount - 1, 0); // Subtract 1 for CSV header
    }
    return total;
  }

  countLinesFast(filePath) {
    try {
      const ext = path.extname(filePath).toLowerCase();
      if (ext === '.zip') {
        const entry = this.csvEntryInZip(filePath);
        if (!entry) return 0;
        const result = execSync(
          `unzip -p ${escapeShell(filePath)} ${escapeShell(entry)} | wc -l`,
          { encoding: 'utf-8' }
        ).trim();
        return parseInt(result, 10);
      } else if (filePath.toLowerCase().endsWith('.gz')) {
        const result = execSync(
          `gunzip -c ${escapeShell(filePath)} | wc -l`,
          { encoding: 'utf-8' }
        ).trim();
        return parseInt(result, 10);
      } else {
        const result = execSync(`wc -l ${escapeShell(filePath)}`, { encoding: 'utf-8' }).trim();
        return parseInt(result.split(/\s+/)[0], 10);
      }
    } catch (error) {
      this.logger.warn(`Failed to count lines in ${filePath}: ${error.message}`);
      return 0;
    }
  }

  async importFile(filePath) {
    if (!fs.statSync(filePath).isFile()) {
      this.logger.warn(`Skipping ${filePath} (not a regular file)`);
      return;
    }

    this.logger.info(`Importing ${filePath}`);

    const [fileYear, fileMonth] = this.extractYearMonthFromFilename(filePath);
    const indexBuffers = {};
    let indexedDocs = 0;
    let processedRows = 0;

    const rows = await this.readCSVRows(filePath);
    for (const row of rows) {
      processedRows++;

      if (processedRows === 1) {
        const hasTimestamp = row.hasOwnProperty('@timestamp');
        const hasFlightDate = row.hasOwnProperty('FlightDate');
        if (!hasTimestamp && !hasFlightDate) {
          const headers = Object.keys(row).slice(0, 10).join(', ');
          this.logger.warn(
            `CSV headers don't include '@timestamp' or 'FlightDate'. Available headers: ${headers}`
          );
        }
      }

      const doc = this.transformRow(row);
      if (!doc || Object.keys(doc).length === 0) continue;

      const timestamp = doc['@timestamp'];
      const indexName = this.extractIndexName(timestamp, fileYear, fileMonth);
      if (!indexName) {
        const timestampRaw = row['@timestamp'] || row['FlightDate'];
        this.logger.warn(
          `Skipping document - missing or invalid timestamp. Raw value: ${JSON.stringify(timestampRaw)}, ` +
          `parsed timestamp: ${JSON.stringify(timestamp)}. Row ${processedRows}: ` +
          `Origin=${row['Origin']}, Dest=${row['Dest']}, Airline=${row['Reporting_Airline']}`
        );
        continue;
      }

      // Remove null/undefined values
      Object.keys(doc).forEach((key) => {
        if (doc[key] == null) delete doc[key];
      });

      await this.ensureIndex(indexName);

      if (!indexBuffers[indexName]) {
        indexBuffers[indexName] = { lines: [], count: 0 };
      }

      const buffer = indexBuffers[indexName];
      buffer.lines.push(JSON.stringify({ index: { _index: indexName } }));
      buffer.lines.push(JSON.stringify(doc));
      buffer.count++;

      if (buffer.count >= this.batchSize) {
        indexedDocs += await this.flushIndex(indexName, buffer.lines, buffer.count);
        buffer.lines = [];
        buffer.count = 0;
      }
    }

    for (const [indexName, buffer] of Object.entries(indexBuffers)) {
      if (buffer.count > 0) {
        indexedDocs += await this.flushIndex(indexName, buffer.lines, buffer.count);
      }
    }

    this.logger.info(
      `Finished ${filePath} (rows processed: ${processedRows}, documents indexed: ${indexedDocs})`
    );
  }

  async flushIndex(indexName, lines, docCount) {
    const payload = lines.join('\n') + '\n';
    const result = await this.client.bulk(indexName, payload, this.refresh);

    if (result.errors) {
      const errors = result.items
        ?.map((item) => item.index)
        .filter((info) => info && info.error)
        .slice(0, 5) || [];
      for (const error of errors) {
        this.logger.error(`Bulk item error for ${indexName}: ${JSON.stringify(error.error)}`);
      }
      throw new Error(`Bulk indexing reported errors for ${indexName}; aborting`);
    }

    this.loadedRecords += docCount;
    if (this.totalRecords > 0) {
      const percentage = Math.round((this.loadedRecords / this.totalRecords) * 100 * 10) / 10;
      process.stdout.write(
        `\r${this.formatNumber(this.loadedRecords)} of ${this.formatNumber(this.totalRecords)} ` +
        `records loaded (${percentage}%)`
      );
    } else {
      process.stdout.write(`\r${this.formatNumber(this.loadedRecords)} records loaded`);
    }

    return docCount;
  }

  async readCSVRows(filePath, limit = null) {
    let content;
    const ext = path.extname(filePath).toLowerCase();

    if (ext === '.zip') {
      const entry = this.csvEntryInZip(filePath);
      if (!entry) {
        throw new Error(`No CSV entry found in ${filePath}`);
      }
      const result = execSync(
        `unzip -p ${escapeShell(filePath)} ${escapeShell(entry)}`,
        { encoding: 'utf-8', maxBuffer: 1024 * 1024 * 100 }
      );
      content = result;
    } else if (filePath.toLowerCase().endsWith('.gz')) {
      const data = fs.readFileSync(filePath);
      const decompressed = gunzipSync(data);
      content = decompressed.toString('utf-8');
    } else {
      content = fs.readFileSync(filePath, 'utf-8');
    }

    const records = parseCSV(content, {
      columns: true,
      skip_empty_lines: true,
      to: limit || undefined,
    });

    return limit ? records.slice(0, limit) : records;
  }

  csvEntryInZip(zipPath) {
    try {
      const stdout = execSync(`unzip -Z1 ${escapeShell(zipPath)}`, { encoding: 'utf-8' });
      return stdout.split('\n').find((line) => line.toLowerCase().endsWith('.csv'));
    } catch (error) {
      throw new Error(`Failed to list entries in ${zipPath}: ${error.message}`);
    }
  }

  extractIndexName(timestamp, fileYear, fileMonth) {
    if (fileYear && fileMonth) {
      return `${this.indexPrefix}-${fileYear}-${fileMonth}`;
    }

    if (fileYear) {
      return `${this.indexPrefix}-${fileYear}`;
    }

    if (!timestamp) return null;

    const match = timestamp.match(/^(\d{4})-(\d{2})-\d{2}/);
    if (match) {
      const year = match[1];
      return `${this.indexPrefix}-${year}`;
    }

    this.logger.warn(`Unable to parse timestamp format: ${timestamp}`);
    return null;
  }

  extractYearMonthFromFilename(filePath) {
    let basename = path.basename(filePath);
    while (true) {
      const newBasename = basename.replace(/\.(gz|csv|zip)$/i, '');
      if (newBasename === basename) break;
      basename = newBasename;
    }

    const match1 = basename.match(/-(\d{4})-(\d{2})$/);
    if (match1) {
      return [match1[1], match1[2]];
    }

    const match2 = basename.match(/-(\d{4})$/);
    if (match2) {
      return [match2[1], null];
    }

    return [null, null];
  }

  transformRow(row) {
    const doc = {};

    const timestamp = present(row['@timestamp']) || present(row['FlightDate']);
    const flightDate = timestamp;
    const reportingAirline = present(row['Reporting_Airline']);
    const flightNumber = present(row['Flight_Number_Reporting_Airline']);
    const origin = present(row['Origin']);
    const dest = present(row['Dest']);

    if (flightDate && reportingAirline && flightNumber && origin && dest) {
      doc.FlightID = `${flightDate}_${reportingAirline}_${flightNumber}_${origin}_${dest}`;
    }

    doc['@timestamp'] = timestamp;

    doc['Reporting_Airline'] = reportingAirline;
    doc['Tail_Number'] = present(row['Tail_Number']);
    doc['Flight_Number'] = flightNumber;
    doc['Origin'] = origin;
    doc['Dest'] = dest;

    doc['CRSDepTimeLocal'] = toInteger(row['CRSDepTime']);
    doc['DepDelayMin'] = toInteger(row['DepDelay']);
    doc['TaxiOutMin'] = toInteger(row['TaxiOut']);
    doc['TaxiInMin'] = toInteger(row['TaxiIn']);
    doc['CRSArrTimeLocal'] = toInteger(row['CRSArrTime']);
    doc['ArrDelayMin'] = toInteger(row['ArrDelay']);

    doc['Cancelled'] = toBoolean(row['Cancelled']);
    doc['Diverted'] = toBoolean(row['Diverted']);

    const cancellationCode = present(row['CancellationCode']);
    doc['CancellationCode'] = cancellationCode;

    const cancellationReason = this.cancellationLookup.lookupReason(cancellationCode);
    if (cancellationReason) {
      doc['CancellationReason'] = cancellationReason;
    }

    doc['ActualElapsedTimeMin'] = toInteger(row['ActualElapsedTime']);
    doc['AirTimeMin'] = toInteger(row['AirTime']);

    doc['Flights'] = toInteger(row['Flights']);
    doc['DistanceMiles'] = toInteger(row['Distance']);

    doc['CarrierDelayMin'] = toInteger(row['CarrierDelay']);
    doc['WeatherDelayMin'] = toInteger(row['WeatherDelay']);
    doc['NASDelayMin'] = toInteger(row['NASDelay']);
    doc['SecurityDelayMin'] = toInteger(row['SecurityDelay']);
    doc['LateAircraftDelayMin'] = toInteger(row['LateAircraftDelay']);

    const originLocation = this.airportLookup.lookupCoordinates(origin);
    if (originLocation) {
      doc['OriginLocation'] = originLocation;
    }

    const destLocation = this.airportLookup.lookupCoordinates(dest);
    if (destLocation) {
      doc['DestLocation'] = destLocation;
    }

    return doc;
  }
}

function present(value) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  return trimmed === '' ? null : trimmed;
}

function toFloat(value) {
  const val = present(value);
  if (!val) return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : num;
}

function toInteger(value) {
  const val = present(value);
  if (!val) return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : Math.round(num);
}

function toBoolean(value) {
  const val = present(value);
  if (!val) return null;

  const lower = val.toLowerCase();
  if (['true', 't', 'yes', 'y'].includes(lower)) return true;
  if (['false', 'f', 'no', 'n'].includes(lower)) return false;

  const numeric = parseFloat(val);
  if (!isNaN(numeric)) {
    return numeric > 0;
  }

  return null;
}

function escapeShell(str) {
  return `'${String(str).replace(/'/g, "'\\''")}'`;
}

function loadConfig(path) {
  try {
    const content = fs.readFileSync(path, 'utf-8');
    return yaml.load(content) || {};
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error(`Config file not found: ${path}`);
    }
    throw error;
  }
}

function loadMapping(path) {
  try {
    const content = fs.readFileSync(path, 'utf-8');
    return JSON.parse(content);
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error(`Mapping file not found: ${path}`);
    }
    throw error;
  }
}

function filesToProcess(options) {
  const dataDir = options.dataDir || 'data';

  if (options.file) {
    return [resolveFilePath(options.file, dataDir)];
  }

  if (options.globFiles && options.globFiles.length > 0) {
    const files = options.globFiles
      .map((f) => resolveFilePath(f, dataDir))
      .filter((f) => fs.statSync(f).isFile())
      .sort();
    return files;
  }

  if (options.glob) {
    const globPattern = options.glob;
    let files = [];

    if (path.isAbsolute(globPattern)) {
      files = globSync(globPattern).filter((f) => fs.statSync(f).isFile()).sort();
    } else {
      files = globSync(globPattern).filter((f) => fs.statSync(f).isFile());
      if (files.length === 0) {
        const expandedPattern = path.join(dataDir, globPattern);
        files = globSync(expandedPattern).filter((f) => fs.statSync(f).isFile());
      }
      files = files.sort();
    }

    if (files.length === 0) {
      throw new Error(`No files found matching pattern: ${globPattern}`);
    }
    return files;
  }

  const patternZip = path.join(dataDir, '*.zip');
  const patternCsv = path.join(dataDir, '*.csv');
  const patternCsvGz = path.join(dataDir, '*.csv.gz');
  const allPatterns = [patternZip, patternCsv, patternCsvGz];
  const files = allPatterns.flatMap((p) => globSync(p)).filter((f) => fs.statSync(f).isFile()).sort();

  if (files.length === 0) {
    throw new Error(`No .zip, .csv, or .csv.gz files found in ${dataDir}`);
  }
  return files;
}

function resolveFilePath(filePath, dataDir) {
  const expanded = path.resolve(filePath);
  if (fs.existsSync(expanded)) {
    return expanded;
  }

  const candidate = path.resolve(dataDir, filePath);
  if (fs.existsSync(candidate)) {
    return candidate;
  }

  throw new Error(`File not found: ${filePath}`);
}

function globSync(pattern) {
  // Simple glob implementation for common patterns
  const dir = path.dirname(pattern);
  const basename = path.basename(pattern);
  const regex = new RegExp('^' + basename.replace(/\*/g, '.*').replace(/\?/g, '.') + '$');

  if (!fs.existsSync(dir)) {
    return [];
  }

  const files = fs.readdirSync(dir);
  return files
    .filter((file) => regex.test(file))
    .map((file) => path.join(dir, file));
}

async function sampleDocument(options, logger) {
  const mapping = loadMapping(options.mapping);
  const loader = new FlightLoader({
    client: null,
    mapping,
    index: 'flights',
    logger,
    batchSize: 1,
    refresh: false,
    airportsFile: options.airportsFile,
    cancellationsFile: options.cancellationsFile,
  });

  const files = filesToProcess(options);
  if (files.length === 0) {
    logger.error('No files found to sample');
    process.exit(1);
  }

  const doc = await loader.sampleDocument(files[0]);
  if (!doc) {
    logger.error('No document found in file');
    process.exit(1);
  }

  console.log(JSON.stringify(doc, null, 2));
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

async function deleteIndicesByPattern(client, logger, pattern) {
  const patternWithWildcard = pattern.endsWith('*') ? pattern : `${pattern}-*`;
  logger.info(`Searching for indices matching pattern: ${patternWithWildcard}`);

  try {
    const deleted = await client.deleteIndicesByPattern(patternWithWildcard);

    if (deleted.length === 0) {
      logger.warn(`No indices found matching pattern: ${patternWithWildcard}`);
    } else {
      logger.info(`Deleted ${deleted.length} index(es): ${deleted.join(', ')}`);
    }
  } catch (error) {
    logger.error(`Failed to delete indices matching pattern '${pattern}': ${error.message}`);
    process.exit(1);
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

async function main() {
  // Only parse arguments if this file is being executed directly
  const isDirectExecution = process.argv[1] && (
    process.argv[1].endsWith('import_flights.js') || 
    import.meta.url.endsWith('import_flights.js')
  );
  
  if (!isDirectExecution) {
    return; // Don't run if imported as a module
  }

  const program = new Command();
  program
    .name('import_flights.js')
    .description('Import flight data into Elasticsearch')
    .option('-c, --config <path>', 'Path to Elasticsearch config YAML', 'config/elasticsearch.yml')
    .option('-m, --mapping <path>', 'Path to mappings JSON', 'config/mappings-flights.json')
    .option('-d, --data-dir <path>', 'Directory containing data files', 'data')
    .option('-f, --file <path>', 'Only import the specified file')
    .option('-a, --all', 'Import all files found in the data directory')
    .option('-g, --glob <pattern>', 'Import files matching the glob pattern')
    .option('--index <name>', 'Override index name', 'flights')
    .option('--batch-size <n>', 'Number of documents per bulk request', '500')
    .option('--refresh', 'Request an index refresh after each bulk request')
    .option('--status', 'Test connection and print cluster health status')
    .option('--delete-index', 'Delete indices matching the index pattern and exit')
    .option('--delete-all', 'Delete all flights-* indices and exit')
    .option('--sample', 'Print the first document and exit')
    .option('--airports-file <path>', 'Path to airports CSV file', 'data/airports.csv.gz')
    .option('--cancellations-file <path>', 'Path to cancellations CSV file', 'data/cancellations.csv')
    .parse(process.argv);

  const options = program.opts();
  const logger = new Logger();

  // Handle glob expansion from shell
  const remainingArgs = program.args;
  if (options.glob && remainingArgs.length > 0) {
    if (!options.glob.includes('*') && !options.glob.includes('?')) {
      options.globFiles = [options.glob, ...remainingArgs];
      options.glob = null;
    }
  }

  // Validation
  if (options.status && (options.deleteIndex || options.deleteAll)) {
    console.error('Cannot use --status with --delete-index or --delete-all');
    process.exit(1);
  }

  if (options.deleteIndex && options.deleteAll) {
    console.error('Cannot use --delete-index and --delete-all together');
    process.exit(1);
  }

  if (!options.status && !options.deleteIndex && !options.deleteAll && !options.sample) {
    const selectionOptions = [
      options.file,
      options.all,
      options.glob,
      options.globFiles,
    ].filter(Boolean);
    if (selectionOptions.length > 1) {
      console.error('Cannot use --file, --all, and --glob together (use only one)');
      process.exit(1);
    }

    if (selectionOptions.length === 0) {
      console.error('Please provide either --file PATH, --all, or --glob PATTERN');
      process.exit(1);
    }
  }

  // Execute based on options
  if (options.sample) {
    await sampleDocument(options, logger);
    return;
  }

  const config = loadConfig(options.config);
  const client = new ElasticsearchClient(config, logger);

  if (options.status) {
    await reportStatus(client, logger);
    return;
  }

  if (options.deleteIndex) {
    await deleteIndicesByPattern(client, logger, options.index);
    return;
  }

  if (options.deleteAll) {
    await deleteIndicesByPattern(client, logger, 'flights-*');
    return;
  }

  const mapping = loadMapping(options.mapping);
  const loader = new FlightLoader({
    client,
    mapping,
    index: options.index,
    logger,
    batchSize: parseInt(options.batchSize, 10),
    refresh: options.refresh,
    airportsFile: options.airportsFile,
    cancellationsFile: options.cancellationsFile,
  });

  const files = filesToProcess(options);
  await loader.importFiles(files);
}

// Run main if this file is executed directly
if (process.argv[1] && process.argv[1].endsWith('import_flights.js')) {
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
}
