use anyhow::{Context, Result};
use clap::Parser;
use csv::ReaderBuilder;
use elasticsearch::{
    auth::Credentials,
    cat::CatIndicesParts,
    cert::CertificateValidation,
    cluster::ClusterHealthParts,
    http::{
        headers::{HeaderMap, HeaderName, HeaderValue},
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts},
    params::Refresh,
    BulkParts, Elasticsearch,
};
use flate2::read::GzDecoder;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use zip::ZipArchive;

// Simple logging macros
macro_rules! info {
    ($($arg:tt)*) => {
        println!("[INFO] {}", format!($($arg)*))
    };
}

macro_rules! warn {
    ($($arg:tt)*) => {
        eprintln!("[WARN] {}", format!($($arg)*))
    };
}

macro_rules! debug {
    ($($arg:tt)*) => {
        if std::env::var("DEBUG").is_ok() {
            println!("[DEBUG] {}", format!($($arg)*));
        }
    };
}

const BATCH_SIZE: usize = 500;

#[derive(Parser)]
#[command(name = "import_flights")]
#[command(about = "Import flight data into Elasticsearch")]
struct Args {
    #[arg(short = 'c', long, default_value = "config/elasticsearch.yml")]
    config: PathBuf,

    #[arg(short = 'm', long, default_value = "config/mappings-flights.json")]
    mapping: PathBuf,

    #[arg(short = 'd', long, default_value = "data")]
    data_dir: PathBuf,

    #[arg(short = 'f', long, conflicts_with_all = ["all", "glob"])]
    file: Option<PathBuf>,

    #[arg(short = 'a', long, conflicts_with_all = ["file", "glob"])]
    all: bool,

    #[arg(short = 'g', long, conflicts_with_all = ["file", "all"])]
    glob: Option<String>,

    #[arg(long, default_value = "flights")]
    index: String,

    #[arg(long, default_value_t = BATCH_SIZE)]
    batch_size: usize,

    #[arg(long)]
    refresh: bool,

    #[arg(long)]
    status: bool,

    #[arg(long, conflicts_with = "delete_all")]
    delete_index: bool,

    #[arg(long)]
    delete_all: bool,

    #[arg(long, conflicts_with_all = ["delete_index", "delete_all", "status"])]
    sample: bool,

    #[arg(long, default_value = "data/airports.csv.gz")]
    airports_file: PathBuf,

    #[arg(long, default_value = "data/cancellations.csv")]
    cancellations_file: PathBuf,
}

#[derive(Debug, Clone)]
struct ElasticsearchConfig {
    endpoint: String,
    headers: HashMap<String, String>,
    user: Option<String>,
    password: Option<String>,
    api_key: Option<String>,
    ssl_verify: bool,
}

impl ElasticsearchConfig {
    fn from_yaml(data: &Value) -> Result<Self> {
        let normalize = |v: Option<&str>| -> Option<String> {
            v.map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        };

        let endpoint = data
            .get("endpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("endpoint is required in the Elasticsearch config"))?
            .to_string();

        let headers = data
            .get("headers")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let user = normalize(data.get("user").and_then(|v| v.as_str()));
        let password = normalize(data.get("password").and_then(|v| v.as_str()));
        let api_key = normalize(data.get("api_key").and_then(|v| v.as_str()));

        let ssl_verify = data
            .get("ssl_verify")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        Ok(Self {
            endpoint,
            headers,
            user,
            password,
            api_key,
            ssl_verify,
        })
    }
}

struct ElasticsearchClient {
    client: Elasticsearch,
}

impl ElasticsearchClient {
    fn new(config: ElasticsearchConfig) -> Result<Self> {
        let url = Url::parse(&config.endpoint).context("Invalid Elasticsearch endpoint URL")?;
        let pool = SingleNodeConnectionPool::new(url);
        let mut builder = TransportBuilder::new(pool);

        if !config.ssl_verify {
            builder = builder.cert_validation(CertificateValidation::None);
        }

        if !config.headers.is_empty() {
            let mut header_map = HeaderMap::new();
            for (k, v) in config.headers.iter() {
                let name = HeaderName::from_bytes(k.as_bytes())
                    .with_context(|| format!("Invalid header name: {}", k))?;
                let value = HeaderValue::from_str(v)
                    .with_context(|| format!("Invalid header value for {}: {}", k, v))?;
                header_map.insert(name, value);
            }
            builder = builder.headers(header_map);
        }

        if let Some(api_key) = &config.api_key {
            builder = builder.auth(Credentials::EncodedApiKey(api_key.clone()));
        } else if let (Some(user), Some(password)) = (&config.user, &config.password) {
            builder = builder.auth(Credentials::Basic(user.clone(), password.clone()));
        }

        let transport = builder.build()?;
        let client = Elasticsearch::new(transport);

        Ok(Self { client })
    }

    async fn index_exists(&self, name: &str) -> Result<bool> {
        let response = self
            .client
            .indices()
            .exists(IndicesExistsParts::Index(&[name]))
            .send()
            .await?;
        Ok(response.status_code().is_success())
    }

    async fn create_index(&self, name: &str, mapping: &Value) -> Result<()> {
        let response = self
            .client
            .indices()
            .create(IndicesCreateParts::Index(name))
            .body(mapping.clone())
            .send()
            .await?;

        let status = response.status_code();
        if status.is_success() {
            info!("Index '{}' created", name);
        } else if status.as_u16() == 409 {
            warn!("Index '{}' already exists (conflict)", name);
        } else {
            let text = response.text().await?;
            anyhow::bail!("Index creation failed ({}): {}", status, text);
        }
        Ok(())
    }

    async fn delete_index(&self, name: &str) -> Result<bool> {
        let response = self
            .client
            .indices()
            .delete(IndicesDeleteParts::Index(&[name]))
            .send()
            .await?;

        let status = response.status_code();
        if status.is_success() {
            Ok(true)
        } else if status.as_u16() == 404 {
            Ok(false)
        } else {
            let text = response.text().await?;
            anyhow::bail!("Index deletion failed ({}): {}", status, text);
        }
    }

    async fn bulk(&self, lines: &[String], refresh: bool) -> Result<Value> {
        let refresh_val = if refresh { Refresh::True } else { Refresh::False };

        let response = self
            .client
            .bulk(BulkParts::None)
            .body(lines.to_vec())
            .refresh(refresh_val)
            .send()
            .await?;

        if !response.status_code().is_success() {
            let text = response.text().await?;
            anyhow::bail!("Bulk request failed: {}", text);
        }

        let result: Value = response.json().await?;
        Ok(result)
    }

    async fn cluster_health(&self) -> Result<Value> {
        let response = self
            .client
            .cluster()
            .health(ClusterHealthParts::None)
            .send()
            .await?;

        if !response.status_code().is_success() {
            let text = response.text().await?;
            anyhow::bail!("Cluster health request failed: {}", text);
        }

        let result: Value = response.json().await?;
        Ok(result)
    }

    async fn list_indices(&self, pattern: &str) -> Result<Vec<String>> {
        let response = self
            .client
            .cat()
            .indices(CatIndicesParts::Index(&[pattern]))
            .format("json")
            .send()
            .await?;

        if !response.status_code().is_success() {
            let text = response.text().await?;
            anyhow::bail!("Failed to list indices: {}", text);
        }

        let parsed: Value = response.json().await?;
        let indices = parsed
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|item| item.get("index").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();
        Ok(indices)
    }

    async fn delete_indices_by_pattern(&self, pattern: &str) -> Result<Vec<String>> {
        let indices = self.list_indices(pattern).await?;
        let mut deleted = Vec::new();

        for index in indices {
            if self.delete_index(&index).await.unwrap_or(false) {
                deleted.push(index);
            }
        }

        Ok(deleted)
    }
}

struct AirportLookup {
    airports: HashMap<String, (f64, f64)>,
}

impl AirportLookup {
    fn new(airports_file: &Path) -> Result<Self> {
        let mut airports = HashMap::new();

        if !airports_file.exists() {
            warn!("Airports file not found: {:?}", airports_file);
            return Ok(Self { airports });
        }

        info!("Loading airports from {:?}", airports_file);
        let mut count = 0;

        let file = File::open(airports_file)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(reader);

        for result in csv_reader.records() {
            let record = result?;
            if record.len() < 8 {
                continue;
            }

            let iata = record.get(4).unwrap_or("").trim();
            if iata.is_empty() || iata == "\\N" {
                continue;
            }

            let lat_str = record.get(6).unwrap_or("").trim();
            let lon_str = record.get(7).unwrap_or("").trim();
            if lat_str.is_empty() || lon_str.is_empty() {
                continue;
            }

            if let (Ok(lat), Ok(lon)) = (lat_str.parse::<f64>(), lon_str.parse::<f64>()) {
                airports.insert(iata.to_uppercase(), (lat, lon));
                count += 1;
            }
        }

        info!("Loaded {} airports into lookup table", count);
        Ok(Self { airports })
    }

    fn lookup_coordinates(&self, iata_code: Option<&str>) -> Option<String> {
        let code = iata_code?.to_uppercase();
        self.airports
            .get(&code)
            .map(|(lat, lon)| format!("{},{}", lat, lon))
    }
}

struct CancellationLookup {
    cancellations: HashMap<String, String>,
}

impl CancellationLookup {
    fn new(cancellations_file: &Path) -> Result<Self> {
        let mut cancellations = HashMap::new();

        if !cancellations_file.exists() {
            warn!("Cancellations file not found: {:?}", cancellations_file);
            return Ok(Self { cancellations });
        }

        info!("Loading cancellations from {:?}", cancellations_file);
        let mut count = 0;

        let file = File::open(cancellations_file)?;
        let reader = BufReader::new(file);
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);

        let headers = csv_reader.headers()?.clone();
        let code_idx = headers.iter().position(|h| h == "Code").unwrap_or(0);
        let desc_idx = headers.iter().position(|h| h == "Description").unwrap_or(1);

        for result in csv_reader.records() {
            let record = result?;
            let code = record.get(code_idx).unwrap_or("").trim();
            let description = record.get(desc_idx).unwrap_or("").trim();

            if !code.is_empty() && !description.is_empty() {
                cancellations.insert(code.to_uppercase(), description.to_string());
                count += 1;
            }
        }

        info!("Loaded {} cancellation reasons into lookup table", count);
        Ok(Self { cancellations })
    }

    fn lookup_reason(&self, code: Option<&str>) -> Option<&String> {
        let code = code?;
        let upper = code.to_uppercase();
        self.cancellations.get(upper.as_str())
    }
}

struct FlightLoader {
    client: ElasticsearchClient,
    mapping: Value,
    index_prefix: String,
    batch_size: usize,
    refresh: bool,
    airport_lookup: AirportLookup,
    cancellation_lookup: CancellationLookup,
    ensured_indices: HashSet<String>,
    loaded_records: usize,
    total_records: usize,
}

impl FlightLoader {
    fn new(
        client: ElasticsearchClient,
        mapping: Value,
        index: String,
        batch_size: usize,
        refresh: bool,
        airport_lookup: AirportLookup,
        cancellation_lookup: CancellationLookup,
    ) -> Self {
        Self {
            client,
            mapping,
            index_prefix: index,
            batch_size,
            refresh,
            airport_lookup,
            cancellation_lookup,
            ensured_indices: HashSet::new(),
            loaded_records: 0,
            total_records: 0,
        }
    }

    async fn ensure_index(&mut self, index_name: &str) -> Result<()> {
        if self.ensured_indices.contains(index_name) {
            debug!("Index {} already ensured in this session", index_name);
            return Ok(());
        }

        // Delete index if it exists before creating a new one
        if self.client.index_exists(index_name).await? {
            info!("Deleting existing index '{}' before import", index_name);
            match self.client.delete_index(index_name).await {
                Ok(true) => info!("Index '{}' deleted", index_name),
                Ok(false) => warn!("Failed to delete index '{}'", index_name),
                Err(e) => warn!("Error deleting index '{}': {}", index_name, e),
            }
        }

        info!("Creating index: {}", index_name);
        self.client.create_index(index_name, &self.mapping).await?;
        self.ensured_indices.insert(index_name.to_string());
        Ok(())
    }

    fn extract_year_month_from_filename(
        &self,
        file_path: &Path,
    ) -> (Option<String>, Option<String>) {
        let mut basename = file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        // Strip compound extensions like .csv.gz or .csv.zip
        loop {
            if basename.ends_with(".csv") || basename.ends_with(".gz") || basename.ends_with(".zip")
            {
                if let Some(stem) = Path::new(&basename).file_stem().and_then(|s| s.to_str()) {
                    basename = stem.to_string();
                    continue;
                }
            }
            break;
        }

        // flights-YYYY-MM
        if let Some((prefix, month_part)) = basename.rsplit_once('-') {
            if month_part.len() == 2 && month_part.chars().all(|c| c.is_ascii_digit()) {
                if let Some((_, year_part)) = prefix.rsplit_once('-') {
                    if year_part.len() == 4 && year_part.chars().all(|c| c.is_ascii_digit()) {
                        return (Some(year_part.to_string()), Some(month_part.to_string()));
                    }
                }
            }
        }

        // flights-YYYY
        if let Some((_, year_part)) = basename.rsplit_once('-') {
            if year_part.len() == 4 && year_part.chars().all(|c| c.is_ascii_digit()) {
                return (Some(year_part.to_string()), None);
            }
        }

        (None, None)
    }

    fn extract_index_name(
        &self,
        timestamp: Option<&str>,
        file_year: Option<&str>,
        file_month: Option<&str>,
    ) -> Option<String> {
        if let Some(year) = file_year {
            if let Some(month) = file_month {
                return Some(format!("{}-{}-{}", self.index_prefix, year, month));
            }
            return Some(format!("{}-{}", self.index_prefix, year));
        }

        let ts = timestamp?;
        if ts.len() >= 4 {
            let year_part = &ts[0..4];
            if year_part.chars().all(|c| c.is_ascii_digit()) {
                return Some(format!("{}-{}", self.index_prefix, year_part));
            }
        }

        None
    }

    fn format_number(&self, number: usize) -> String {
        number.to_string()
            .chars()
            .rev()
            .collect::<Vec<_>>()
            .chunks(3)
            .map(|chunk| chunk.iter().collect::<String>())
            .collect::<Vec<_>>()
            .join(",")
            .chars()
            .rev()
            .collect()
    }

    fn record_to_map(
        &self,
        headers: &csv::StringRecord,
        record: &csv::StringRecord,
    ) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for (i, field) in record.iter().enumerate() {
            if let Some(header) = headers.get(i) {
                map.insert(header.to_string(), field.to_string());
            }
        }
        map
    }

    fn count_lines_fast(&self, file_path: &Path) -> usize {
        match self.count_lines(file_path) {
            Ok(n) => n,
            Err(e) => {
                warn!(
                    "Failed to count lines in {:?}: {}",
                    file_path.file_name().unwrap_or_default(),
                    e
                );
                0
            }
        }
    }

    fn count_lines(&self, file_path: &Path) -> Result<usize> {
        let ext = file_path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        if ext == "zip" {
            self.count_lines_in_zip(file_path)
        } else if file_path
            .to_string_lossy()
            .to_lowercase()
            .ends_with(".gz")
        {
            self.count_lines_in_gzip(file_path)
        } else {
            self.count_lines_plain(file_path)
        }
    }

    fn count_lines_plain(&self, file_path: &Path) -> Result<usize> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut count = 0;
        for line in reader.lines() {
            if line.is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }

    fn count_lines_in_gzip(&self, file_path: &Path) -> Result<usize> {
        let file = File::open(file_path)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        let mut count = 0;
        for line in reader.lines() {
            if line.is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }

    fn first_csv_entry_index(&self, archive: &mut ZipArchive<File>) -> Result<usize> {
        for i in 0..archive.len() {
            let name = archive.by_index(i)?.name().to_lowercase();
            if name.ends_with(".csv") {
                return Ok(i);
            }
        }
        anyhow::bail!("No CSV entry found in archive");
    }

    fn count_lines_in_zip(&self, file_path: &Path) -> Result<usize> {
        let file = File::open(file_path)?;
        let mut archive = ZipArchive::new(file)?;
        let entry_index = self.first_csv_entry_index(&mut archive)?;
        let mut entry = archive.by_index(entry_index)?;
        let reader = BufReader::new(&mut entry);

        let mut count = 0;
        for line in reader.lines() {
            if line.is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    fn count_total_records_fast(&self, files: &[PathBuf]) -> usize {
        let mut total = 0;
        for file_path in files {
            if file_path.is_file() {
                let line_count = self.count_lines_fast(file_path);
                total += line_count.saturating_sub(1); // Subtract 1 for CSV header
            }
        }
        total
    }

    fn transform_row(&self, row: &HashMap<String, String>) -> Value {
        let present = |key: &str| -> Option<String> {
            row.get(key)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        };

        let to_integer = |key: &str| -> Option<i64> {
            present(key)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.round() as i64)
        };

        let to_boolean = |key: &str| -> Option<bool> {
            present(key).and_then(|s| {
                let lower = s.to_lowercase();
                match lower.as_str() {
                    "true" | "t" | "yes" | "y" => Some(true),
                    "false" | "f" | "no" | "n" => Some(false),
                    _ => s.parse::<f64>().ok().map(|n| n > 0.0),
                }
            })
        };

        let mut doc = json!({});

        let timestamp = present("@timestamp").or_else(|| present("FlightDate"));
        doc["@timestamp"] = json!(timestamp);

        let reporting_airline = present("Reporting_Airline");
        let flight_number = present("Flight_Number_Reporting_Airline");
        let origin = present("Origin");
        let dest = present("Dest");

        if let (Some(flight_date), Some(airline), Some(number), Some(orig), Some(dst)) = (
            timestamp.clone(),
            reporting_airline.clone(),
            flight_number.clone(),
            origin.clone(),
            dest.clone(),
        ) {
            doc["FlightID"] = json!(format!(
                "{}_{}_{}_{}_{}",
                flight_date, airline, number, orig, dst
            ));
        }

        doc["Reporting_Airline"] = json!(reporting_airline);
        doc["Tail_Number"] = json!(present("Tail_Number"));
        doc["Flight_Number"] = json!(flight_number);
        doc["Origin"] = json!(origin);
        doc["Dest"] = json!(dest);

        doc["CRSDepTimeLocal"] = json!(to_integer("CRSDepTime"));
        doc["DepDelayMin"] = json!(to_integer("DepDelay"));
        doc["TaxiOutMin"] = json!(to_integer("TaxiOut"));
        doc["TaxiInMin"] = json!(to_integer("TaxiIn"));
        doc["CRSArrTimeLocal"] = json!(to_integer("CRSArrTime"));
        doc["ArrDelayMin"] = json!(to_integer("ArrDelay"));

        doc["Cancelled"] = json!(to_boolean("Cancelled"));
        doc["Diverted"] = json!(to_boolean("Diverted"));

        let cancellation_code = present("CancellationCode");
        doc["CancellationCode"] = json!(cancellation_code);

        if let Some(reason) = self
            .cancellation_lookup
            .lookup_reason(cancellation_code.as_deref())
        {
            doc["CancellationReason"] = json!(reason);
        }

        doc["ActualElapsedTimeMin"] = json!(to_integer("ActualElapsedTime"));
        doc["AirTimeMin"] = json!(to_integer("AirTime"));

        doc["Flights"] = json!(to_integer("Flights"));
        doc["DistanceMiles"] = json!(to_integer("Distance"));

        doc["CarrierDelayMin"] = json!(to_integer("CarrierDelay"));
        doc["WeatherDelayMin"] = json!(to_integer("WeatherDelay"));
        doc["NASDelayMin"] = json!(to_integer("NASDelay"));
        doc["SecurityDelayMin"] = json!(to_integer("SecurityDelay"));
        doc["LateAircraftDelayMin"] = json!(to_integer("LateAircraftDelay"));

        if let Some(loc) = origin
            .as_deref()
            .and_then(|orig| self.airport_lookup.lookup_coordinates(Some(orig)))
        {
            doc["OriginLocation"] = json!(loc);
        }

        if let Some(loc) = dest
            .as_deref()
            .and_then(|dst| self.airport_lookup.lookup_coordinates(Some(dst)))
        {
            doc["DestLocation"] = json!(loc);
        }

        doc
    }

    fn compact_document(&self, doc: Value) -> Value {
        let mut compacted = serde_json::Map::new();

        if let Some(map) = doc.as_object() {
            for (key, value) in map {
                if !value.is_null() {
                    compacted.insert(key.clone(), value.clone());
                }
            }
        }

        Value::Object(compacted)
    }

    async fn flush(&mut self, lines: &[String], index_name: &str) -> Result<usize> {
        let result = self
            .client
            .bulk(lines, self.refresh)
            .await?;

        if let Some(errors) = result.get("errors").and_then(|v| v.as_bool()) {
            if errors {
                let empty: Vec<Value> = Vec::new();
                let items = result
                    .get("items")
                    .and_then(|v| v.as_array())
                    .unwrap_or(&empty);
                let error_items: Vec<_> = items
                    .iter()
                    .filter_map(|item| {
                        item.get("index")
                            .and_then(|idx| idx.get("error"))
                            .map(|e| e.to_string())
                    })
                    .take(5)
                    .collect();

                for error in &error_items {
                    warn!("Bulk item error for {}: {}", index_name, error);
                }
                anyhow::bail!("Bulk indexing reported errors for {}; aborting", index_name);
            }
        }

        let doc_count = lines.len() / 2;
        self.loaded_records += doc_count;

        if self.total_records > 0 {
            let percentage = (self.loaded_records as f64 / self.total_records as f64 * 100.0 * 10.0)
                .round()
                / 10.0;
            print!(
                "\r{} of {} records loaded ({:.1}%)",
                self.format_number(self.loaded_records),
                self.format_number(self.total_records),
                percentage
            );
        } else {
            print!("\r{} records loaded", self.format_number(self.loaded_records));
        }
        std::io::stdout().flush().ok();

        Ok(doc_count)
    }

    async fn process_reader<R: Read>(
        &mut self,
        reader: R,
        file_year: Option<&str>,
        file_month: Option<&str>,
    ) -> Result<(usize, usize)> {
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);

        let headers = csv_reader.headers()?.clone();
        let mut record = csv::StringRecord::new();
        let mut processed_rows = 0;
        let mut indexed_docs = 0;
        let mut index_buffers: HashMap<String, (Vec<String>, usize)> = HashMap::new();

        while csv_reader.read_record(&mut record)? {
            processed_rows += 1;

            if processed_rows == 1 {
                let has_timestamp = headers.iter().any(|h| h == "@timestamp");
                let has_flight_date = headers.iter().any(|h| h == "FlightDate");
                if !has_timestamp && !has_flight_date {
                    warn!(
                        "CSV headers don't include '@timestamp' or 'FlightDate'. Available headers: {}",
                        headers.iter().take(10).collect::<Vec<_>>().join(", ")
                    );
                }
            }

            let row_map = self.record_to_map(&headers, &record);
            let mut doc = self.transform_row(&row_map);

            let timestamp_value = doc
                .get("@timestamp")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let index_name = self.extract_index_name(
                timestamp_value.as_deref(),
                file_year,
                file_month,
            );

            if index_name.is_none() {
                let timestamp_raw = row_map
                    .get("@timestamp")
                    .or_else(|| row_map.get("FlightDate"))
                    .cloned()
                    .unwrap_or_default();
                warn!(
                    "Skipping document - missing or invalid timestamp. Raw value: {:?}. Row {}: Origin={:?}, Dest={:?}, Airline={:?}",
                    timestamp_raw,
                    processed_rows,
                    row_map.get("Origin"),
                    row_map.get("Dest"),
                    row_map.get("Reporting_Airline")
                );
                continue;
            }
            let index_name = index_name.unwrap();

            doc = self.compact_document(doc);
            if doc
                .as_object()
                .map(|map| map.is_empty())
                .unwrap_or(true)
            {
                continue;
            }

            self.ensure_index(&index_name).await?;

            let buffer = index_buffers
                .entry(index_name.clone())
                .or_insert_with(|| (Vec::new(), 0));

            buffer
                .0
                .push(serde_json::to_string(&json!({"index": {"_index": index_name}}))?);
            buffer.0.push(serde_json::to_string(&doc)?);
            buffer.1 += 1;

            if buffer.1 >= self.batch_size {
                let docs_in_batch = self.flush(&buffer.0, &index_name).await?;
                indexed_docs += docs_in_batch;
                buffer.0.clear();
                buffer.1 = 0;
            }
        }

        for (index_name, (lines, count)) in index_buffers.iter_mut() {
            if *count > 0 {
                let docs_in_batch = self.flush(lines, index_name).await?;
                indexed_docs += docs_in_batch;
                lines.clear();
                *count = 0;
            }
        }

        Ok((processed_rows, indexed_docs))
    }

    async fn import_file(&mut self, file_path: &Path) -> Result<()> {
        if !file_path.is_file() {
            warn!("Skipping {:?} (not a regular file)", file_path);
            return Ok(());
        }

        let (file_year, file_month) = self.extract_year_month_from_filename(file_path);
        info!("Importing {:?}", file_path);

        let results = if file_path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .eq_ignore_ascii_case("zip")
        {
            let file = File::open(file_path)?;
            let mut archive = ZipArchive::new(file)?;
            let entry_index = self.first_csv_entry_index(&mut archive)?;
            let entry = archive.by_index(entry_index)?;
            self.process_reader(
                entry,
                file_year.as_deref(),
                file_month.as_deref(),
            )
            .await?
        } else if file_path
            .to_string_lossy()
            .to_lowercase()
            .ends_with(".gz")
        {
            let file = File::open(file_path)?;
            let decoder = GzDecoder::new(file);
            self.process_reader(
                decoder,
                file_year.as_deref(),
                file_month.as_deref(),
            )
            .await?
        } else {
            let file = File::open(file_path)?;
            self.process_reader(
                file,
                file_year.as_deref(),
                file_month.as_deref(),
            )
            .await?
        };

        let (processed_rows, indexed_docs) = results;
        info!(
            "Finished {:?} (rows processed: {}, documents indexed: {})",
            file_path, processed_rows, indexed_docs
        );

        Ok(())
    }

    fn sample_from_reader<R: Read>(
        &self,
        reader: R,
        file_year: Option<&str>,
        file_month: Option<&str>,
    ) -> Result<Option<Value>> {
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);

        let headers = csv_reader.headers()?.clone();
        let mut record = csv::StringRecord::new();

        if csv_reader.read_record(&mut record)? {
            let row_map = self.record_to_map(&headers, &record);
            let doc = self.transform_row(&row_map);
            let compacted = self.compact_document(doc);

            // Include derived index name for debugging if we can determine it
            let timestamp_value = compacted
                .get("@timestamp")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let index_name =
                self.extract_index_name(timestamp_value.as_deref(), file_year, file_month);

            let mut output = compacted;
            if let Some(index_name) = index_name {
                output["__index"] = json!(index_name);
            }
            return Ok(Some(output));
        }

        Ok(None)
    }

    fn sample_document(&self, file_path: &Path) -> Result<Option<Value>> {
        if !file_path.is_file() {
            warn!("Skipping {:?} (not a regular file)", file_path);
            return Ok(None);
        }

        let (file_year, file_month) = self.extract_year_month_from_filename(file_path);

        if file_path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .eq_ignore_ascii_case("zip")
        {
            let file = File::open(file_path)?;
            let mut archive = ZipArchive::new(file)?;
            let entry_index = self.first_csv_entry_index(&mut archive)?;
            let entry = archive.by_index(entry_index)?;
            return self.sample_from_reader(entry, file_year.as_deref(), file_month.as_deref());
        } else if file_path
            .to_string_lossy()
            .to_lowercase()
            .ends_with(".gz")
        {
            let file = File::open(file_path)?;
            let decoder = GzDecoder::new(file);
            return self.sample_from_reader(decoder, file_year.as_deref(), file_month.as_deref());
        }

        let file = File::open(file_path)?;
        self.sample_from_reader(file, file_year.as_deref(), file_month.as_deref())
    }

    async fn import_files(&mut self, files: &[PathBuf]) -> Result<()> {
        info!("Counting records in {} file(s)...", files.len());
        self.total_records = self.count_total_records_fast(files);
        info!(
            "Total records to import: {}",
            self.format_number(self.total_records)
        );
        info!("Importing {} file(s)...", files.len());

        for file_path in files {
            self.import_file(file_path).await?;
        }

        println!();
        info!(
            "Import complete: {} of {} records loaded",
            self.format_number(self.loaded_records),
            self.format_number(self.total_records)
        );

        Ok(())
    }
}

fn load_yaml(path: &Path) -> Result<Value> {
    let resolved = resolve_with_project_fallback(path)
        .with_context(|| format!("Config file not found: {:?}", path))?;
    let content = std::fs::read_to_string(&resolved)
        .with_context(|| format!("Config file not found: {:?}", resolved))?;
    serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse YAML: {:?}", resolved))
}

fn load_json(path: &Path) -> Result<Value> {
    let resolved = resolve_with_project_fallback(path)
        .with_context(|| format!("Mapping file not found: {:?}", path))?;
    let content = std::fs::read_to_string(&resolved)
        .with_context(|| format!("Mapping file not found: {:?}", resolved))?;
    serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse JSON: {:?}", resolved))
}

fn resolve_with_project_fallback(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() && path.exists() {
        return Ok(path.to_path_buf());
    }

    if path.exists() {
        return Ok(path.canonicalize()?);
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        manifest_dir.join(path),
        manifest_dir.join("..").join(path),
    ];
    for candidate in candidates.iter() {
        if candidate.exists() {
            return Ok(candidate.canonicalize()?);
        }
    }

    anyhow::bail!("Path not found: {:?}", path);
}

fn resolve_file_path(path: &Path, data_dir: &Path) -> Result<PathBuf> {
    // If path is absolute and exists, use it
    if path.is_absolute() && path.exists() {
        return Ok(path.to_path_buf());
    }

    // If path exists relative to current directory, use it
    if path.exists() {
        return Ok(path.canonicalize()?);
    }

    // Try relative to data_dir
    let candidate = data_dir.join(path);
    if candidate.exists() {
        return Ok(candidate.canonicalize()?);
    }

    // Try relative to project root or crate dir
    if let Some(resolved) = resolve_with_project_fallback(path).ok() {
        if resolved.exists() {
            return Ok(resolved);
        }
    }

    anyhow::bail!("File not found: {:?}", path);
}

fn files_to_process(args: &Args) -> Result<Vec<PathBuf>> {
    if let Some(file) = &args.file {
        return Ok(vec![resolve_file_path(file, &args.data_dir)?]);
    }

    if args.all {
        let mut files = Vec::new();
        for pattern in &["*.zip", "*.csv", "*.csv.gz"] {
            let full_pattern = if args.data_dir.is_absolute() {
                format!("{}/{}", args.data_dir.display(), pattern)
            } else {
                format!("{}/{}", args.data_dir.display(), pattern)
            };
            for entry in glob::glob(&full_pattern)? {
                if let Ok(path) = entry {
                    if path.is_file() {
                        files.push(path);
                    }
                }
            }
        }
        files.sort();
        if files.is_empty() {
            anyhow::bail!(
                "No .zip, .csv, or .csv.gz files found in {:?}",
                args.data_dir
            );
        }
        return Ok(files);
    }

    if let Some(glob_pattern) = &args.glob {
        let mut files = Vec::new();
        let pattern = if Path::new(glob_pattern).is_absolute() {
            glob_pattern.clone()
        } else {
            // Try as-is first
            let mut found = false;
            for entry in glob::glob(glob_pattern)? {
                if let Ok(path) = entry {
                    if path.is_file() {
                        files.push(path);
                        found = true;
                    }
                }
            }
            if !found {
                // Try relative to data_dir
                format!("{}/{}", args.data_dir.display(), glob_pattern)
            } else {
                glob_pattern.clone()
            }
        };

        if files.is_empty() {
            for entry in glob::glob(&pattern)? {
                if let Ok(path) = entry {
                    if path.is_file() {
                        files.push(path);
                    }
                }
            }
        }

        files.sort();
        if files.is_empty() {
            anyhow::bail!("No files found matching pattern: {}", glob_pattern);
        }
        return Ok(files);
    }

    anyhow::bail!("Please provide either --file PATH, --all, or --glob PATTERN");
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    let args = Args::parse();

    if args.status && (args.delete_index || args.delete_all) {
        anyhow::bail!("Cannot combine --status with delete operations");
    }

    let config_data = load_yaml(&args.config)?;
    let es_config = ElasticsearchConfig::from_yaml(&config_data)?;
    let client = ElasticsearchClient::new(es_config)?;

    if args.status {
        let health = client.cluster_health().await?;
        info!(
            "Cluster status: {}",
            health.get("status").unwrap_or(&json!(null))
        );
        info!(
            "Active shards: {}, node count: {}",
            health.get("active_shards").unwrap_or(&json!(null)),
            health.get("number_of_nodes").unwrap_or(&json!(null))
        );
        return Ok(());
    }

    if args.delete_index {
        let pattern = if args.index.ends_with('*') {
            args.index.clone()
        } else {
            format!("{}-*", args.index)
        };

        let deleted = client.delete_indices_by_pattern(&pattern).await?;
        if deleted.is_empty() {
            warn!("No indices found matching pattern: {}", pattern);
        } else {
            info!(
                "Deleted {} index(es): {}",
                deleted.len(),
                deleted.join(", ")
            );
        }
        return Ok(());
    }

    if args.delete_all {
        let deleted = client.delete_indices_by_pattern("flights-*").await?;
        if deleted.is_empty() {
            warn!("No indices found matching pattern: flights-*");
        } else {
            info!(
                "Deleted {} index(es): {}",
                deleted.len(),
                deleted.join(", ")
            );
        }
        return Ok(());
    }

    let mapping = load_json(&args.mapping)?;
    let airports_path = resolve_with_project_fallback(&args.airports_file)?;
    let cancellations_path = resolve_with_project_fallback(&args.cancellations_file)?;
    let airport_lookup = AirportLookup::new(&airports_path)?;
    let cancellation_lookup = CancellationLookup::new(&cancellations_path)?;

    let mut loader = FlightLoader::new(
        client,
        mapping,
        args.index.clone(),
        args.batch_size,
        args.refresh,
        airport_lookup,
        cancellation_lookup,
    );

    let files = files_to_process(&args)?;
    if args.sample {
        let file = files
            .first()
            .ok_or_else(|| anyhow::anyhow!("No files found to sample"))?;

        if let Some(doc) = loader.sample_document(file)? {
            println!("{}", serde_json::to_string_pretty(&doc)?);
        } else {
            anyhow::bail!("No document found in file");
        }
        return Ok(());
    }

    loader.import_files(&files).await?;

    let duration = start_time.elapsed();
    let minutes = duration.as_secs() / 60;
    let seconds = duration.as_secs_f64() % 60.0;
    if minutes > 0 {
        println!("\nTotal time: {}m {:.2}s", minutes, seconds);
    } else {
        println!("\nTotal time: {:.2}s", seconds);
    }

    Ok(())
}
