use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use clap::Parser;
use elasticsearch::{
    auth::Credentials,
    cert::CertificateValidation,
    cluster::ClusterHealthParts,
    http::{
        headers::{HeaderMap, HeaderName, HeaderValue},
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts},
    ingest::IngestPutPipelineParts,
    params::Refresh,
    Elasticsearch,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

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

macro_rules! error {
    ($($arg:tt)*) => {
        eprintln!("[ERROR] {}", format!($($arg)*))
    };
}

const ES_INDEX: &str = "contracts";
const PIPELINE_NAME: &str = "pdf_pipeline";
const DEFAULT_INFERENCE_ENDPOINT: &str = ".elser-2-elastic";

#[derive(Parser)]
#[command(name = "import_contracts")]
#[command(about = "Import PDF contracts into Elasticsearch")]
struct Args {
    #[arg(short = 'c', long, default_value = "config/elasticsearch.yml")]
    config: PathBuf,

    #[arg(short = 'm', long, default_value = "config/mappings-contracts.json")]
    mapping: PathBuf,

    #[arg(long)]
    pdf_path: Option<PathBuf>,

    #[arg(long)]
    setup_only: bool,

    #[arg(long)]
    ingest_only: bool,

    #[arg(long)]
    inference_endpoint: Option<String>,

    #[arg(long)]
    status: bool,
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
    endpoint: String,
    config: ElasticsearchConfig,
}

impl ElasticsearchClient {
    fn new(config: ElasticsearchConfig) -> Result<Self> {
        let url = Url::parse(&config.endpoint).context("Invalid Elasticsearch endpoint URL")?;
        let pool = SingleNodeConnectionPool::new(url.clone());
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

        Ok(Self {
            client,
            endpoint: config.endpoint.clone(),
            config,
        })
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

    async fn create_pipeline(&self, name: &str, pipeline_config: &Value) -> Result<()> {
        let response = self
            .client
            .ingest()
            .put_pipeline(IngestPutPipelineParts::Id(name))
            .body(pipeline_config.clone())
            .send()
            .await?;

        if !response.status_code().is_success() {
            let text = response.text().await?;
            anyhow::bail!("Pipeline creation failed: {}", text);
        }

        info!("Pipeline '{}' created/updated", name);
        Ok(())
    }

    async fn index_document(
        &self,
        index_name: &str,
        document: &Value,
        pipeline: Option<&str>,
    ) -> Result<()> {
        use elasticsearch::IndexParts;
        
        let mut request = self
            .client
            .index(IndexParts::Index(index_name))
            .body(document.clone())
            .refresh(Refresh::WaitFor); // Wait for refresh to ensure document is searchable

        if let Some(pipeline) = pipeline {
            request = request.pipeline(pipeline);
        }

        let response = request.send().await;

        match response {
            Ok(resp) => {
                let status = resp.status_code();
                
                if !status.is_success() {
                    // For error status, try to get error message
                    match resp.text().await {
                        Ok(text) => anyhow::bail!("Document indexing failed ({}): {}", status, text),
                        Err(_) => anyhow::bail!("Document indexing failed with status {}", status),
                    }
                }
                
                // For success status, try to parse JSON to check for errors in response body
                // (some pipeline errors might still return 200 OK)
                match resp.json::<Value>().await {
                    Ok(json) => {
                        if let Some(error) = json.get("error") {
                            warn!("Elasticsearch returned error in response: {}", error);
                            anyhow::bail!("Document indexing failed: {}", error);
                        }
                        Ok(())
                    }
                    Err(_) => {
                        // If we can't parse JSON but status was OK, assume success
                        Ok(())
                    }
                }
            }
            Err(e) => {
                // Log the full error for debugging
                error!("Elasticsearch error during indexing: {:?}", e);
                Err(e.into())
            }
        }
    }

    async fn get_inference_endpoints(&self) -> Result<Value> {
        // Use low-level HTTP request since inference endpoints API may not be in typed client
        let url = format!("{}/_inference/_all", self.endpoint.trim_end_matches('/'));
        
        let mut client_builder = reqwest::Client::builder();
        
        // Handle SSL verification
        if !self.config.ssl_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }
        
        let mut request_builder = client_builder
            .build()
            .context("Failed to build HTTP client")?
            .get(&url);
        
        // Add authentication
        if let Some(api_key) = &self.config.api_key {
            request_builder = request_builder.header("Authorization", format!("ApiKey {}", api_key));
        } else if let (Some(user), Some(password)) = (&self.config.user, &self.config.password) {
            let credentials = general_purpose::STANDARD.encode(format!("{}:{}", user, password));
            request_builder = request_builder.header("Authorization", format!("Basic {}", credentials));
        }
        
        // Add custom headers
        for (key, value) in &self.config.headers {
            request_builder = request_builder.header(key, value);
        }
        
        match request_builder.send().await {
            Ok(response) => {
                if !response.status().is_success() {
                    warn!("Failed to get inference endpoints: HTTP {}", response.status());
                    return Ok(json!({"endpoints": []}));
                }
                match response.json().await {
                    Ok(result) => Ok(result),
                    Err(_) => Ok(json!({"endpoints": []})),
                }
            }
            Err(e) => {
                warn!("Failed to get inference endpoints: {}", e);
                Ok(json!({"endpoints": []}))
            }
        }
    }

    async fn count_documents(&self, index_name: &str) -> Result<u64> {
        let response = self
            .client
            .count(elasticsearch::CountParts::Index(&[index_name]))
            .send()
            .await?;

        if !response.status_code().is_success() {
            warn!("Failed to count documents: HTTP {}", response.status_code());
            return Ok(0);
        }

        let result: Value = response.json().await?;
        let count = result
            .get("count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(count)
    }
}

struct ContractLoader {
    client: ElasticsearchClient,
    mapping: Value,
    inference_endpoint: String,
    indexed_count: usize,
}

impl ContractLoader {
    fn new(
        client: ElasticsearchClient,
        mapping: Value,
        inference_endpoint: Option<String>,
    ) -> Self {
        Self {
            client,
            mapping,
            inference_endpoint: inference_endpoint
                .unwrap_or_else(|| DEFAULT_INFERENCE_ENDPOINT.to_string()),
            indexed_count: 0,
        }
    }

    async fn check_elasticsearch(&self) -> Result<bool> {
        match self.client.cluster_health().await {
            Ok(health) => {
                let cluster_name = health
                    .get("cluster_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let status = health.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");
                info!("Cluster: {}", cluster_name);
                info!("Status: {}", status);
                Ok(true)
            }
            Err(e) => {
                error!("Connection error: {}", e);
                Ok(false)
            }
        }
    }

    async fn check_inference_endpoint(&mut self) -> Result<bool> {
        match self.client.get_inference_endpoints().await {
            Ok(response) => {
                let endpoints = response
                    .get("endpoints")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                // First, try to find the specified endpoint
                if endpoints.iter().any(|ep| {
                    ep.get("inference_id")
                        .and_then(|v| v.as_str())
                        .map(|id| id == self.inference_endpoint)
                        .unwrap_or(false)
                }) {
                    info!("Found inference endpoint: {}", self.inference_endpoint);
                    return Ok(true);
                }

                // Auto-detect ELSER endpoints
                let elser_endpoints: Vec<_> = endpoints
                    .iter()
                    .filter(|ep| {
                        ep.get("inference_id")
                            .and_then(|v| v.as_str())
                            .map(|id| id.to_lowercase().contains("elser"))
                            .unwrap_or(false)
                    })
                    .collect();

                if !elser_endpoints.is_empty() {
                    // Prefer endpoints starting with .elser-2- or .elser_model_2
                    let preferred: Vec<_> = elser_endpoints
                        .iter()
                        .filter(|ep| {
                            ep.get("inference_id")
                                .and_then(|v| v.as_str())
                                .map(|id| {
                                    id.contains(".elser-2-") || id.contains(".elser_model_2")
                                })
                                .unwrap_or(false)
                        })
                        .collect();

                    if let Some(pref) = preferred.first() {
                        if let Some(id) = pref.get("inference_id").and_then(|v| v.as_str()) {
                            self.inference_endpoint = id.to_string();
                        }
                    } else if let Some(first) = elser_endpoints.first() {
                        if let Some(id) = first.get("inference_id").and_then(|v| v.as_str()) {
                            self.inference_endpoint = id.to_string();
                        }
                    }

                    warn!(
                        "Specified endpoint not found, using auto-detected: {}",
                        self.inference_endpoint
                    );
                    return Ok(true);
                }

                error!("Inference endpoint '{}' not found", self.inference_endpoint);
                info!("Available endpoints:");
                for ep in endpoints {
                    if let Some(id) = ep.get("inference_id").and_then(|v| v.as_str()) {
                        info!("  - {}", id);
                    }
                }
                Ok(false)
            }
            Err(e) => {
                warn!("Error checking inference endpoint: {}", e);
                warn!("Continuing anyway...");
                Ok(true)
            }
        }
    }

    async fn create_pipeline(&self) -> Result<bool> {
        let pipeline_config = json!({
            "description": "Extract text from PDF - semantic_text field handles chunking and embeddings",
            "processors": [
                {
                    "attachment": {
                        "field": "data",
                        "target_field": "attachment",
                        "remove_binary": true
                    }
                },
                {
                    "set": {
                        "field": "semantic_content",
                        "copy_from": "attachment.content",
                        "ignore_empty_value": true
                    }
                },
                {
                    "remove": {
                        "field": "data",
                        "ignore_missing": true
                    }
                },
                {
                    "set": {
                        "field": "upload_date",
                        "value": "{{ _ingest.timestamp }}"
                    }
                }
            ]
        });

        match self.client.create_pipeline(PIPELINE_NAME, &pipeline_config).await {
            Ok(_) => Ok(true),
            Err(e) => {
                error!("Error creating pipeline: {}", e);
                Ok(false)
            }
        }
    }

    async fn create_index(&mut self) -> Result<bool> {
        // Delete index if it exists before creating a new one
        if self.client.index_exists(ES_INDEX).await? {
            info!("Deleting existing index '{}' before import", ES_INDEX);
            match self.client.delete_index(ES_INDEX).await {
                Ok(true) => info!("Index '{}' deleted", ES_INDEX),
                Ok(false) => warn!("Failed to delete index '{}'", ES_INDEX),
                Err(e) => warn!("Error deleting index '{}': {}", ES_INDEX, e),
            }
        }

        // Update mapping with detected inference endpoint
        let mut mapping_with_inference = self.mapping.clone();
        if let Some(mappings) = mapping_with_inference.get_mut("mappings") {
            if let Some(properties) = mappings.get_mut("properties") {
                if let Some(semantic_content) = properties.get_mut("semantic_content") {
                    if let Some(obj) = semantic_content.as_object_mut() {
                        obj.insert(
                            "inference_id".to_string(),
                            json!(self.inference_endpoint),
                        );
                    }
                }
            }
        }

        info!("Creating index: {}", ES_INDEX);
        match self
            .client
            .create_index(ES_INDEX, &mapping_with_inference)
            .await
        {
            Ok(_) => {
                info!("Successfully created index: {}", ES_INDEX);
                Ok(true)
            }
            Err(e) => {
                error!("Error creating index: {}", e);
                Ok(false)
            }
        }
    }

    fn extract_airline_name(&self, filename: &str) -> String {
        let filename_lower = filename.to_lowercase();

        if filename_lower.contains("american") {
            "American Airlines".to_string()
        } else if filename_lower.contains("southwest") {
            "Southwest".to_string()
        } else if filename_lower.contains("united") {
            "United".to_string()
        } else if filename_lower.contains("delta") || filename_lower.contains("dl-") {
            "Delta".to_string()
        } else {
            "Unknown".to_string()
        }
    }

    fn get_pdf_files(&self, path: &Path) -> Result<Vec<PathBuf>> {
        if !path.exists() {
            error!("Path '{:?}' does not exist", path);
            return Ok(vec![]);
        }

        if path.is_file() {
            if path.extension().and_then(|s| s.to_str()) == Some("pdf") {
                return Ok(vec![path.to_path_buf()]);
            } else {
                error!("'{:?}' is not a PDF file", path);
                return Ok(vec![]);
            }
        } else if path.is_dir() {
            let mut pdf_files = Vec::new();
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file()
                    && path.extension().and_then(|s| s.to_str()) == Some("pdf")
                {
                    pdf_files.push(path);
                }
            }
            pdf_files.sort();
            if pdf_files.is_empty() {
                warn!("No PDF files found in directory '{:?}'", path);
            }
            return Ok(pdf_files);
        }

        Ok(vec![])
    }

    async fn index_pdf(&mut self, pdf_path: &Path) -> Result<bool> {
        let filename = pdf_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        let airline = self.extract_airline_name(filename);

        match fs::read(pdf_path) {
            Ok(pdf_data) => {
                let encoded_pdf = general_purpose::STANDARD.encode(&pdf_data);

                let document = json!({
                    "data": encoded_pdf,
                    "filename": filename,
                    "airline": airline
                });

                match self
                    .client
                    .index_document(ES_INDEX, &document, Some(PIPELINE_NAME))
                    .await
                {
                    Ok(_) => {
                        // Don't log here - progress is handled in ingest_pdfs()
                        self.indexed_count += 1;
                        Ok(true)
                    }
                    Err(e) => {
                        error!("Error processing {}: {}", filename, e);
                        // Log the full error chain for debugging
                        if let Some(source) = e.source() {
                            error!("  Caused by: {}", source);
                        }
                        Ok(false)
                    }
                }
            }
            Err(e) => {
                error!("Error reading {}: {}", filename, e);
                Ok(false)
            }
        }
    }

    async fn ingest_pdfs(&mut self, pdf_path: &Path) -> Result<bool> {
        let pdf_files = self.get_pdf_files(pdf_path)?;

        if pdf_files.is_empty() {
            error!("No PDF files to process");
            return Ok(false);
        }

        let total_files = pdf_files.len();
        info!("Processing {} PDF file(s)...", total_files);

        let mut success_count = 0;
        let mut failed_count = 0;
        let mut processed_count = 0;

        for pdf_file in pdf_files {
            if self.index_pdf(&pdf_file).await.unwrap_or(false) {
                success_count += 1;
            } else {
                failed_count += 1;
            }
            
            processed_count += 1;
            
            // Update progress
            let percentage = (processed_count as f64 / total_files as f64 * 100.0 * 10.0).round() / 10.0;
            print!("\r{} of {} files processed ({:.1}%)", processed_count, total_files, percentage);
            std::io::stdout().flush().ok();
        }

        // Print newline after progress line
        println!();

        info!("Indexed {} of {} file(s)", success_count, total_files);
        if failed_count > 0 {
            warn!("Failed: {}", failed_count);
        }

        Ok(failed_count == 0)
    }

    async fn verify_ingestion(&self) -> Result<()> {
        match self.client.count_documents(ES_INDEX).await {
            Ok(count) => {
                info!("Index '{}' contains {} document(s)", ES_INDEX, count);
                if count == 0 && self.indexed_count > 0 {
                    warn!(
                        "Warning: Expected {} document(s) but count shows 0. Documents may have failed during pipeline processing.",
                        self.indexed_count
                    );
                }
            }
            Err(e) => {
                warn!("Could not verify document count: {}", e);
            }
        }
        Ok(())
    }
}

fn load_yaml(path: &Path) -> Result<Value> {
    let resolved = resolve_with_project_fallback(path)
        .with_context(|| format!("Config file not found: {:?}", path))?;
    let content = fs::read_to_string(&resolved)
        .with_context(|| format!("Config file not found: {:?}", resolved))?;
    serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse YAML: {:?}", resolved))
}

fn load_json(path: &Path) -> Result<Value> {
    let resolved = resolve_with_project_fallback(path)
        .with_context(|| format!("Mapping file not found: {:?}", path))?;
    let content = fs::read_to_string(&resolved)
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    let args = Args::parse();

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

    let mapping = load_json(&args.mapping)?;

    let inference_endpoint = args.inference_endpoint;
    let mut loader = ContractLoader::new(client, mapping, inference_endpoint);

    // Check Elasticsearch connection
    if !loader.check_elasticsearch().await? {
        error!("Cannot connect to Elasticsearch. Exiting.");
        std::process::exit(1);
    }

    // Setup phase
    if !args.ingest_only {
        // Check ELSER endpoint
        if !loader.check_inference_endpoint().await? {
            error!("ELSER inference endpoint not found!");
            error!("Please deploy ELSER via Kibana or API before continuing.");
            error!("See: Management → Machine Learning → Trained Models → ELSER → Deploy");
            std::process::exit(1);
        }

        // Create pipeline
        if !loader.create_pipeline().await? {
            error!("Failed to create pipeline. Exiting.");
            std::process::exit(1);
        }

        // Create index (will delete existing one if present)
        if !loader.create_index().await? {
            error!("Failed to create index. Exiting.");
            std::process::exit(1);
        }
    }

    // Ingestion phase
    if !args.setup_only {
        let ingestion_start = std::time::Instant::now();

        let pdf_path = args.pdf_path.unwrap_or_else(|| {
            resolve_with_project_fallback(Path::new("data"))
                .unwrap_or_else(|_| PathBuf::from("data"))
        });

        if !loader.ingest_pdfs(&pdf_path).await? {
            error!("PDF ingestion had errors.");
            std::process::exit(1);
        }

        let elapsed = ingestion_start.elapsed();
        info!("Total ingestion time: {:.2} seconds", elapsed.as_secs_f64());

        // Verify ingestion
        loader.verify_ingestion().await?;
    }

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
