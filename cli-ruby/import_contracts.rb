#!/usr/bin/env ruby
# frozen_string_literal: true

require 'optparse'
require 'yaml'
require 'json'
require 'base64'
require 'elasticsearch'
require 'logger'
require 'time'
require 'pathname'

# Reuse ElasticsearchClient from import_flights.rb pattern
class ElasticsearchClient
  def initialize(config, logger:)
    endpoint = config.fetch('endpoint') do
      raise ArgumentError, 'endpoint is required in the Elasticsearch config'
    end

    @endpoint = endpoint
    @logger = logger
    @client = build_client(config, endpoint)
  end

  def index_exists?(name)
    @client.indices.exists(index: name)
  rescue Elastic::Transport::Transport::Error => e
    if e.message.include?('Connection refused') || e.message.include?('timeout')
      raise "Cannot connect to Elasticsearch at #{@endpoint}: #{e.message}. Please check your endpoint configuration and network connectivity."
    end
    raise "Failed to check index existence: #{e.message}"
  end

  def create_index(name, mapping)
    @client.indices.create(index: name, body: mapping)
    @logger.info("Index '#{name}' created")
  rescue Elastic::Transport::Transport::Errors::Conflict => e
    @logger.warn("Index '#{name}' already exists (conflict)")
  rescue Elastic::Transport::Transport::Error => e
    if e.message.include?('Connection refused') || e.message.include?('timeout')
      raise "Cannot connect to Elasticsearch at #{@endpoint}: #{e.message}. Please check your endpoint configuration and network connectivity."
    end
    raise "Index creation failed: #{e.message}"
  end

  def delete_index(name)
    @client.indices.delete(index: name)
    true
  rescue Elastic::Transport::Transport::Errors::NotFound => e
    false
  rescue Elastic::Transport::Transport::Error => e
    raise "Index deletion failed: #{e.message}"
  end

  def cluster_health
    @client.cluster.health
  rescue Elastic::Transport::Transport::Error => e
    raise "Cluster health request failed: #{e.message}"
  end

  def create_pipeline(name, pipeline_config)
    @client.ingest.put_pipeline(id: name, body: pipeline_config)
    @logger.info("Pipeline '#{name}' created/updated")
  rescue Elastic::Transport::Transport::Error => e
    raise "Pipeline creation failed: #{e.message}"
  end

  def index_document(index_name, document, pipeline: nil)
    options = { index: index_name, body: document }
    options[:pipeline] = pipeline if pipeline
    @client.index(options)
  rescue Elastic::Transport::Transport::Error => e
    raise "Document indexing failed: #{e.message}"
  end

  def get_inference_endpoints
    response = @client.transport.perform_request('GET', '/_inference/_all', {}, nil, {})
    response.body || { 'endpoints' => [] }
  rescue Elastic::Transport::Transport::Error => e
    @logger.warn("Failed to get inference endpoints: #{e.message}")
    { 'endpoints' => [] }
  end

  def count_documents(index_name)
    @client.count(index: index_name)['count']
  rescue Elastic::Transport::Transport::Error => e
    @logger.warn("Failed to count documents: #{e.message}")
    0
  end

  private

  def build_client(config, endpoint)
    client_options = {
      url: endpoint,
      log: false # We use our own logger
    }

    # Handle authentication
    if config['api_key'] && !config['api_key'].empty?
      client_options[:api_key] = config['api_key']
    elsif config['user'] && config['password']
      client_options[:user] = config['user']
      client_options[:password] = config['password']
    end

    # Handle SSL configuration
    ssl_options = {}
    ssl_verify = config.fetch('ssl_verify', true)
    ssl_options[:verify] = ssl_verify
    ssl_options[:ca_file] = presence(config['ca_file']) if config['ca_file']
    ssl_options[:ca_path] = presence(config['ca_path']) if config['ca_path']
    client_options[:ssl] = ssl_options unless ssl_options.empty?

    # Handle custom headers
    if config['headers'] && !config['headers'].empty?
      client_options[:transport_options] = {
        headers: config['headers'].transform_keys(&:to_s)
      }
    end

    Elasticsearch::Client.new(client_options)
  end

  def presence(value)
    return nil if value.nil?
    trimmed = value.to_s.strip
    trimmed.empty? ? nil : trimmed
  end
end

class ContractLoader
  ES_INDEX = 'contracts'
  PIPELINE_NAME = 'pdf_pipeline'
  DEFAULT_INFERENCE_ENDPOINT = '.elser-2-elastic'

  def initialize(client:, mapping:, logger:, inference_endpoint: nil)
    @client = client
    @mapping = mapping
    @logger = logger
    @inference_endpoint = inference_endpoint || DEFAULT_INFERENCE_ENDPOINT
    @indexed_count = 0
  end

  def check_elasticsearch
    health = @client.cluster_health
    @logger.info("Cluster: #{health['cluster_name'] || 'unknown'}")
    @logger.info("Status: #{health['status']}")
    true
  rescue StandardError => e
    @logger.error("Connection error: #{e.message}")
    false
  end

  def check_inference_endpoint
    begin
      response = @client.get_inference_endpoints
      endpoints = response['endpoints'] || []
      
      # First, try to find the specified endpoint
      found_endpoint = endpoints.find { |ep| ep['inference_id'] == @inference_endpoint }
      
      if found_endpoint
        @logger.info("Found inference endpoint: #{@inference_endpoint}")
        return true
      end
      
      # Auto-detect ELSER endpoints
      elser_endpoints = endpoints.select { |ep| ep['inference_id']&.downcase&.include?('elser') }
      
      if elser_endpoints.any?
        # Prefer endpoints starting with .elser-2- or .elser_model_2
        preferred = elser_endpoints.select { |ep| 
          id = ep['inference_id']
          id&.include?('.elser-2-') || id&.include?('.elser_model_2')
        }
        
        if preferred.any?
          @inference_endpoint = preferred.first['inference_id']
        else
          @inference_endpoint = elser_endpoints.first['inference_id']
        end
        
        @logger.warn("Specified endpoint not found, using auto-detected: #{@inference_endpoint}")
        return true
      end
      
      @logger.error("Inference endpoint '#{@inference_endpoint}' not found")
      @logger.info("Available endpoints:")
      endpoints.each do |ep|
        @logger.info("  - #{ep['inference_id']}")
      end
      false
    rescue StandardError => e
      @logger.warn("Error checking inference endpoint: #{e.message}")
      @logger.warn("Continuing anyway...")
      true
    end
  end

  def create_pipeline
    pipeline_config = {
      'description' => 'Extract text from PDF - semantic_text field handles chunking and embeddings',
      'processors' => [
        {
          'attachment' => {
            'field' => 'data',
            'target_field' => 'attachment',
            'remove_binary' => true
          }
        },
        {
          'set' => {
            'field' => 'semantic_content',
            'copy_from' => 'attachment.content',
            'ignore_empty_value' => true
          }
        },
        {
          'remove' => {
            'field' => 'data',
            'ignore_missing' => true
          }
        },
        {
          'set' => {
            'field' => 'upload_date',
            'value' => '{{ _ingest.timestamp }}'
          }
        }
      ]
    }
    
    @client.create_pipeline(PIPELINE_NAME, pipeline_config)
    true
  rescue StandardError => e
    @logger.error("Error creating pipeline: #{e.message}")
    false
  end

  def create_index
    # Delete index if it exists before creating a new one
    if @client.index_exists?(ES_INDEX)
      @logger.info("Deleting existing index '#{ES_INDEX}' before import")
      if @client.delete_index(ES_INDEX)
        @logger.info("Index '#{ES_INDEX}' deleted")
      else
        @logger.warn("Failed to delete index '#{ES_INDEX}'")
      end
    end
    
    # Update mapping with detected inference endpoint
    mapping_with_inference = @mapping.dup
    if mapping_with_inference['mappings'] && mapping_with_inference['mappings']['properties'] && 
       mapping_with_inference['mappings']['properties']['semantic_content']
      mapping_with_inference['mappings']['properties']['semantic_content']['inference_id'] = @inference_endpoint
    end
    
    @logger.info("Creating index: #{ES_INDEX}")
    @client.create_index(ES_INDEX, mapping_with_inference)
    @logger.info("Successfully created index: #{ES_INDEX}")
    true
  rescue StandardError => e
    @logger.error("Error creating index: #{e.message}")
    false
  end

  def extract_airline_name(filename)
    filename_lower = filename.downcase
    
    if filename_lower.include?('american')
      'American Airlines'
    elsif filename_lower.include?('southwest')
      'Southwest'
    elsif filename_lower.include?('united')
      'United'
    elsif filename_lower.include?('delta') || filename_lower.include?('dl-')
      'Delta'
    else
      'Unknown'
    end
  end

  def get_pdf_files(path)
    path_obj = Pathname.new(path)
    
    unless path_obj.exist?
      @logger.error("Path '#{path}' does not exist")
      return []
    end
    
    if path_obj.file?
      if path_obj.extname.downcase == '.pdf'
        [path_obj]
      else
        @logger.error("'#{path}' is not a PDF file")
        []
      end
    elsif path_obj.directory?
      pdf_files = Dir.glob(File.join(path_obj.to_s, '*.pdf')).map { |p| Pathname.new(p) }.sort
      if pdf_files.empty?
        @logger.warn("No PDF files found in directory '#{path}'")
      end
      pdf_files
    else
      []
    end
  end

  def index_pdf(pdf_path)
    pdf_path = Pathname.new(pdf_path)
    filename = pdf_path.basename.to_s
    airline = extract_airline_name(filename)
    
    begin
      # Read and encode the PDF
      pdf_data = pdf_path.binread
      encoded_pdf = Base64.strict_encode64(pdf_data)
      
      # Index the document
      document = {
        'data' => encoded_pdf,
        'filename' => filename,
        'airline' => airline
      }
      
      result = @client.index_document(ES_INDEX, document, pipeline: PIPELINE_NAME)
      
      @logger.info("Indexed: #{filename} (airline: #{airline})")
      @indexed_count += 1
      true
    rescue StandardError => e
      @logger.error("Error processing #{filename}: #{e.message}")
      false
    end
  end

  def ingest_pdfs(pdf_path)
    pdf_files = get_pdf_files(pdf_path)
    
    if pdf_files.empty?
      @logger.error('No PDF files to process')
      return false
    end
    
    @logger.info("Processing #{pdf_files.length} PDF file(s)...")
    
    success_count = 0
    failed_count = 0
    
    pdf_files.each do |pdf_file|
      if index_pdf(pdf_file)
        success_count += 1
      else
        failed_count += 1
      end
    end
    
    @logger.info("Indexed #{success_count} of #{pdf_files.length} file(s)")
    @logger.warn("Failed: #{failed_count}") if failed_count > 0
    
    failed_count == 0
  end

  def verify_ingestion
    begin
      count = @client.count_documents(ES_INDEX)
      @logger.info("Index '#{ES_INDEX}' contains #{count} document(s)")
      true
    rescue StandardError => e
      @logger.warn("Could not verify document count: #{e.message}")
      true
    end
  end
end

def parse_options(argv)
  options = {
    config: 'config/elasticsearch.yml',
    mapping: 'config/mappings-contracts.json',
    data_dir: 'data',
    index: 'contracts',
    setup_only: false,
    ingest_only: false,
    inference_endpoint: nil,
    status: false
  }

  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: import_contracts.rb [options]'

    opts.on('-c', '--config PATH', 'Path to Elasticsearch config YAML (default: config/elasticsearch.yml)') do |path|
      options[:config] = path
    end

    opts.on('-m', '--mapping PATH', 'Path to mappings JSON (default: config/mappings-contracts.json)') do |path|
      options[:mapping] = path
    end

    opts.on('--pdf-path PATH', 'Path to PDF file or directory containing PDFs (default: data)') do |path|
      options[:pdf_path] = path
    end


    opts.on('--setup-only', 'Only setup infrastructure (pipeline and index), skip PDF ingestion') do
      options[:setup_only] = true
    end

    opts.on('--ingest-only', 'Skip setup, only ingest PDFs (assumes infrastructure exists)') do
      options[:ingest_only] = true
    end

    opts.on('--inference-endpoint NAME', 'Inference endpoint ID (default: .elser-2-elastic, will auto-detect if not found)') do |name|
      options[:inference_endpoint] = name
    end

    opts.on('--status', 'Test connection and print cluster health status') do
      options[:status] = true
    end

    opts.on('-h', '--help', 'Show this help message') do
      puts opts
      puts "\nExamples:"
      puts "  # Setup and ingest PDFs from default location"
      puts "  ruby import_contracts.rb"
      puts ""
      puts "  # Setup and ingest PDFs from specific directory"
      puts "  ruby import_contracts.rb --pdf-path /path/to/pdfs"
      puts ""
      puts "  # Only setup infrastructure (skip PDF ingestion)"
      puts "  ruby import_contracts.rb --setup-only"
      puts ""
      puts "  # Skip setup and only ingest PDFs"
      puts "  ruby import_contracts.rb --ingest-only"
      exit
    end
  end

  parser.parse!(argv)
  options
end

def build_logger
  logger = Logger.new($stdout)
  logger.level = Logger::INFO
  logger
end

def resolve_path(path)
  # If path is absolute, use as-is
  return path if Pathname.new(path).absolute?
  
  # Try relative to current directory first (if it exists)
  return path if File.exist?(path)
  
  # Try relative to workspace root (one level up from script directory)
  script_dir = File.dirname(File.expand_path(__FILE__))
  workspace_root = File.expand_path(File.join(script_dir, '..'))
  candidate = File.expand_path(File.join(workspace_root, path))
  
  # Return resolved path even if file doesn't exist (for optional files)
  candidate
end

def load_config(path)
  resolved_path = resolve_path(path)
  YAML.safe_load(File.read(resolved_path)) || {}
rescue Errno::ENOENT
  raise "Config file not found: #{path} (tried: #{resolved_path})"
end

def load_mapping(path)
  resolved_path = resolve_path(path)
  JSON.parse(File.read(resolved_path))
rescue Errno::ENOENT
  raise "Mapping file not found: #{path} (tried: #{resolved_path})"
end

def main(argv)
  options = parse_options(argv)
  logger = build_logger

  config = load_config(options[:config])
  client = ElasticsearchClient.new(config, logger: logger)

  if options[:status]
    report_status(client, logger)
    return
  end

  mapping = load_mapping(options[:mapping])
  
  inference_endpoint = options[:inference_endpoint] || ContractLoader::DEFAULT_INFERENCE_ENDPOINT
  
  loader = ContractLoader.new(
    client: client,
    mapping: mapping,
    logger: logger,
    inference_endpoint: inference_endpoint
  )

  # Check Elasticsearch connection
  unless loader.check_elasticsearch
    logger.error("Cannot connect to Elasticsearch. Exiting.")
    exit 1
  end

  # Setup phase
  unless options[:ingest_only]
    # Check ELSER endpoint
    unless loader.check_inference_endpoint
      logger.error("ELSER inference endpoint not found!")
      logger.error("Please deploy ELSER via Kibana or API before continuing.")
      logger.error("See: Management → Machine Learning → Trained Models → ELSER → Deploy")
      exit 1
    end

    # Create pipeline
    unless loader.create_pipeline
      logger.error("Failed to create pipeline. Exiting.")
      exit 1
    end

    # Create index (will delete existing one if present)
    unless loader.create_index
      logger.error("Failed to create index. Exiting.")
      exit 1
    end
  end

  # Ingestion phase
  unless options[:setup_only]
    start_time = Time.now
    
    pdf_path = options[:pdf_path] || resolve_path('data')
    
    unless loader.ingest_pdfs(pdf_path)
      logger.error("PDF ingestion had errors.")
      exit 1
    end
    
    elapsed_time = Time.now - start_time
    logger.info("Total ingestion time: #{elapsed_time.round(2)} seconds")
    
    # Verify ingestion
    loader.verify_ingestion
  end
end

def report_status(client, logger)
  status = client.cluster_health
  logger.info("Cluster status: #{status['status']}")
  logger.info("Active shards: #{status['active_shards']}, node count: #{status['number_of_nodes']}")
rescue StandardError => e
  logger.error("Failed to retrieve cluster status: #{e.message}")
  exit 1
end

if $PROGRAM_NAME == __FILE__
  start_time = Time.now
  begin
    main(ARGV)
  ensure
    end_time = Time.now
    duration = end_time - start_time
    minutes = (duration / 60).floor
    seconds = (duration % 60).round(2)
    if minutes > 0
      $stdout.puts "\nTotal time: #{minutes}m #{seconds}s"
    else
      $stdout.puts "\nTotal time: #{seconds}s"
    end
  end
end
