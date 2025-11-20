#!/usr/bin/env ruby
# frozen_string_literal: true

require 'optparse'
require 'yaml'
require 'json'
require 'csv'
require 'elasticsearch'
require 'logger'

# Embedded mapping for cancellations index
CANCELLATIONS_MAPPING = {
  'settings' => {
    'index' => {
      'mode' => 'lookup'
    }
  },
  'mappings' => {
    'properties' => {
      'Code' => {
        'type' => 'keyword'
      },
      'Description' => {
        'type' => 'keyword'
      }
    }
  }
}.freeze

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
  rescue Elasticsearch::Transport::Transport::Error => e
    if e.message.include?('Connection refused') || e.message.include?('timeout')
      raise "Cannot connect to Elasticsearch at #{@endpoint}: #{e.message}. Please check your endpoint configuration and network connectivity."
    end
    raise "Failed to check index existence: #{e.message}"
  end

  def create_index(name, mapping)
    @client.indices.create(index: name, body: mapping)
    @logger.info("Index '#{name}' created")
  rescue Elasticsearch::Transport::Transport::Errors::Conflict => e
    @logger.warn("Index '#{name}' already exists (conflict)")
  rescue Elasticsearch::Transport::Transport::Error => e
    if e.message.include?('Connection refused') || e.message.include?('timeout')
      raise "Cannot connect to Elasticsearch at #{@endpoint}: #{e.message}. Please check your endpoint configuration and network connectivity."
    end
    raise "Index creation failed: #{e.message}"
  end

  def bulk(index, payload, refresh: false)
    # The official gem handles bulk operations directly
    # payload is already in NDJSON format (string)
    result = @client.bulk(index: index, body: payload, refresh: refresh)
    result
  rescue Elasticsearch::Transport::Transport::Error => e
    raise "Bulk request failed: #{e.message}"
  end

  def delete_index(name)
    @client.indices.delete(index: name)
    true
  rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
    false
  rescue Elasticsearch::Transport::Transport::Error => e
    raise "Index deletion failed: #{e.message}"
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
end

def presence(value)
  return nil if value.nil?
  trimmed = value.to_s.strip
  trimmed.empty? ? nil : trimmed
end

class CancellationsLoader
  BATCH_SIZE = 1000

  def initialize(client:, index:, logger:, batch_size: BATCH_SIZE, refresh: false)
    @client = client
    @index = index
    @logger = logger
    @batch_size = batch_size
    @refresh = refresh
  end

  def ensure_index
    if @client.index_exists?(@index)
      @logger.info("Deleting existing index '#{@index}'")
      @client.delete_index(@index)
    end

    @client.create_index(@index, CANCELLATIONS_MAPPING)
  end

  def import_file(file_path)
    unless File.file?(file_path)
      @logger.warn("Skipping #{file_path} (not a regular file)")
      return
    end

    @logger.info("Importing cancellations from #{file_path}")

    buffered_lines = []
    buffered_docs = 0
    indexed_docs = 0
    processed_rows = 0

    CSV.foreach(file_path, headers: true) do |row|
      processed_rows += 1

      code = presence(row['Code'])
      description = presence(row['Description'])

      # Skip rows without Code or Description
      next if code.nil? || code.empty?
      next if description.nil? || description.empty?

      doc = {
        'Code' => code,
        'Description' => description
      }

      buffered_lines << { index: {} }.to_json
      buffered_lines << doc.to_json
      buffered_docs += 1

      if buffered_docs >= @batch_size
        indexed_docs += flush(buffered_lines, buffered_docs)
        buffered_lines.clear
        buffered_docs = 0
      end
    end

    if buffered_docs.positive?
      indexed_docs += flush(buffered_lines, buffered_docs)
    end

    @logger.info("Finished #{file_path} (rows processed: #{processed_rows}, documents indexed: #{indexed_docs})")
  end

  private

  def flush(lines, doc_count)
    payload = lines.join("\n") + "\n"
    result = @client.bulk(@index, payload, refresh: @refresh)

    if result['errors']
      errors = result.fetch('items', []).map { |item| item['index'] }.select { |info| info && info['error'] }
      errors.first(5).each do |error|
        @logger.error("Bulk item error: #{error['error']}")
      end
      raise 'Bulk indexing reported errors; aborting'
    end

    doc_count
  rescue StandardError => e
    @logger.error("Bulk flush failed: #{e.message}")
    raise
  end
end

def parse_options(argv)
  options = {
    config: 'config/elasticsearch.yml',
    cancellations_file: 'data/cancellations.csv',
    index: 'cancellations',
    batch_size: CancellationsLoader::BATCH_SIZE,
    refresh: false,
    delete_index: false
  }

  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: import_cancellations.rb [options]'

    opts.on('-c', '--config PATH', 'Path to Elasticsearch config YAML (default: config/elasticsearch.yml)') do |path|
      options[:config] = path
    end

    opts.on('-f', '--file PATH', 'Path to cancellations CSV file (default: data/cancellations.csv)') do |path|
      options[:cancellations_file] = path
    end

    opts.on('--index NAME', "Override index name (default: cancellations)") do |name|
      options[:index] = name
    end

    opts.on('--batch-size N', Integer, 'Number of documents per bulk request (default: 1000)') do |size|
      options[:batch_size] = size
    end

    opts.on('--refresh', 'Request an index refresh after each bulk request') do
      options[:refresh] = true
    end

    opts.on('--delete-index', 'Delete the target index and exit') do
      options[:delete_index] = true
    end

    opts.on('-h', '--help', 'Show this help message') do
      puts opts
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

def load_config(path)
  YAML.safe_load(File.read(path)) || {}
rescue Errno::ENOENT
  raise "Config file not found: #{path}"
end

def resolve_file_path(path)
  expanded = File.expand_path(path)
  return expanded if File.exist?(expanded)

  raise "File not found: #{path}"
end

def main(argv)
  options = parse_options(argv)
  logger = build_logger

  config = load_config(options[:config])

  client = ElasticsearchClient.new(config, logger: logger)

  if options[:delete_index]
    if client.delete_index(options[:index])
      logger.info("Index '#{options[:index]}' deleted")
    else
      logger.warn("Index '#{options[:index]}' was not found")
    end
    return
  end

  cancellations_file = resolve_file_path(options[:cancellations_file])

  loader = CancellationsLoader.new(
    client: client,
    index: options[:index],
    logger: logger,
    batch_size: options[:batch_size],
    refresh: options[:refresh]
  )

  loader.ensure_index
  loader.import_file(cancellations_file)
end

if $PROGRAM_NAME == __FILE__
  main(ARGV)
end
