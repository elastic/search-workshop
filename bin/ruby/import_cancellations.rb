#!/usr/bin/env ruby
# frozen_string_literal: true

require 'optparse'
require 'yaml'
require 'json'
require 'csv'
require 'net/http'
require 'uri'
require 'logger'
require 'openssl'

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

    @base_uri = URI(endpoint)
    @base_path = @base_uri.path
    @base_path = '' if @base_path.nil? || @base_path == '/'
    @logger = logger
    @headers = (config['headers'] || {}).transform_keys(&:to_s)
    @user = config['user']
    @password = config['password']
    @api_key = config['api_key']
    @ssl_verify = config.fetch('ssl_verify', true)
    @ca_file = presence(config['ca_file'])
    @ca_path = presence(config['ca_path'])
  end

  def index_exists?(name)
    response = request(:head, index_path(name))
    response.is_a?(Net::HTTPSuccess)
  rescue SocketError, Errno::ECONNREFUSED, Errno::ETIMEDOUT => e
    raise "Cannot connect to Elasticsearch at #{@base_uri}: #{e.message}. Please check your endpoint configuration and network connectivity."
  rescue StandardError => e
    raise "Failed to check index existence: #{e.message}"
  end

  def create_index(name, mapping)
    response = request(
      :put,
      index_path(name),
      body: JSON.dump(mapping),
      headers: { 'Content-Type' => 'application/json' }
    )

    case response
    when Net::HTTPSuccess
      @logger.info("Index '#{name}' created")
    when Net::HTTPConflict
      @logger.warn("Index '#{name}' already exists (conflict)")
    else
      raise "Index creation failed: #{response.code} #{response.body}"
    end
  rescue SocketError, Errno::ECONNREFUSED, Errno::ETIMEDOUT => e
    raise "Cannot connect to Elasticsearch at #{@base_uri}: #{e.message}. Please check your endpoint configuration and network connectivity."
  end

  def bulk(index, payload, refresh: false)
    response = request(
      :post,
      "#{index_path(index)}/_bulk",
      body: payload,
      headers: { 'Content-Type' => 'application/x-ndjson' },
      params: { refresh: refresh ? 'true' : 'false' }
    )

    unless response.is_a?(Net::HTTPSuccess)
      raise "Bulk request failed: #{response.code} #{response.body}"
    end

    JSON.parse(response.body)
  end

  def delete_index(name)
    response = request(:delete, index_path(name))

    case response
    when Net::HTTPSuccess
      true
    when Net::HTTPNotFound
      false
    else
      raise "Index deletion failed: #{response.code} #{response.body}"
    end
  end

  private

  def index_path(name)
    "/#{name}"
  end

  def request(method, path, body: nil, headers: {}, params: nil)
    uri = build_uri(path, params: params)
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = uri.scheme == 'https'
    http.verify_mode = @ssl_verify ? OpenSSL::SSL::VERIFY_PEER : OpenSSL::SSL::VERIFY_NONE
    http.ca_file = @ca_file if @ca_file
    http.ca_path = @ca_path if @ca_path

    request = build_request(method, uri, body, headers)

    http.request(request)
  end

  def build_request(method, uri, body, headers)
    request_class = case method.to_sym
                    when :get then Net::HTTP::Get
                    when :head then Net::HTTP::Head
                    when :put then Net::HTTP::Put
                    when :post then Net::HTTP::Post
                    when :delete then Net::HTTP::Delete
                    else
                      raise ArgumentError, "Unsupported HTTP method: #{method}"
                    end

    request = request_class.new(uri.request_uri)

    merged_headers = @headers.merge(headers.transform_keys(&:to_s))
    merged_headers.each { |k, v| request[k] = v }

    if @api_key && !@api_key.empty?
      request['Authorization'] = "ApiKey #{@api_key}"
    elsif @user && @password
      request.basic_auth(@user, @password)
    end

    if body
      request.body = body
      request['Content-Length'] = body.bytesize.to_s
    end

    request
  end

  def build_uri(path, params: nil)
    normalized_path = path.start_with?('/') ? path : "/#{path}"
    full_path = "#{@base_path}#{normalized_path}"

    uri = @base_uri.dup
    uri.path = full_path
    uri.query = params ? URI.encode_www_form(params) : nil
    uri
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
