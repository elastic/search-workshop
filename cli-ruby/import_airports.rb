#!/usr/bin/env ruby
# frozen_string_literal: true

require 'optparse'
require 'yaml'
require 'json'
require 'csv'
require 'elasticsearch'
require 'logger'
require 'time'
require 'open3'
require 'English'
require 'zlib'
require 'set'
require 'shellwords'
require 'pathname'

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
    result = @client.bulk(body: payload, refresh: refresh)
    result
  rescue Elasticsearch::Transport::Transport::Error => e
    raise "Bulk request failed: #{e.message}"
  end

  def cluster_health
    @client.cluster.health
  rescue Elasticsearch::Transport::Transport::Error => e
    raise "Cluster health request failed: #{e.message}"
  end

  def delete_index(name)
    @client.indices.delete(index: name)
    true
  rescue Elasticsearch::Transport::Transport::Errors::NotFound => e
    false
  rescue Elasticsearch::Transport::Transport::Error => e
    raise "Index deletion failed: #{e.message}"
  end

  def list_indices(pattern = '*')
    response = @client.cat.indices(format: 'json', index: pattern)
    response.map { |idx| idx['index'] }.compact
  rescue Elasticsearch::Transport::Transport::Error => e
    raise "Failed to list indices: #{e.message}"
  end

  def delete_indices_by_pattern(pattern)
    indices = list_indices(pattern)
    return [] if indices.empty?

    deleted = []
    indices.each do |index_name|
      if delete_index(index_name)
        deleted << index_name
      end
    end
    deleted
  end

  private

  def build_client(config, endpoint)
    client_options = {
      url: endpoint,
      log: false
    }

    if config['api_key'] && !config['api_key'].empty?
      client_options[:api_key] = config['api_key']
    elsif config['user'] && config['password']
      client_options[:user] = config['user']
      client_options[:password] = config['password']
    end

    ssl_options = {}
    ssl_verify = config.fetch('ssl_verify', true)
    ssl_options[:verify] = ssl_verify
    ssl_options[:ca_file] = presence(config['ca_file']) if config['ca_file']
    ssl_options[:ca_path] = presence(config['ca_path']) if config['ca_path']
    client_options[:ssl] = ssl_options unless ssl_options.empty?

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

class AirportLoader
  BATCH_SIZE = 500

  def initialize(client: nil, mapping:, index:, logger:, batch_size: BATCH_SIZE, refresh: false)
    @client = client
    @mapping = mapping
    @index = index
    @logger = logger
    @batch_size = batch_size
    @refresh = refresh
    @loaded_records = 0
    @total_records = 0
  end

  def ensure_index
    return unless @client

    if @client.index_exists?(@index)
      @logger.info("Deleting existing index '#{@index}' before import")
      if @client.delete_index(@index)
        @logger.info("Index '#{@index}' deleted")
      else
        @logger.warn("Failed to delete index '#{@index}'")
      end
    end

    @logger.info("Creating index: #{@index}")
    @client.create_index(@index, @mapping)
    @logger.info("Successfully created index: #{@index}")
  end

  def import_file(file_path)
    unless File.file?(file_path)
      @logger.warn("Skipping #{file_path} (not a regular file)")
      return
    end

    @logger.info("Counting records in #{file_path}...")
    @total_records = count_total_records_fast(file_path)
    @logger.info("Total records to import: #{format_number(@total_records)}")
    @logger.info("Importing #{file_path}...")

    ensure_index

    buffer = []
    buffer_count = 0
    processed_rows = 0
    skipped_rows = 0

    with_data_io(file_path) do |io|
      # CSV has no headers, parse positionally
      csv = CSV.new(io, headers: false, liberal_parsing: true)

      csv.each do |row|
        processed_rows += 1
        
        begin
          doc = transform_row(row)
          next if doc.nil? || doc.empty?

          buffer << { index: { _index: @index } }.to_json
          buffer << doc.to_json
          buffer_count += 1

          if buffer_count >= @batch_size
            flush_buffer(buffer, buffer_count)
            buffer.clear
            buffer_count = 0
          end
        rescue CSV::MalformedCSVError => e
          skipped_rows += 1
          @logger.warn("Skipping malformed CSV row #{processed_rows}: #{e.message}")
          next
        rescue StandardError => e
          skipped_rows += 1
          @logger.warn("Skipping row #{processed_rows} due to error: #{e.message}")
          next
        end
      end
    end

    if buffer_count.positive?
      flush_buffer(buffer, buffer_count)
    end

    $stdout.puts
    if skipped_rows > 0
      @logger.info("Finished #{file_path} (rows processed: #{processed_rows}, documents indexed: #{@loaded_records}, skipped: #{skipped_rows})")
    else
      @logger.info("Finished #{file_path} (rows processed: #{processed_rows}, documents indexed: #{@loaded_records})")
    end
  end

  def sample_document(file_path)
    unless File.file?(file_path)
      @logger.warn("Skipping #{file_path} (not a regular file)")
      return nil
    end

    @logger.info("Sampling first document from #{file_path}")

    with_data_io(file_path) do |io|
      csv = CSV.new(io, headers: false, liberal_parsing: true)
      row = csv.first
      return nil if row.nil?

      doc = transform_row(row)
      doc
    end
  end

  private

  def format_number(number)
    number.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse
  end

  def count_total_records_fast(file_path)
    line_count = count_lines_fast(file_path)
    [line_count, 0].max
  end

  def count_lines_fast(file_path)
    if file_path.downcase.end_with?('.gz')
      result = `gunzip -c #{Shellwords.escape(file_path)} | wc -l`.strip
      result.to_i
    else
      result = `wc -l #{Shellwords.escape(file_path)}`.strip
      result.split.first.to_i
    end
  rescue StandardError => e
    @logger.warn("Failed to count lines in #{file_path}: #{e.message}")
    0
  end

  def flush_buffer(buffer, doc_count)
    payload = buffer.join("\n") + "\n"
    result = @client.bulk(@index, payload, refresh: @refresh)

    if result['errors']
      errors = result.fetch('items', []).map { |item| item['index'] }.select { |info| info && info['error'] }
      errors.first(5).each do |error|
        @logger.error("Bulk item error for #{@index}: #{error['error']}")
      end
      raise "Bulk indexing reported errors for #{@index}; aborting"
    end

    @loaded_records += doc_count
    if @total_records > 0
      percentage = (@loaded_records.to_f / @total_records * 100).round(1)
      $stdout.print "\r#{format_number(@loaded_records)} of #{format_number(@total_records)} records loaded (#{percentage}%)"
    else
      $stdout.print "\r#{format_number(@loaded_records)} records loaded"
    end
    $stdout.flush
  rescue StandardError => e
    @logger.error("Bulk flush failed for #{@index}: #{e.message}")
    raise
  end

  def with_data_io(file_path)
    if file_path.downcase.end_with?('.gz')
      File.open(file_path, 'rb') do |file|
        gz = Zlib::GzipReader.new(file)
        begin
          yield gz
        ensure
          gz.close
        end
      end
    else
      File.open(file_path, 'r', encoding: 'UTF-8') do |io|
        yield io
      end
    end
  end

  def transform_row(row)
    doc = {}

    # CSV columns (no headers):
    # 0: ID
    # 1: Name
    # 2: City
    # 3: Country
    # 4: IATA
    # 5: ICAO
    # 6: Latitude
    # 7: Longitude
    # 8: Altitude
    # 9: Timezone offset
    # 10: DST
    # 11: Timezone
    # 12: Type
    # 13: Source

    return nil if row.nil? || row.length < 8

    # ID
    id_value = present(row[0])
    if id_value
      doc['id'] = to_integer(id_value)
    end

    # Name
    doc['name'] = present(row[1])

    # City
    doc['city'] = present(row[2])

    # Country
    doc['country'] = present(row[3])

    # IATA code
    iata = present(row[4])
    doc['iata'] = iata if iata && iata != '\\N'

    # ICAO code
    icao = present(row[5])
    doc['icao'] = icao if icao && icao != '\\N'

    # Latitude
    lat_value = present(row[6])
    if lat_value
      lat = to_float(lat_value)
      doc['latitude'] = lat if lat
    end

    # Longitude
    lon_value = present(row[7])
    if lon_value
      lon = to_float(lon_value)
      doc['longitude'] = lon if lon
    end

    # Location (geo_point) - combine lat/lon if both present
    if doc['latitude'] && doc['longitude']
      doc['location'] = "#{doc['latitude']},#{doc['longitude']}"
    end

    # Altitude
    if row.length > 8
      alt_value = present(row[8])
      if alt_value
        alt = to_integer(alt_value)
        doc['altitude'] = alt if alt
      end
    end

    # Timezone offset
    if row.length > 9
      tz_offset_value = present(row[9])
      if tz_offset_value
        tz_offset = to_float(tz_offset_value)
        doc['timezone_offset'] = tz_offset if tz_offset
      end
    end

    # DST
    if row.length > 10
      dst = present(row[10])
      doc['dst'] = dst if dst && dst != '\\N'
    end

    # Timezone
    if row.length > 11
      timezone = present(row[11])
      doc['timezone'] = timezone if timezone && timezone != '\\N'
    end

    # Type
    if row.length > 12
      type = present(row[12])
      doc['type'] = type if type && type != '\\N'
    end

    # Source
    if row.length > 13
      source = present(row[13])
      doc['source'] = source if source && source != '\\N'
    end

    doc.compact
  end

  def present(value)
    return nil if value.nil?
    trimmed = value.to_s.strip
    trimmed.empty? ? nil : trimmed
  end

  def to_float(value)
    value = present(value)
    return nil unless value

    Float(value)
  rescue ArgumentError
    nil
  end

  def to_integer(value)
    value = present(value)
    return nil unless value

    Float(value).round
  rescue ArgumentError
    nil
  end
end

def parse_options(argv)
  options = {
    config: 'config/elasticsearch.yml',
    mapping: 'config/mappings-airports.json',
    index: 'airports',
    batch_size: AirportLoader::BATCH_SIZE,
    refresh: false,
    status: false,
    delete_index: false,
    sample: false
  }

  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: import_airports.rb [options]'

    opts.on('-c', '--config PATH', 'Path to Elasticsearch config YAML (default: config/elasticsearch.yml)') do |path|
      options[:config] = path
    end

    opts.on('-m', '--mapping PATH', 'Path to mappings JSON (default: config/mappings-airports.json)') do |path|
      options[:mapping] = path
    end

    opts.on('-f', '--file PATH', 'Path to airports CSV file (default: data/airports.csv.gz)') do |path|
      options[:file] = path
    end

    opts.on('--index NAME', 'Override index name (default: airports)') do |name|
      options[:index] = name
    end

    opts.on('--batch-size N', Integer, 'Number of documents per bulk request (default: 500)') do |size|
      options[:batch_size] = size
    end

    opts.on('--refresh', 'Request an index refresh after each bulk request') do
      options[:refresh] = true
    end

    opts.on('--status', 'Test connection and print cluster health status') do
      options[:status] = true
    end

    opts.on('--delete-index', 'Delete the airports index and exit') do
      options[:delete_index] = true
    end

    opts.on('--sample', 'Print the first document and exit') do
      options[:sample] = true
    end

    opts.on('-h', '--help', 'Show this help message') do
      puts opts
      exit
    end
  end

  parser.parse!(argv)

  unless options[:status] || options[:delete_index] || options[:sample]
    options[:file] ||= 'data/airports.csv.gz'
  end

  options
end

def build_logger
  logger = Logger.new($stdout)
  logger.level = Logger::INFO
  logger
end

def resolve_path(path)
  return path if Pathname.new(path).absolute?
  return path if File.exist?(path)

  script_dir = File.dirname(File.expand_path(__FILE__))
  workspace_root = File.expand_path(File.join(script_dir, '..'))
  candidate = File.expand_path(File.join(workspace_root, path))
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

  if options[:sample]
    sample_document(options: options, logger: logger)
    return
  end

  config = load_config(options[:config])
  client = ElasticsearchClient.new(config, logger: logger)

  if options[:status]
    report_status(client, logger)
    return
  end

  if options[:delete_index]
    delete_index(client, logger, options[:index])
    return
  end

  mapping = load_mapping(options[:mapping])
  resolved_file = resolve_path(options[:file])

  loader = AirportLoader.new(
    client: client,
    mapping: mapping,
    index: options[:index],
    logger: logger,
    batch_size: options[:batch_size],
    refresh: options[:refresh]
  )

  loader.import_file(resolved_file)
end

def sample_document(options:, logger:)
  mapping = load_mapping(options[:mapping])
  resolved_file = resolve_path(options[:file])

  loader = AirportLoader.new(
    client: nil,
    mapping: mapping,
    index: 'airports',
    logger: logger,
    batch_size: 1,
    refresh: false
  )

  doc = loader.sample_document(resolved_file)
  if doc.nil?
    logger.error('No document found in file')
    exit 1
  end

  puts JSON.pretty_generate(doc)
end

def report_status(client, logger)
  status = client.cluster_health
  logger.info("Cluster status: #{status['status']}")
  logger.info("Active shards: #{status['active_shards']}, node count: #{status['number_of_nodes']}")
rescue StandardError => e
  logger.error("Failed to retrieve cluster status: #{e.message}")
  exit 1
end

def delete_index(client, logger, index_name)
  logger.info("Deleting index: #{index_name}")

  if client.delete_index(index_name)
    logger.info("Index '#{index_name}' deleted")
  else
    logger.warn("Index '#{index_name}' not found")
  end
rescue StandardError => e
  logger.error("Failed to delete index '#{index_name}': #{e.message}")
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
