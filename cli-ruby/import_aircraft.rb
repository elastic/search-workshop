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

class ModelLookup
  def initialize(models_file:, logger:)
    @logger = logger
    @models = {}
    @miss_count = 0
    @miss_limit = 10  # Only log first 10 misses
    load_models(models_file) if models_file && File.exist?(models_file)
  end

  def lookup(code)
    return nil if code.nil? || code.empty?

    @models[code.strip.upcase]
  end

  def log_miss(code)
    return if @miss_count >= @miss_limit
    
    @miss_count += 1
    @logger.warn("Model lookup miss for code: #{code.inspect} (total models loaded: #{@models.size})")
    
    # Show a sample of available codes for debugging
    if @miss_count == 1 && @models.size > 0
      sample_codes = @models.keys.first(5).join(', ')
      @logger.info("Sample model codes in lookup: #{sample_codes}")
    end
  end

  private

  def load_models(file_path)
    @logger.info("Loading models from #{file_path}")

    count = 0
    skipped = 0
    File.open(file_path, 'rb') do |file|
      gz = Zlib::GzipReader.new(file)
      begin
        # Read headers first
        header_line = gz.gets
        raise "No header line found" if header_line.nil?
        
        # Remove BOM (Byte Order Mark) if present
        header_line = header_line.sub(/^\xEF\xBB\xBF/, '').strip
        
        headers = CSV.parse_line(header_line, liberal_parsing: true)
        raise "Failed to parse headers" if headers.nil?
        
        # Clean headers - remove BOM and nil values
        headers = headers.map { |h| h&.sub(/^\xEF\xBB\xBF/, '')&.strip }.compact
        
        @logger.debug("Parsed headers: #{headers.inspect}") if @logger.respond_to?(:debug)

        # Process each line individually to handle malformed CSV gracefully
        line_number = 1
        gz.each_line do |line|
          line_number += 1
          
          begin
            # Strip line endings but preserve content
            line = line.chomp.strip
            next if line.empty?
            
            # Parse the line using CSV parser
            row_data = CSV.parse_line(line, liberal_parsing: true)
            if row_data.nil? || row_data.empty?
              skipped += 1
              @logger.debug("Skipping empty row #{line_number}") if @logger.respond_to?(:debug) && skipped <= 5
              next
            end
            
            # Check if we have enough columns
            if row_data.length < headers.length
              # Pad with nil values if needed
              row_data += [nil] * (headers.length - row_data.length)
            elsif row_data.length > headers.length
              # Trim excess columns
              row_data = row_data[0, headers.length]
            end
            
            # Convert to hash using headers
            row = CSV::Row.new(headers, row_data)
            
            code = row['CODE']&.strip
            if code.nil? || code.empty? || code == '\\N'
              skipped += 1
              @logger.debug("Skipping row #{line_number} - invalid CODE") if @logger.respond_to?(:debug) && skipped <= 5
              next
            end

            # Normalize code for storage (strip and uppercase)
            normalized_code = code.strip.upcase

            model_data = {
              code: code,
              mfr: present(row['MFR']),
              model: present(row['MODEL']),
              type_acft: present(row['TYPE-ACFT']),
              type_eng: present(row['TYPE-ENG']),
              ac_cat: present(row['AC-CAT']),
              build_cert_ind: present(row['BUILD-CERT-IND']),
              no_eng: present(row['NO-ENG']),
              no_seats: present(row['NO-SEATS']),
              ac_weight: present(row['AC-WEIGHT']),
              speed: present(row['SPEED']),
              tc_data_sheet: present(row['TC-DATA-SHEET']),
              tc_data_holder: present(row['TC-DATA-HOLDER'])
            }

            # Remove nil values
            model_data.compact!
            
            @models[normalized_code] = model_data
            count += 1
            
            # Log first few successful loads for debugging
            if count <= 3
              @logger.debug("Loaded model #{count}: CODE=#{code}, normalized=#{normalized_code}") if @logger.respond_to?(:debug)
            end
          rescue CSV::MalformedCSVError => e
            skipped += 1
            if skipped <= 10
              @logger.warn("Skipping malformed CSV row #{line_number} in models file: #{e.message}")
            end
            next
          rescue StandardError => e
            skipped += 1
            if skipped <= 10
              @logger.warn("Skipping row #{line_number} in models file due to error: #{e.message}")
              @logger.warn("  Error class: #{e.class}, Backtrace: #{e.backtrace.first(2).join(', ')}")
            end
            next
          end
        end
      ensure
        gz.close
      end
    end

    if skipped > 0
      @logger.info("Loaded #{count} models into lookup table (skipped #{skipped} malformed rows)")
    else
      @logger.info("Loaded #{count} models into lookup table")
    end
    
    if count == 0
      @logger.error("WARNING: No models were loaded! Check the models file format.")
    end
  end

  def present(value)
    return nil if value.nil?
    trimmed = value.to_s.strip
    trimmed.empty? ? nil : trimmed
  end
end

class AircraftLoader
  BATCH_SIZE = 500

  def initialize(client: nil, mapping:, index:, logger:, batch_size: BATCH_SIZE, refresh: false, models_file: nil)
    @client = client
    @mapping = mapping
    @index = index
    @logger = logger
    @batch_size = batch_size
    @refresh = refresh
    @model_lookup = ModelLookup.new(models_file: models_file, logger: logger)
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
      # Read headers first
      header_line = io.gets
      raise "No header line found" if header_line.nil?
      
      headers = CSV.parse_line(header_line.strip, liberal_parsing: true)
      raise "Failed to parse headers" if headers.nil?

      # Process each line individually to handle malformed CSV gracefully
      line_number = 1
      io.each_line do |line|
        line_number += 1
        processed_rows += 1
        
        begin
          # Parse the line
          row_data = CSV.parse_line(line.strip, liberal_parsing: true)
          next if row_data.nil? || row_data.empty?
          
          # Convert to hash using headers
          row = CSV::Row.new(headers, row_data)
          
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
          @logger.warn("Skipping malformed CSV row #{processed_rows} (line #{line_number}): #{e.message}")
          next
        rescue StandardError => e
          skipped_rows += 1
          @logger.warn("Skipping row #{processed_rows} (line #{line_number}) due to error: #{e.message}")
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
      csv = CSV.new(io, headers: true, return_headers: false)
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
    [line_count - 1, 0].max
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

    # Map CSV headers to Elasticsearch field names
    # CSV headers have spaces and special characters, ES fields use underscores
    header_mapping = {
      'N-NUMBER' => 'N_NUMBER',
      'SERIAL NUMBER' => 'SERIAL_NUMBER',
      'MFR MDL CODE' => 'MFR_MDL_CODE',
      'ENG MFR MDL' => 'ENG_MFR_MDL',
      'YEAR MFR' => 'YEAR_MFR',
      'TYPE REGISTRANT' => 'TYPE_REGISTRANT',
      'ZIP CODE' => 'ZIP_CODE',
      'LAST ACTION DATE' => 'LAST_ACTION_DATE',
      'CERT ISSUE DATE' => 'CERT_ISSUE_DATE',
      'TYPE AIRCRAFT' => 'TYPE_AIRCRAFT',
      'TYPE ENGINE' => 'TYPE_ENGINE',
      'STATUS CODE' => 'STATUS_CODE',
      'MODE S CODE' => 'MODE_S_CODE',
      'FRACT OWNER' => 'FRACT_OWNER',
      'AIR WORTH DATE' => 'AIR_WORTH_DATE',
      'OTHER NAMES(1)' => 'OTHER_NAMES_1',
      'OTHER NAMES(2)' => 'OTHER_NAMES_2',
      'OTHER NAMES(3)' => 'OTHER_NAMES_3',
      'OTHER NAMES(4)' => 'OTHER_NAMES_4',
      'OTHER NAMES(5)' => 'OTHER_NAMES_5',
      'EXPIRATION DATE' => 'EXPIRATION_DATE',
      'UNIQUE ID' => 'UNIQUE_ID',
      'KIT MFR' => 'KIT_MFR',
      'KIT MODEL' => 'KIT_MODEL',
      'MODE S CODE HEX' => 'MODE_S_CODE_HEX'
    }

    # Process each CSV column
    row.headers.each do |csv_header|
      next if csv_header.nil?
      es_field = header_mapping[csv_header] || csv_header.upcase.gsub(/[^A-Z0-9]/, '_').gsub(/_+/, '_').gsub(/^_|_$/, '')
      value = present(row[csv_header])
      next if value.nil?

      # Handle date fields (convert yyyyMMdd to yyyy-MM-dd)
      if %w[LAST_ACTION_DATE CERT_ISSUE_DATE AIR_WORTH_DATE EXPIRATION_DATE].include?(es_field)
        doc[es_field] = format_date(value)
      # Handle integer fields
      elsif %w[YEAR_MFR TYPE_REGISTRANT].include?(es_field)
        int_value = to_integer(value)
        doc[es_field] = int_value if int_value
      # Handle N_NUMBER - prepend 'N' if not already present
      elsif es_field == 'N_NUMBER'
        n_number = value.strip
        doc[es_field] = n_number.start_with?('N') ? n_number : "N#{n_number}"
      else
        doc[es_field] = value
      end
    end

    # Lookup and enrich with model data
    mfr_mdl_code = doc['MFR_MDL_CODE']
    if mfr_mdl_code
      # Strip and normalize the code for lookup
      lookup_code = mfr_mdl_code.to_s.strip.upcase
      model = @model_lookup.lookup(lookup_code)
      if model
        # Add model fields to document
        doc['Model_MFR'] = model[:mfr] if model[:mfr]
        doc['Model_MODEL'] = model[:model] if model[:model]
        doc['Model_TYPE_ACFT'] = model[:type_acft] if model[:type_acft]
        doc['Model_TYPE_ENG'] = model[:type_eng] if model[:type_eng]
        doc['Model_AC_CAT'] = model[:ac_cat] if model[:ac_cat]
        doc['Model_BUILD_CERT_IND'] = model[:build_cert_ind] if model[:build_cert_ind]
        doc['Model_NO_ENG'] = model[:no_eng] if model[:no_eng]
        doc['Model_NO_SEATS'] = model[:no_seats] if model[:no_seats]
        doc['Model_AC_WEIGHT'] = model[:ac_weight] if model[:ac_weight]
        doc['Model_SPEED'] = model[:speed] if model[:speed]
        doc['Model_TC_DATA_SHEET'] = model[:tc_data_sheet] if model[:tc_data_sheet]
        doc['Model_TC_DATA_HOLDER'] = model[:tc_data_holder] if model[:tc_data_holder]
      else
        # Debug: log when model lookup fails (only first few times to avoid spam)
        @model_lookup.log_miss(lookup_code) if @model_lookup.respond_to?(:log_miss)
      end
    end

    # Copy YEAR_MFR to @timestamp (convert year to yyyyMMdd format: yyyy0101)
    if doc['YEAR_MFR']
      year = doc['YEAR_MFR'].to_s
      # Convert year (e.g., 1940) to date format (e.g., 19400101 for Jan 1st)
      if year =~ /^\d{4}$/
        doc['@timestamp'] = "#{year}0101"
      end
    # Fallback to AIR_WORTH_DATE if YEAR_MFR is not available
    elsif doc['AIR_WORTH_DATE']
      doc['@timestamp'] = doc['AIR_WORTH_DATE']
    end

    doc.compact
  end

  def present(value)
    return nil if value.nil?
    trimmed = value.to_s.strip
    trimmed.empty? ? nil : trimmed
  end

  def format_date(value)
    return nil unless value

    value = value.strip
    return nil if value.empty?

    # Keep dates in yyyyMMdd format as expected by the mapping
    # The CSV already provides dates in this format (e.g., "20230122")
    if value =~ /^\d{8}$/
      value
    elsif value =~ /^(\d{4})-(\d{2})-(\d{2})$/
      # Convert from yyyy-MM-dd to yyyyMMdd if needed
      "#{Regexp.last_match(1)}#{Regexp.last_match(2)}#{Regexp.last_match(3)}"
    else
      # Return as-is if already in correct format or unknown format
      value
    end
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
    mapping: 'config/mappings-aircraft.json',
    index: 'aircraft',
    batch_size: AircraftLoader::BATCH_SIZE,
    refresh: false,
    status: false,
    delete_index: false,
    sample: false,
    models_file: 'data/models.csv.gz'
  }

  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: import_aircraft.rb [options]'

    opts.on('-c', '--config PATH', 'Path to Elasticsearch config YAML (default: config/elasticsearch.yml)') do |path|
      options[:config] = path
    end

    opts.on('-m', '--mapping PATH', 'Path to mappings JSON (default: config/mappings-aircraft.json)') do |path|
      options[:mapping] = path
    end

    opts.on('-f', '--file PATH', 'Path to aircraft CSV file (default: data/aircraft.csv.gz)') do |path|
      options[:file] = path
    end

    opts.on('--index NAME', 'Override index name (default: aircraft)') do |name|
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

    opts.on('--delete-index', 'Delete the aircraft index and exit') do
      options[:delete_index] = true
    end

    opts.on('--sample', 'Print the first document and exit') do
      options[:sample] = true
    end

    opts.on('--models-file PATH', 'Path to models CSV file (default: data/models.csv.gz)') do |path|
      options[:models_file] = path
    end

    opts.on('-h', '--help', 'Show this help message') do
      puts opts
      exit
    end
  end

  parser.parse!(argv)

  unless options[:status] || options[:delete_index] || options[:sample]
    options[:file] ||= 'data/aircraft.csv.gz'
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
  resolved_models_file = options[:models_file] ? resolve_path(options[:models_file]) : nil

  loader = AircraftLoader.new(
    client: client,
    mapping: mapping,
    index: options[:index],
    logger: logger,
    batch_size: options[:batch_size],
    refresh: options[:refresh],
    models_file: resolved_models_file
  )

  loader.import_file(resolved_file)
end

def sample_document(options:, logger:)
  mapping = load_mapping(options[:mapping])
  resolved_file = resolve_path(options[:file])
  resolved_models_file = options[:models_file] ? resolve_path(options[:models_file]) : nil

  loader = AircraftLoader.new(
    client: nil,
    mapping: mapping,
    index: 'aircraft',
    logger: logger,
    batch_size: 1,
    refresh: false,
    models_file: resolved_models_file
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
