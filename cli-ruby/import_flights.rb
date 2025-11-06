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
require 'time'
require 'open3'
require 'English'
require 'zlib'
require 'set'

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
    # When _index is specified in action lines, use global _bulk endpoint
    # The index parameter is kept for backward compatibility but ignored when _index is in payload
    bulk_path = '/_bulk'
    response = request(
      :post,
      bulk_path,
      body: payload,
      headers: { 'Content-Type' => 'application/x-ndjson' },
      params: { refresh: refresh ? 'true' : 'false' }
    )

    unless response.is_a?(Net::HTTPSuccess)
      raise "Bulk request failed: #{response.code} #{response.body}"
    end

    JSON.parse(response.body)
  end

  def cluster_health
    response = request(:get, '/_cluster/health')
    unless response.is_a?(Net::HTTPSuccess)
      raise "Cluster health request failed: #{response.code} #{response.body}"
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

  def list_indices(pattern = '*')
    response = request(:get, '/_cat/indices', params: { format: 'json', index: pattern })
    
    unless response.is_a?(Net::HTTPSuccess)
      raise "Failed to list indices: #{response.code} #{response.body}"
    end

    JSON.parse(response.body).map { |idx| idx['index'] }.compact
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

class AirportLookup
  def initialize(airports_file:, logger:)
    @logger = logger
    @airports = {}
    load_airports(airports_file) if airports_file && File.exist?(airports_file)
  end

  def lookup_coordinates(iata_code)
    return nil if iata_code.nil? || iata_code.empty?

    airport = @airports[iata_code.upcase]
    return nil unless airport

    "#{airport[:lat]},#{airport[:lon]}"
  end

  private

  def load_airports(file_path)
    @logger.info("Loading airports from #{file_path}")

    count = 0
    File.open(file_path, 'rb') do |file|
      gz = Zlib::GzipReader.new(file)
      begin
        CSV.new(gz, headers: false).each do |row|
          # Columns: ID, Name, City, Country, IATA, ICAO, Lat, Lon, ...
          iata = row[4]&.strip
          next if iata.nil? || iata.empty? || iata == '\\N'

          lat_str = row[6]&.strip
          lon_str = row[7]&.strip
          next if lat_str.nil? || lat_str.empty? || lon_str.nil? || lon_str.empty?

          begin
            lat = Float(lat_str)
            lon = Float(lon_str)
            @airports[iata.upcase] = { lat: lat, lon: lon }
            count += 1
          rescue ArgumentError
            # Skip invalid coordinates
            next
          end
        end
      ensure
        gz.close
      end
    end

    @logger.info("Loaded #{count} airports into lookup table")
  end
end

class FlightLoader
  BATCH_SIZE = 500

  def initialize(client: nil, mapping:, index:, logger:, batch_size: BATCH_SIZE, refresh: false, airports_file: nil)
    @client = client
    @mapping = mapping
    @index_prefix = index # Store as prefix, e.g. 'flights'
    @logger = logger
    @batch_size = batch_size
    @refresh = refresh
    @airport_lookup = AirportLookup.new(airports_file: airports_file, logger: logger)
    @ensured_indices = Set.new # Track which indices we've already created
  end

  def ensure_index(index_name)
    return unless @client
    
    if @ensured_indices.include?(index_name)
      @logger.debug("Index #{index_name} already ensured in this session")
      return
    end
    
    if @client.index_exists?(index_name)
      @logger.info("Index #{index_name} already exists")
      @ensured_indices.add(index_name)
      return
    end

    @logger.info("Creating index: #{index_name}")
    @client.create_index(index_name, @mapping)
    @ensured_indices.add(index_name)
    @logger.info("Successfully created index: #{index_name}")
  end

  def import_files(files)
    files.each do |file_path|
      import_file(file_path)
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

  def import_file(file_path)
    unless File.file?(file_path)
      @logger.warn("Skipping #{file_path} (not a regular file)")
      return
    end

    @logger.info("Importing #{file_path}")

    # Extract year and month from filename if available
    file_year, file_month = extract_year_month_from_filename(file_path)

    # Buffer documents by index name (year-month)
    index_buffers = {} # { index_name => { lines: [], count: 0 } }
    indexed_docs = 0
    processed_rows = 0

    with_data_io(file_path) do |io|
      csv = CSV.new(io, headers: true, return_headers: false)

      csv.each do |row|
        processed_rows += 1
        
        # Debug: check if we have timestamp source (only log first time)
        if processed_rows == 1
          has_timestamp = row.headers.include?('@timestamp')
          has_flight_date = row.headers.include?('FlightDate')
          unless has_timestamp || has_flight_date
            @logger.warn("CSV headers don't include '@timestamp' or 'FlightDate'. Available headers: #{row.headers.first(10).join(', ')}")
          end
        end
        
        doc = transform_row(row)
        next if doc.nil? || doc.empty?

        # Extract index name from timestamp or filename (must be checked before compact! removes it)
        timestamp = doc['@timestamp']
        index_name = extract_index_name(timestamp, file_year: file_year, file_month: file_month)
        unless index_name
          timestamp_raw = row['@timestamp'] || row['FlightDate']
          @logger.warn("Skipping document - missing or invalid timestamp. Raw value: #{timestamp_raw.inspect}, parsed timestamp: #{timestamp.inspect}. Row #{processed_rows}: Origin=#{row['Origin']}, Dest=#{row['Dest']}, Airline=#{row['Reporting_Airline']}")
          next
        end

        # Now compact the document (removing nil values) since we've extracted what we need
        doc.compact!

        # Ensure index exists
        ensure_index(index_name)

        # Initialize buffer for this index if needed
        index_buffers[index_name] ||= { lines: [], count: 0 }

        # Add document to buffer
        buffer = index_buffers[index_name]
        buffer[:lines] << { index: { _index: index_name } }.to_json
        buffer[:lines] << doc.to_json
        buffer[:count] += 1

        # Flush if buffer is full
        if buffer[:count] >= @batch_size
          indexed_docs += flush_index(index_name, buffer[:lines], buffer[:count])
          buffer[:lines].clear
          buffer[:count] = 0
        end
      end
    end

    # Flush any remaining buffers
    index_buffers.each do |index_name, buffer|
      if buffer[:count].positive?
        indexed_docs += flush_index(index_name, buffer[:lines], buffer[:count])
      end
    end

    @logger.info("Finished #{file_path} (rows processed: #{processed_rows}, documents indexed: #{indexed_docs})")
  end

  def flush_index(index_name, lines, doc_count)
    @logger.info("Flushing #{doc_count} documents to index: #{index_name}")
    payload = lines.join("\n") + "\n"
    result = @client.bulk(index_name, payload, refresh: @refresh)

    if result['errors']
      errors = result.fetch('items', []).map { |item| item['index'] }.select { |info| info && info['error'] }
      errors.first(5).each do |error|
        @logger.error("Bulk item error for #{index_name}: #{error['error']}")
      end
      raise "Bulk indexing reported errors for #{index_name}; aborting"
    end

    doc_count
  rescue StandardError => e
    @logger.error("Bulk flush failed for #{index_name}: #{e.message}")
    raise
  end

  def with_data_io(file_path)
    if File.extname(file_path).downcase == '.zip'
      entry = csv_entry_in_zip(file_path)
      raise "No CSV entry found in #{file_path}" unless entry

      IO.popen(['unzip', '-p', file_path, entry], 'r', encoding: 'UTF-8') do |io|
        yield io
      end

      unless $CHILD_STATUS&.success?
        raise "Failed to read #{entry} from #{file_path} (exit status #{$CHILD_STATUS&.exitstatus})"
      end
    elsif file_path.downcase.end_with?('.gz')
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

  def csv_entry_in_zip(zip_path)
    stdout, stderr, status = Open3.capture3('unzip', '-Z1', zip_path)
    unless status.success?
      raise "Failed to list entries in #{zip_path}: #{stderr}"
    end

    stdout.split("\n").find { |line| line.downcase.end_with?('.csv') }
  end

  def extract_index_name(timestamp, file_year: nil, file_month: nil)
    # If filename specifies month, use that format: flights-<year>-<month>
    if file_year && file_month
      return "#{@index_prefix}-#{file_year}-#{file_month}"
    end

    # If filename specifies only year, use that format: flights-<year>
    if file_year
      return "#{@index_prefix}-#{file_year}"
    end

    # Otherwise, derive from timestamp
    return nil unless timestamp

    # Parse YYYY-MM-DD format and extract YYYY-MM or YYYY
    # If month was specified in filename but not found, we'd have used it above
    # So here we extract year-only or year-month based on what we have
    if timestamp =~ /^(\d{4})-(\d{2})-\d{2}/
      year = Regexp.last_match(1)
      month = Regexp.last_match(2)
      # Since filename didn't specify month, use year-only format
      "#{@index_prefix}-#{year}"
    else
      @logger.warn("Unable to parse timestamp format: #{timestamp}")
      nil
    end
  end

  def extract_year_month_from_filename(file_path)
    basename = File.basename(file_path)
    # Remove extensions (.gz, .csv, .zip) - handle multiple extensions like .csv.gz
    # Keep removing extensions until no more match
    loop do
      new_basename = basename.gsub(/\.(gz|csv|zip)$/i, '')
      break if new_basename == basename
      basename = new_basename
    end
    
    # Try pattern: flights-YYYY-MM (e.g., flights-2024-07)
    if basename =~ /-(\d{4})-(\d{2})$/
      year = Regexp.last_match(1)
      month = Regexp.last_match(2)
      return [year, month]
    end
    
    # Try pattern: flights-YYYY (e.g., flights-2019)
    if basename =~ /-(\d{4})$/
      year = Regexp.last_match(1)
      return [year, nil]
    end
    
    # No pattern matched
    [nil, nil]
  end

  def transform_row(row)
    doc = {}

    # Get timestamp - prefer @timestamp column if it exists, otherwise use FlightDate
    timestamp = present(row['@timestamp']) || present(row['FlightDate'])
    
    # Flight ID - construct from date, airline, flight number, origin, and destination
    # Extract date from timestamp if it's in YYYY-MM-DD format, otherwise use timestamp as-is
    flight_date = timestamp
    
    reporting_airline = present(row['Reporting_Airline'])
    flight_number = present(row['Flight_Number_Reporting_Airline'])
    origin = present(row['Origin'])
    dest = present(row['Dest'])
    
    if flight_date && reporting_airline && flight_number && origin && dest
      doc['FlightID'] = "#{flight_date}_#{reporting_airline}_#{flight_number}_#{origin}_#{dest}"
    end

    # @timestamp field - use timestamp directly (required for index routing)
    # Store it even if nil so we can detect missing dates and skip the document
    doc['@timestamp'] = timestamp

    # Direct mappings from CSV to mapping field names
    doc['Reporting_Airline'] = reporting_airline
    doc['Tail_Number'] = present(row['Tail_Number'])
    doc['Flight_Number'] = flight_number
    doc['Origin'] = origin
    doc['Dest'] = dest

    # Time fields - convert to integers (minutes or time in HHMM format)
    doc['CRSDepTimeLocal'] = to_integer(row['CRSDepTime'])
    doc['DepDelayMin'] = to_integer(row['DepDelay'])
    doc['TaxiOutMin'] = to_integer(row['TaxiOut'])
    doc['TaxiInMin'] = to_integer(row['TaxiIn'])
    doc['CRSArrTimeLocal'] = to_integer(row['CRSArrTime'])
    doc['ArrDelayMin'] = to_integer(row['ArrDelay'])

    # Boolean fields
    doc['Cancelled'] = to_boolean(row['Cancelled'])
    doc['Diverted'] = to_boolean(row['Diverted'])

    # Cancellation code
    doc['CancellationCode'] = present(row['CancellationCode'])

    # Time duration fields (convert to minutes as integers)
    doc['ActualElapsedTimeMin'] = to_integer(row['ActualElapsedTime'])
    doc['AirTimeMin'] = to_integer(row['AirTime'])

    # Count and distance
    doc['Flights'] = to_integer(row['Flights'])
    doc['DistanceMiles'] = to_integer(row['Distance'])

    # Delay fields (with Min suffix to match mapping)
    doc['CarrierDelayMin'] = to_integer(row['CarrierDelay'])
    doc['WeatherDelayMin'] = to_integer(row['WeatherDelay'])
    doc['NASDelayMin'] = to_integer(row['NASDelay'])
    doc['SecurityDelayMin'] = to_integer(row['SecurityDelay'])
    doc['LateAircraftDelayMin'] = to_integer(row['LateAircraftDelay'])

    # Geo point fields - lookup from airports data
    origin_location = @airport_lookup.lookup_coordinates(origin)
    doc['OriginLocation'] = origin_location if origin_location

    dest_location = @airport_lookup.lookup_coordinates(dest)
    doc['DestLocation'] = dest_location if dest_location

    # Don't compact here - we need @timestamp to stay even if nil so we can detect missing dates
    # compact! will be called in import_file after we extract the index name
    doc
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

  def to_boolean(value)
    value = present(value)
    return nil unless value

    return true if %w[true t yes y].include?(value.downcase)
    return false if %w[false f no n].include?(value.downcase)

    numeric = Float(value) rescue nil
    return nil if numeric.nil?

    numeric.positive?
  end

  def classify_delay(group_value)
    group = to_integer(group_value)
    return nil if group.nil?

    case group
    when -1 then 'early_or_ontime'
    when 0 then 'late_0_14'
    when 1 then 'late_15_29'
    when 2 then 'late_30_44'
    when 3 then 'late_45_59'
    when 4 then 'late_60_74'
    when 5 then 'late_75_89'
    when 6 then 'late_90_104'
    when 7 then 'late_105_119'
    when 8 then 'late_120_134'
    when 9 then 'late_135_149'
    when 10 then 'late_150_plus'
    else
      'unknown'
    end
  end
end

def parse_options(argv)
  options = {
    config: 'config/elasticsearch.yml',
    mapping: 'mappings-flights.json',
    data_dir: 'data',
    index: 'flights',
    batch_size: FlightLoader::BATCH_SIZE,
    refresh: false,
    status: false,
    delete_index: false,
    delete_all: false,
    sample: false,
    airports_file: 'data/airports.csv.gz'
  }

  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: import_flights.rb [options]'

    opts.on('-c', '--config PATH', 'Path to Elasticsearch config YAML (default: config/elasticsearch.yml)') do |path|
      options[:config] = path
    end

    opts.on('-m', '--mapping PATH', 'Path to mappings JSON (default: mappings-flights.json)') do |path|
      options[:mapping] = path
    end

    opts.on('-d', '--data-dir PATH', 'Directory containing data files (default: data)') do |path|
      options[:data_dir] = path
    end

    opts.on('-f', '--file PATH', 'Only import the specified file') do |path|
      options[:file] = path
    end

    opts.on('-a', '--all', 'Import all files found in the data directory') do
      options[:all] = true
    end

    opts.on('-g', '--glob PATTERN', 'Import files matching the glob pattern') do |pattern|
      options[:glob] = pattern
    end

    opts.on('--index NAME', "Override index name (default: flights)") do |name|
      options[:index] = name
    end

    opts.on('--batch-size N', Integer, 'Number of documents per bulk request (default: 1000)') do |size|
      options[:batch_size] = size
    end

    opts.on('--refresh', 'Request an index refresh after each bulk request') do
      options[:refresh] = true
    end

    opts.on('--status', 'Test connection and print cluster health status') do
      options[:status] = true
    end

    opts.on('--delete-index', 'Delete indices matching the index pattern and exit') do
      options[:delete_index] = true
    end

    opts.on('--delete-all', 'Delete all flights-* indices and exit') do
      options[:delete_all] = true
    end

    opts.on('--sample', 'Print the first document and exit') do
      options[:sample] = true
    end

    opts.on('-h', '--help', 'Show this help message') do
      puts opts
      exit
    end
  end

  remaining_args = parser.parse!(argv)

  # If -g was used and there are leftover arguments, the shell likely expanded the glob
  # Collect all arguments (the pattern + any remaining) as expanded file paths
  if options[:glob] && !remaining_args.empty?
    # If the glob pattern doesn't contain wildcards, it was likely expanded by the shell
    unless options[:glob].include?('*') || options[:glob].include?('?')
      # Treat the glob pattern and all remaining args as expanded file paths
      options[:glob_files] = [options[:glob]] + remaining_args
      options[:glob] = nil
    end
  end

  if options[:status] && (options[:delete_index] || options[:delete_all])
    warn 'Cannot use --status with --delete-index or --delete-all'
    exit 1
  end

  if options[:delete_index] && options[:delete_all]
    warn 'Cannot use --delete-index and --delete-all together'
    exit 1
  end

  unless options[:status] || options[:delete_index] || options[:delete_all] || options[:sample]
    selection_options = [options[:file], options[:all], options[:glob], options[:glob_files]].compact
    if selection_options.length > 1
      warn 'Cannot use --file, --all, and --glob together (use only one)'
      exit 1
    end

    unless options[:file] || options[:all] || options[:glob] || options[:glob_files]
      warn 'Please provide either --file PATH, --all, or --glob PATTERN'
      exit 1
    end
  end

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

def load_mapping(path)
  JSON.parse(File.read(path))
rescue Errno::ENOENT
  raise "Mapping file not found: #{path}"
end

def files_to_process(options)
  if options[:file]
    [resolve_file_path(options[:file], options[:data_dir])]
  elsif options[:glob_files]
    # Shell-expanded glob: use the file paths directly
    files = options[:glob_files].map { |f| resolve_file_path(f, options[:data_dir]) }
    files.select { |f| File.file?(f) }.sort
  elsif options[:glob]
    # Resolve glob pattern - try as-is first, then relative to data_dir
    glob_pattern = options[:glob]
    
    # If absolute path, use as-is
    if glob_pattern.start_with?('/')
      files = Dir.glob(glob_pattern).select { |f| File.file?(f) }.sort
    else
      # Try the pattern as-is first (in case it's relative to current directory)
      files = Dir.glob(glob_pattern).select { |f| File.file?(f) }
      if files.empty?
        # If no matches, try relative to data_dir
        expanded_pattern = File.join(options[:data_dir], glob_pattern)
        files = Dir.glob(expanded_pattern).select { |f| File.file?(f) }
      end
      files = files.sort
    end
    
    if files.empty?
      raise "No files found matching pattern: #{glob_pattern}"
    end
    files
  else
    pattern_zip = File.join(options[:data_dir], '*.zip')
    pattern_csv = File.join(options[:data_dir], '*.csv')
    pattern_csv_gz = File.join(options[:data_dir], '*.csv.gz')
    files = Dir.glob([pattern_zip, pattern_csv, pattern_csv_gz]).sort
    if files.empty?
      raise "No .zip, .csv, or .csv.gz files found in #{options[:data_dir]}"
    end
    files
  end
end

def resolve_file_path(path, data_dir)
  expanded = File.expand_path(path)
  return expanded if File.exist?(expanded)

  candidate = File.expand_path(File.join(data_dir, path))
  return candidate if File.exist?(candidate)

  raise "File not found: #{path}"
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
    delete_indices_by_pattern(client, logger, options[:index])
    return
  end

  if options[:delete_all]
    delete_indices_by_pattern(client, logger, 'flights-*')
    return
  end

  mapping = load_mapping(options[:mapping])
  loader = FlightLoader.new(
    client: client,
    mapping: mapping,
    index: options[:index],
    logger: logger,
    batch_size: options[:batch_size],
    refresh: options[:refresh],
    airports_file: options[:airports_file]
  )

  files = files_to_process(options)
  loader.import_files(files)
end

def sample_document(options:, logger:)
  mapping = load_mapping(options[:mapping])
  loader = FlightLoader.new(
    client: nil,
    mapping: mapping,
    index: 'flights',
    logger: logger,
    batch_size: 1,
    refresh: false,
    airports_file: options[:airports_file]
  )

  files = files_to_process(options)
  if files.empty?
    logger.error('No files found to sample')
    exit 1
  end

  doc = loader.sample_document(files.first)
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

def delete_indices_by_pattern(client, logger, pattern)
  pattern_with_wildcard = pattern.end_with?('*') ? pattern : "#{pattern}-*"
  logger.info("Searching for indices matching pattern: #{pattern_with_wildcard}")
  
  deleted = client.delete_indices_by_pattern(pattern_with_wildcard)
  
  if deleted.empty?
    logger.warn("No indices found matching pattern: #{pattern_with_wildcard}")
  else
    logger.info("Deleted #{deleted.length} index(es): #{deleted.join(', ')}")
  end
rescue StandardError => e
  logger.error("Failed to delete indices matching pattern '#{pattern}': #{e.message}")
  exit 1
end

if $PROGRAM_NAME == __FILE__
  main(ARGV)
end
