#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'csv'
require 'open3'
require 'fileutils'
require 'zlib'

class FieldExtractor
  def initialize(fields_json_path, input_dir, output_dir)
    @fields_json_path = fields_json_path
    @input_dir = input_dir
    @output_dir = output_dir
    @fields_to_extract = load_fields
  end

  def extract_all
    ensure_output_directory

    zip_files = sort_files_by_year_month(Dir.glob(File.join(@input_dir, '*.zip')))

    if zip_files.empty?
      warn "No zip files found in #{@input_dir}"
      return
    end

    puts "Found #{zip_files.length} zip file(s) to process"
    puts "Extracting fields: #{@fields_to_extract.join(', ')}"
    puts

    # Group files by year
    files_by_year = group_files_by_year(zip_files)

    puts "Spawning #{files_by_year.keys.length} process(es) to process #{files_by_year.keys.length} year(s)..."
    puts

    # Spawn a process for each year
    pids = []
    files_by_year.each do |year, files|
      pid = Process.fork do
        extract_year(year, files)
      end
      pids << pid
      puts "Started process #{pid} for year #{year}"
    end

    # Wait for all processes to complete
    pids.each do |pid|
      Process.wait(pid)
      status = $CHILD_STATUS
      if status.success?
        puts "Process #{pid} completed successfully"
      else
        warn "Process #{pid} exited with status #{status.exitstatus}"
      end
    end

    puts
    puts "Done! All processes completed"
  end

  def extract_year(year, files)
    output_path = File.join(@output_dir, "flights-#{year}.csv.gz")
    $stdout.puts "[Year #{year}] Starting processing of #{files.length} file(s)"
    $stdout.flush

    header_written = false
    year_rows = 0

    File.open(output_path, 'wb') do |file|
      Zlib::GzipWriter.wrap(file) do |gzip|
        files.each_with_index do |zip_path, index|
          $stdout.puts "[Year #{year}] [#{index + 1}/#{files.length}] Processing #{File.basename(zip_path)}"
          $stdout.flush
          rows_extracted = extract_from_zip(zip_path, gzip, header_written)
          year_rows += rows_extracted
          header_written = true if rows_extracted > 0
        end
      end
    end

    $stdout.puts "[Year #{year}] Completed: Extracted #{year_rows} rows to #{output_path}"
    $stdout.flush
  end

  def extract_year_only(year)
    ensure_output_directory

    zip_files = sort_files_by_year_month(Dir.glob(File.join(@input_dir, '*.zip')))

    if zip_files.empty?
      warn "No zip files found in #{@input_dir}"
      return
    end

    # Filter files by year
    matching_files = filter_files_by_year(zip_files, year)

    if matching_files.empty?
      warn "No zip files found for year #{year}"
      return
    end

    output_path = File.join(@output_dir, "flights-#{year}.csv.gz")
    puts "Found #{matching_files.length} zip file(s) to process for #{year}"
    puts "Extracting fields: #{@fields_to_extract.join(', ')}"
    puts
    puts "[Year #{year}] Starting processing of #{matching_files.length} file(s)"
    $stdout.flush

    header_written = false
    rows_count = 0

    File.open(output_path, 'wb') do |file|
      Zlib::GzipWriter.wrap(file) do |gzip|
        matching_files.each_with_index do |zip_path, index|
          $stdout.puts "[Year #{year}] [#{index + 1}/#{matching_files.length}] Processing #{File.basename(zip_path)}"
          $stdout.flush
          rows_extracted = extract_from_zip(zip_path, gzip, header_written)
          rows_count += rows_extracted
          header_written = true if rows_extracted > 0
        end
      end
    end

    $stdout.puts "[Year #{year}] Completed: Extracted #{rows_count} rows to #{output_path}"
    $stdout.flush
  end

  def extract_year_month(year, month)
    ensure_output_directory

    zip_files = sort_files_by_year_month(Dir.glob(File.join(@input_dir, '*.zip')))

    if zip_files.empty?
      warn "No zip files found in #{@input_dir}"
      return
    end

    # Filter files by year and month
    matching_files = filter_files_by_year_month(zip_files, year, month)

    if matching_files.empty?
      warn "No zip files found for year #{year}, month #{month}"
      return
    end

    output_path = File.join(@output_dir, "flights-#{year}-#{month.to_s.rjust(2, '0')}.csv.gz")
    puts "Found #{matching_files.length} zip file(s) to process for #{year}-#{month.to_s.rjust(2, '0')}"
    puts "Extracting fields: #{@fields_to_extract.join(', ')}"
    puts
    puts "[Year #{year}, Month #{month}] Starting processing of #{matching_files.length} file(s)"
    $stdout.flush

    header_written = false
    rows_count = 0

    File.open(output_path, 'wb') do |file|
      Zlib::GzipWriter.wrap(file) do |gzip|
        matching_files.each_with_index do |zip_path, index|
          $stdout.puts "[Year #{year}, Month #{month}] [#{index + 1}/#{matching_files.length}] Processing #{File.basename(zip_path)}"
          $stdout.flush
          rows_extracted = extract_from_zip(zip_path, gzip, header_written)
          rows_count += rows_extracted
          header_written = true if rows_extracted > 0
        end
      end
    end

    $stdout.puts "[Year #{year}, Month #{month}] Completed: Extracted #{rows_count} rows to #{output_path}"
    $stdout.flush
  end

  private

  def sort_files_by_year_month(zip_files)
    zip_files.sort_by do |zip_path|
      year_month = extract_year_month_from_zip(zip_path)
      if year_month
        year, month = year_month
        [year, month]
      else
        # If we can't extract year/month, put at the end
        [9999, 99]
      end
    end
  end

  def group_files_by_year(zip_files)
    files_by_year = {}

    zip_files.each do |zip_path|
      year = extract_year_from_zip(zip_path)
      if year
        files_by_year[year] ||= []
        files_by_year[year] << zip_path
      else
        warn "Warning: Could not determine year from #{File.basename(zip_path)}, skipping"
      end
    end

    # Sort files within each year by month
    files_by_year.each do |year, files|
      files_by_year[year] = files.sort_by do |zip_path|
        year_month = extract_year_month_from_zip(zip_path)
        if year_month
          year_month[1] # month
        else
          99 # put at end if we can't extract month
        end
      end
    end

    files_by_year
  end

  def extract_year_from_zip(zip_path)
    # Try to extract year from filename first
    # Pattern: On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2019_1.zip
    # or: ..._2024_1.zip
    filename = File.basename(zip_path, '.zip')
    
    # Look for 4-digit year pattern (1987-present format has year before month number)
    # Match pattern like: _2019_1 or _2024_1
    if filename =~ /_(\d{4})_\d+$/
      return $1
    end

    # Fallback: try to read year from first row's FlightDate field
    extract_year_from_first_row(zip_path)
  end

  def extract_year_month_from_zip(zip_path)
    # Try to extract year and month from filename first
    # Pattern: On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2019_1.zip
    # or: ..._2024_1.zip
    filename = File.basename(zip_path, '.zip')
    
    # Look for 4-digit year and month pattern
    # Match pattern like: _2019_1 or _2024_12
    if filename =~ /_(\d{4})_(\d+)$/
      return [$1.to_i, $2.to_i]
    end

    # Fallback: try to read year and month from first row's FlightDate field
    extract_year_month_from_first_row(zip_path)
  end

  def filter_files_by_year(zip_files, target_year)
    zip_files.select do |zip_path|
      year = extract_year_from_zip(zip_path)
      if year
        year.to_i == target_year
      else
        false
      end
    end
  end

  def filter_files_by_year_month(zip_files, target_year, target_month)
    zip_files.select do |zip_path|
      year_month = extract_year_month_from_zip(zip_path)
      if year_month
        year, month = year_month
        year == target_year && month == target_month
      else
        false
      end
    end
  end

  def extract_year_from_first_row(zip_path)
    csv_entry = csv_entry_in_zip(zip_path)
    return nil unless csv_entry

    IO.popen(['unzip', '-p', zip_path, csv_entry], 'r', encoding: 'UTF-8') do |input_io|
      # Read header
      headers_line = input_io.readline
      return nil unless headers_line
      
      headers = CSV.parse_line(headers_line, encoding: 'UTF-8')
      flight_date_idx = headers.index('FlightDate')
      return nil unless flight_date_idx
      
      # Read first data row
      first_line = input_io.readline
      return nil unless first_line

      row = CSV.parse_line(first_line, encoding: 'UTF-8')
      flight_date = row[flight_date_idx] if row && flight_date_idx < row.length
      return nil unless flight_date

      # FlightDate format is YYYY-MM-DD
      if flight_date =~ /^(\d{4})-\d{2}-\d{2}/
        return $1
      end
    end

    nil
  rescue StandardError
    nil
  end

  def extract_year_month_from_first_row(zip_path)
    csv_entry = csv_entry_in_zip(zip_path)
    return nil unless csv_entry

    IO.popen(['unzip', '-p', zip_path, csv_entry], 'r', encoding: 'UTF-8') do |input_io|
      # Read header
      headers_line = input_io.readline
      return nil unless headers_line
      
      headers = CSV.parse_line(headers_line, encoding: 'UTF-8')
      flight_date_idx = headers.index('FlightDate')
      return nil unless flight_date_idx
      
      # Read first data row
      first_line = input_io.readline
      return nil unless first_line

      row = CSV.parse_line(first_line, encoding: 'UTF-8')
      flight_date = row[flight_date_idx] if row && flight_date_idx < row.length
      return nil unless flight_date

      # FlightDate format is YYYY-MM-DD
      if flight_date =~ /^(\d{4})-(\d{2})-\d{2}/
        return [$1.to_i, $2.to_i]
      end
    end

    nil
  rescue StandardError
    nil
  end

  def load_fields
    fields_data = JSON.parse(File.read(@fields_json_path))
    fields_data.keys
  rescue Errno::ENOENT
    raise "Fields JSON file not found: #{@fields_json_path}"
  rescue JSON::ParserError => e
    raise "Failed to parse fields JSON: #{e.message}"
  end

  def ensure_output_directory
    FileUtils.mkdir_p(@output_dir)
  end

  def extract_from_zip(zip_path, gzip, header_written)
    csv_entry = csv_entry_in_zip(zip_path)
    unless csv_entry
      warn "  Warning: No CSV entry found in #{zip_path}, skipping"
      return 0
    end

    row_count = 0

    begin
      IO.popen(['unzip', '-p', zip_path, csv_entry], 'r', encoding: 'UTF-8:UTF-8') do |input_io|
        # Read header line first
        begin
          header_line = input_io.readline
        rescue EOFError
          warn "  Warning: Empty file #{zip_path}, skipping"
          return 0
        end

        begin
          headers = CSV.parse_line(header_line, encoding: 'UTF-8')
        rescue CSV::MalformedCSVError => e
          warn "  Warning: Malformed header in #{zip_path}: #{e.message}"
          return 0
        end

        # Fields required for FlightID generation
        flight_id_fields = ['FlightDate', 'Reporting_Airline', 'Flight_Number_Reporting_Airline', 'Origin', 'Dest']
        
        # Verify which fields exist in the CSV (including FlightID required fields)
        all_fields_needed = (@fields_to_extract + flight_id_fields).uniq
        fields_to_use = all_fields_needed.select do |field_name|
          headers.include?(field_name)
        end

        # Only warn about missing fields on the first file
        unless header_written
          missing_fields = @fields_to_extract - fields_to_use
          missing_fields.each do |field_name|
            $stderr.puts "  Warning: Field '#{field_name}' not found in CSV headers"
            $stderr.flush
          end

          # Build output header: replace FlightDate with @timestamp and add FlightID
          output_fields = fields_to_use.map do |field_name|
            field_name == 'FlightDate' ? '@timestamp' : field_name
          end
          
          # Add FlightID after @timestamp if @timestamp exists, otherwise at the beginning
          if output_fields.include?('@timestamp')
            timestamp_idx = output_fields.index('@timestamp')
            output_fields.insert(timestamp_idx + 1, 'FlightID')
          else
            output_fields.insert(0, 'FlightID')
          end

          # Write header row
          begin
            gzip.write(CSV.generate_line(output_fields))
          rescue Zlib::BufError, IOError => e
            warn "  Error writing header: #{e.class} - #{e.message}"
            raise
          end
        end

        # Get indices of fields to extract
        field_indices = fields_to_use.map { |field_name| headers.index(field_name) }
        
        # Get indices for FlightID calculation
        flight_id_indices = {
          flight_date: headers.index('FlightDate'),
          reporting_airline: headers.index('Reporting_Airline'),
          flight_number: headers.index('Flight_Number_Reporting_Airline'),
          origin: headers.index('Origin'),
          dest: headers.index('Dest')
        }
        
        # Find @timestamp position in output (which replaces FlightDate)
        output_fields_with_flight_id = fields_to_use.map { |f| f == 'FlightDate' ? '@timestamp' : f }
        timestamp_idx = output_fields_with_flight_id.index('@timestamp')
        flight_id_position = timestamp_idx ? timestamp_idx + 1 : 0

        # Process data rows
        begin
          input_io.each_line do |line|
            begin
              # Skip empty lines
              next if line.nil? || line.strip.empty?
              
              row = CSV.parse_line(line, encoding: 'UTF-8')
              next unless row && row.length > 0

              extracted_row = field_indices.map do |idx|
                idx && idx < row.length ? row[idx] : nil
              end
              
              # Generate FlightID from the original row values
              flight_date = flight_id_indices[:flight_date] && flight_id_indices[:flight_date] < row.length ? row[flight_id_indices[:flight_date]] : nil
              reporting_airline = flight_id_indices[:reporting_airline] && flight_id_indices[:reporting_airline] < row.length ? row[flight_id_indices[:reporting_airline]] : nil
              flight_number = flight_id_indices[:flight_number] && flight_id_indices[:flight_number] < row.length ? row[flight_id_indices[:flight_number]] : nil
              origin = flight_id_indices[:origin] && flight_id_indices[:origin] < row.length ? row[flight_id_indices[:origin]] : nil
              dest = flight_id_indices[:dest] && flight_id_indices[:dest] < row.length ? row[flight_id_indices[:dest]] : nil
              
              flight_id = [flight_date, reporting_airline, flight_number, origin, dest].compact.join('-')
              
              # Build final output row: values remain the same, just insert FlightID at the correct position
              output_row = extracted_row.dup
              
              # Insert FlightID at the correct position
              output_row.insert(flight_id_position, flight_id)
              
              begin
                csv_line = CSV.generate_line(output_row)
                gzip.write(csv_line)
                row_count += 1
              rescue Zlib::BufError, IOError => e
                $stderr.puts "  Warning: Error writing row to gzip: #{e.class} - #{e.message}"
                $stderr.flush
                # Try to continue - the gzip stream might recover
                next
              end
            rescue CSV::MalformedCSVError => e
              # Skip malformed rows but continue processing
              $stderr.puts "  Warning: Skipping malformed row: #{e.message}"
              $stderr.flush
              next
            rescue StandardError => e
              # Skip rows that cause other errors
              $stderr.puts "  Warning: Error processing row: #{e.class} - #{e.message}"
              $stderr.flush
              next
            end
          end
        rescue EOFError
          # Expected when file ends
        rescue IOError => e
          warn "  Warning: IO error while reading #{zip_path}: #{e.message}"
          # Continue to check exit status
        end
      end

      unless $CHILD_STATUS&.success?
        raise "Failed to read #{csv_entry} from #{zip_path} (exit status #{$CHILD_STATUS&.exitstatus})"
      end
    rescue StandardError => e
      warn "  Error processing #{zip_path}: #{e.class} - #{e.message}"
      if ENV['DEBUG']
        warn e.backtrace
      end
      raise
    end

    $stdout.puts "  Extracted #{row_count} rows"
    $stdout.flush
    row_count
  end

  def csv_entry_in_zip(zip_path)
    stdout, stderr, status = Open3.capture3('unzip', '-Z1', zip_path)
    unless status.success?
      raise "Failed to list entries in #{zip_path}: #{stderr}"
    end

    stdout.split("\n").find { |line| line.downcase.end_with?('.csv') }
  end
end

def show_help
  puts <<~HELP
    #{File.basename($PROGRAM_NAME)} - Extract flight data fields from BTS On-Time Performance archives

    SYNOPSIS
        #{$PROGRAM_NAME} [OPTIONS] <year> [month]

    DESCRIPTION
        Extracts specified fields from flight data zip files and outputs them to a
        compressed CSV file. The fields to extract are defined in data/sample-flight.json.

        Source files are read from ~/data/flights/raw/
        Output files are written to the workspace data/ directory.

    ARGUMENTS
        year                4-digit year (e.g., 2024)
                            Valid range: 1900-2100

        month               Optional month number (1-12)
                            If omitted, extracts all months for the specified year

    OPTIONS
        -h, --help          Display this help message and exit

    OUTPUT FILES
        When only year is specified:
            data/flights-YYYY.csv.gz

        When year and month are specified:
            data/flights-YYYY-MM.csv.gz

    EXAMPLES
        Extract all flights for 2024:
            #{$PROGRAM_NAME} 2024

        Extract flights for January 2024:
            #{$PROGRAM_NAME} 2024 1

        Extract flights for December 2023:
            #{$PROGRAM_NAME} 2023 12

    SEE ALSO
        data/sample-flight.json     Defines which fields to extract from the flight data
  HELP
end

def main(argv)
  # Check for help flag
  if argv.empty? || argv.include?('--help') || argv.include?('-h')
    show_help
    exit 0
  end

  # Parse arguments
  if argv.length < 1
    warn "Usage: #{$PROGRAM_NAME} <year> [month]"
    warn "Run '#{$PROGRAM_NAME} --help' for more information."
    exit 1
  end

  year = argv[0].to_i

  if year < 1900 || year > 2100
    warn "Error: Invalid year #{year}. Expected a year between 1900 and 2100."
    exit 1
  end

  # Resolve paths relative to the workspace root (two levels up from bin/ruby/)
  workspace_root = File.expand_path('../..', __dir__)
  
  fields_json_path = File.join(workspace_root, 'data', 'sample-flight.json')
  input_dir = File.expand_path('~/data/flights/raw/')
  output_dir = File.join(workspace_root, 'data')

  extractor = FieldExtractor.new(fields_json_path, input_dir, output_dir)

  if argv.length >= 2
    # Month specified
    month = argv[1].to_i

    if month < 1 || month > 12
      warn "Error: Invalid month #{month}. Expected a month between 1 and 12."
      exit 1
    end

    extractor.extract_year_month(year, month)
  else
    # Only year specified
    extractor.extract_year_only(year)
  end
rescue StandardError => e
  warn "Error: #{e.class} - #{e.message}"
  warn e.backtrace if ENV['DEBUG']
  exit 1
end

if $PROGRAM_NAME == __FILE__
  main(ARGV)
end
