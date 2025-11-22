#!/usr/bin/env ruby
# frozen_string_literal: true

require 'optparse'
require 'csv'
require 'logger'
require 'zlib'
require 'pathname'

class CSVValidator
  def initialize(logger:)
    @logger = logger
    @errors = []
    @warnings = []
    @stats = {
      total_rows: 0,
      valid_rows: 0,
      invalid_rows: 0,
      empty_rows: 0,
      column_count: nil,
      headers: nil
    }
  end

  def validate(file_path, options = {})
    @logger.info("Validating CSV file: #{file_path}")

    # Check file exists and is readable
    unless File.exist?(file_path)
      @errors << "File does not exist: #{file_path}"
      report_results(options)
      return false
    end

    unless File.readable?(file_path)
      @errors << "File is not readable: #{file_path}"
      report_results(options)
      return false
    end

    # Check file is not empty
    if File.size(file_path).zero?
      @errors << "File is empty: #{file_path}"
      report_results(options)
      return false
    end

    # Validate CSV structure
    begin
      validate_csv_structure(file_path, options)
    rescue StandardError => e
      @errors << "Error reading CSV file: #{e.message}"
      @logger.error("Exception: #{e.class}: #{e.message}")
      @logger.error(e.backtrace.first(5).join("\n")) if options[:verbose]
      report_results(options)
      return false
    end

    # Report results
    report_results(options)

    @errors.empty?
  end

  def valid?
    @errors.empty?
  end

  def errors
    @errors.dup
  end

  def warnings
    @warnings.dup
  end

  def stats
    @stats.dup
  end

  private

  def validate_csv_structure(file_path, options)
    has_headers = options.fetch(:has_headers, true)
    sample_size = options.fetch(:sample_rows, nil)
    check_consistency = options.fetch(:check_consistency, true)

    with_data_io(file_path) do |io|
      csv_options = {
        headers: has_headers,
        return_headers: false,
        liberal_parsing: options.fetch(:liberal_parsing, false)
      }

      csv = CSV.new(io, **csv_options)

      # Read first row to establish baseline
      first_row = csv.first
      if first_row.nil?
        @errors << 'CSV file contains no data rows'
        return
      end

      @stats[:total_rows] = 1
      @stats[:valid_rows] = 1

      if has_headers && first_row.is_a?(CSV::Row)
        @stats[:headers] = first_row.headers
        @stats[:column_count] = first_row.length
        @logger.info("Found #{@stats[:column_count]} columns: #{@stats[:headers].first(10).join(', ')}#{@stats[:headers].length > 10 ? '...' : ''}")
      else
        @stats[:column_count] = first_row.length
        @logger.info("Found #{@stats[:column_count]} columns (no headers)")
      end

      # Validate remaining rows
      rows_to_check = sample_size || Float::INFINITY
      rows_checked = 0

      csv.each do |row|
        rows_checked += 1
        break if rows_checked >= rows_to_check

        @stats[:total_rows] += 1

        # Check if row is empty
        if row.nil? || (row.is_a?(Array) && row.all?(&:nil?)) || (row.is_a?(CSV::Row) && row.to_a.all?(&:nil?))
          @stats[:empty_rows] += 1
          @warnings << "Row #{@stats[:total_rows]} is empty"
          next
        end

        # Check column count consistency
        if check_consistency
          row_length = row.is_a?(CSV::Row) ? row.length : row.size
          if row_length != @stats[:column_count]
            @stats[:invalid_rows] += 1
            @errors << "Row #{@stats[:total_rows]} has #{row_length} columns, expected #{@stats[:column_count]}"
            next unless options[:continue_on_error]
          end
        end

        @stats[:valid_rows] += 1
      end

      if sample_size && rows_checked >= sample_size
        @logger.info("Sampled first #{sample_size} rows for validation")
      end
    end
  rescue CSV::MalformedCSVError => e
    @errors << "Malformed CSV: #{e.message}"
    @logger.error("CSV parsing error at line #{e.line_number}: #{e.message}") if e.respond_to?(:line_number)
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

  def report_results(options)
    @logger.info('=' * 60)
    @logger.info('Validation Results')
    @logger.info('=' * 60)
    @logger.info("Total rows checked: #{@stats[:total_rows]}")
    @logger.info("Valid rows: #{@stats[:valid_rows]}")
    @logger.info("Invalid rows: #{@stats[:invalid_rows]}")
    @logger.info("Empty rows: #{@stats[:empty_rows]}")

    if @stats[:column_count]
      @logger.info("Column count: #{@stats[:column_count]}")
    end

    if @stats[:headers]
      @logger.info("Headers: #{@stats[:headers].join(', ')}")
    end

    if @errors.any?
      @logger.error('=' * 60)
      @logger.error("Errors (#{@errors.length}):")
      @errors.each { |error| @logger.error("  - #{error}") }
    end

    if @warnings.any?
      @logger.warn('=' * 60)
      @logger.warn("Warnings (#{@warnings.length}):")
      @warnings.first(20).each { |warning| @logger.warn("  - #{warning}") }
      if @warnings.length > 20
        @logger.warn("  ... and #{@warnings.length - 20} more warnings")
      end
    end

    @logger.info('=' * 60)

    if @errors.empty? && @warnings.empty?
      @logger.info('✓ CSV file is valid!')
    elsif @errors.empty?
      @logger.warn('⚠ CSV file has warnings but no errors')
    else
      @logger.error('✗ CSV file has errors')
    end
  end
end

def parse_options(argv)
  options = {
    has_headers: true,
    check_consistency: true,
    continue_on_error: false,
    liberal_parsing: false,
    sample_rows: nil,
    verbose: false
  }

  parser = OptionParser.new do |opts|
    opts.banner = 'Usage: validate_csv.rb [options] FILE'

    opts.on('--no-headers', 'CSV file does not have a header row') do
      options[:has_headers] = false
    end

    opts.on('--no-consistency-check', 'Skip column count consistency checks') do
      options[:check_consistency] = false
    end

    opts.on('--continue-on-error', 'Continue validation even when errors are found') do
      options[:continue_on_error] = true
    end

    opts.on('--liberal-parsing', 'Use liberal CSV parsing (more forgiving)') do
      options[:liberal_parsing] = true
    end

    opts.on('--sample N', Integer, 'Only validate first N rows (default: validate all)') do |n|
      options[:sample_rows] = n
    end

    opts.on('-v', '--verbose', 'Show verbose output including stack traces') do
      options[:verbose] = true
    end

    opts.on('-h', '--help', 'Show this help message') do
      puts opts
      exit
    end
  end

  remaining_args = parser.parse!(argv)

  if remaining_args.empty?
    warn 'Error: CSV file path is required'
    warn parser
    exit 1
  end

  if remaining_args.length > 1
    warn 'Error: Only one CSV file can be validated at a time'
    warn parser
    exit 1
  end

  options[:file] = remaining_args.first
  options
end

def resolve_file_path(path)
  # If path is absolute, use as-is
  return path if Pathname.new(path).absolute?

  # Try relative to current directory first
  return path if File.exist?(path)

  # Try relative to workspace root
  script_dir = File.dirname(File.expand_path(__FILE__))
  workspace_root = File.expand_path(File.join(script_dir, '..'))
  candidate = File.expand_path(File.join(workspace_root, path))

  return candidate if File.exist?(candidate)

  # Return the resolved path even if it doesn't exist (let validator report the error)
  candidate
end

def build_logger(verbose: false)
  logger = Logger.new($stdout)
  logger.level = verbose ? Logger::DEBUG : Logger::INFO
  logger.formatter = proc do |severity, datetime, _progname, msg|
    "#{severity}: #{msg}\n"
  end
  logger
end

def main(argv)
  options = parse_options(argv)
  logger = build_logger(verbose: options[:verbose])

  file_path = resolve_file_path(options[:file])

  validator = CSVValidator.new(logger: logger)

  validation_options = {
    has_headers: options[:has_headers],
    check_consistency: options[:check_consistency],
    continue_on_error: options[:continue_on_error],
    liberal_parsing: options[:liberal_parsing],
    sample_rows: options[:sample_rows],
    verbose: options[:verbose]
  }

  is_valid = validator.validate(file_path, validation_options)

  exit(is_valid ? 0 : 1)
end

if $PROGRAM_NAME == __FILE__
  main(ARGV)
end
