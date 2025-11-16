#!/usr/bin/env bash

./cli-ruby/import_flights.rb --file data/flights-2019.csv.gz 2>&1 >/dev/null &
./cli-ruby/import_flights.rb --file data/flights-2020.csv.gz 2>&1 >/dev/null &
./cli-ruby/import_flights.rb --file data/flights-2021.csv.gz 2>&1 >/dev/null &
./cli-ruby/import_flights.rb --file data/flights-2022.csv.gz 2>&1 >/dev/null &
./cli-ruby/import_flights.rb --file data/flights-2023.csv.gz 2>&1 >/dev/null &
./cli-ruby/import_flights.rb --file data/flights-2024.csv.gz 2>&1 >/dev/null &
./cli-ruby/import_flights.rb --file data/flights-2025.csv.gz 2>&1 >/dev/null &
