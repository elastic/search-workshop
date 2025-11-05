# Search & Analytics Workshop

In this workshop we're going to be working with airline flights.

## Historical Flight Data

The flight data for domestic flights since January, 2019 is stored in the data/flights/ directory. A sample of the flight data is in the sample-flight.csv file.

## Airline Lookup Data

We need the full name of each Airline. This data is stored locally in the data/airlines.csv.gz file.

Github:
https://github.com/jpatokal/openflights/blob/master/data/airlines.dat

Raw:
https://raw.githubusercontent.com/jpatokal/openflights/refs/heads/master/data/airlines.dat

Example:

22,"Aloha Airlines",\N,"AQ","AAH","ALOHA","United States","Y"
23,"Alaska Island Air",\N,"","AAK","ALASKA ISLAND","United States","N"
24,"American Airlines",\N,"AA","AAL","AMERICAN","United States","Y"

Fields:

Airline ID	Unique OpenFlights identifier for this airline.
Name	Name of the airline.
Alias	Alias of the airline. For example, All Nippon Airways is commonly known as "ANA".
IATA	2-letter IATA code, if available.
ICAO	3-letter ICAO code, if available.
Callsign	Airline callsign.
Country	Country or territory where airport is located. See Countries to cross-reference to ISO 3166-1 codes.
Active	"Y" if the airline is or has until recently been operational, "N" if it is defunct. This field is not reliable: in particular, major airlines that stopped flying long ago, but have not had their IATA code reassigned (eg. Ansett/AN), will incorrectly show as "Y".

## Airport Lookup Data

We need the full name and geolocation of each Airport. This data is stored locally in the data/airports.csv.gz file.

Github:
https://github.com/jpatokal/openflights/blob/master/data/airports.dat

Raw:
https://raw.githubusercontent.com/jpatokal/openflights/refs/heads/master/data/airports.dat

Example:

3493,"Lafayette Regional Airport","Lafayette","United States","LFT","KLFT",30.20529938,-91.98760223,42,-6,"A","America/Chicago","airport","OurAirports"
3494,"Newark Liberty International Airport","Newark","United States","EWR","KEWR",40.692501068115234,-74.168701171875,18,-5,"A","America/New_York","airport","OurAirports"
3495,"Boise Air Terminal/Gowen Field","Boise","United States","BOI","KBOI",43.5644,-116.223,2871,-7,"A","America/Denver","airport","OurAirports"

Fields:

Airport ID	Unique OpenFlights identifier for this airport.
Name	Name of airport. May or may not contain the City name.
City	Main city served by airport. May be spelled differently from Name.
Country	Country or territory where airport is located. See Countries to cross-reference to ISO 3166-1 codes.
IATA	3-letter IATA code. Null if not assigned/unknown.
ICAO	4-letter ICAO code.
Null if not assigned.
Latitude	Decimal degrees, usually to six significant digits. Negative is South, positive is North.
Longitude	Decimal degrees, usually to six significant digits. Negative is West, positive is East.
Altitude	In feet.
Timezone	Hours offset from UTC. Fractional hours are expressed as decimals, eg. India is 5.5.
DST	Daylight savings time. One of E (Europe), A (US/Canada), S (South America), O (Australia), Z (New Zealand), N (None) or U (Unknown). See also: Help: Time
Tz database timezone	Timezone in "tz" (Olson) format, eg. "America/Los_Angeles".
Type	Type of the airport. Value "airport" for air terminals, "station" for train stations, "port" for ferry terminals and "unknown" if not known. In airports.csv, only type=airport is included.
Source	Source of this data. "OurAirports" for data sourced from OurAirports, "Legacy" for old data not matched to OurAirports (mostly DAFIF), "User" for unverified user contributions. In airports.csv, only source=OurAirports is included.

## Flight File Layout

The BTS On-Time Performance extracts are stored one per month under `data/flights/*.zip`. Each archive contains a single CSV with the full set of DOT columns. The importer scripts (`bin/ruby/import_flights.rb` and `bin/python/import_flights.py`) expect the files to remain zipped and will stream the CSV contents directly from the archive. If you need to inspect the raw data manually, use `unzip -p <zip> | head` to avoid expanding the entire file.

`sample-flight.csv` is a small slice of the 2024-01 data with the original DOT header row. `sample-flight.json` shows the same flights as fully expanded JSON rows to help when mapping fields or prototyping transformations.

## Import Scripts

- `import_flights.rb` is the Ruby entry point for loading data into Elasticsearch. It only relies on the Ruby standard library; no Bundler setup or Gemfile is required.
- `import_flights.py` provides the same workflow in Python. Install dependencies via `python3 -m pip install -r bin/python/requirements.txt` so the script can load `config/elasticsearch.yml`.
- Copy `config/elasticsearch.sample.yml` to `config/elasticsearch.yml` (or pass `--config`) and fill in the `endpoint`, credentials, and any custom headers.
- `mappings-flights.json` defines the index mapping used when the importer creates the `flights` index. Update it before importing if you need additional fields or different types.
- Common commands include: `ruby bin/ruby/import_flights.rb --status` or `python3 bin/python/import_flights.py --status` (verify connectivity), `ruby bin/ruby/import_flights.rb --all` or `python3 bin/python/import_flights.py --all` (stream every `.zip`/`.csv` under `./data`), and `ruby bin/ruby/import_flights.rb --file sample-flight.csv --batch-size 500 --refresh` or `python3 bin/python/import_flights.py --file sample-flight.csv --batch-size 500 --refresh` (import a single file with smaller bulk batches and force refreshes for demos).
- Both importers automatically create the index when it does not exist, batch documents (default 1,000 per `_bulk` call), and stop if Elasticsearch reports item-level errors. Use `--delete-index` to clear the current target before a new load.

## Lookup Joins

- Airlines: join on `IATA_CODE_Reporting_Airline` (fallback to `Reporting_Airline` when IATA is blank) against `data/airlines.csv.gz` to enrich flights with carrier names or callsigns. Remember that the OpenFlights dataset uses `"\\N"` to denote null values.
- Airports: join on DOT `OriginAirportID` / `DestAirportID` or the `Origin` / `Dest` IATA code to fetch airport names and coordinates from `data/airports.csv.gz`. Use `Latitude` and `Longitude` to populate the `OriginLocation` and `DestLocation` `geo_point` fields from the mapping.
- Country/state context in the lookup tables is useful for building aggregations and filter facets (for example, grouping delays by `OriginCountry` or `DestRegion`).
