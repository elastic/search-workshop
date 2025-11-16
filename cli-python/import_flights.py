#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Load BTS On-Time Performance data into Elasticsearch."""

from __future__ import annotations

import argparse
import base64
import csv
import glob
import gzip
import io
import json
import logging
import os
import shlex
import ssl
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    yaml = None


LOGGER = logging.getLogger(__name__)
BATCH_SIZE = 1_000
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR.parent / "config" / "elasticsearch.yml"
DEFAULT_MAPPING_PATH = SCRIPT_DIR.parent / "config" / "mappings-flights.json"


def load_yaml(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to load configuration files. Install with 'pip install PyYAML'."
        )

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Configuration must be a mapping (found {type(data).__name__})")
    return data


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError(f"Mapping file must define a JSON object (found {type(data).__name__})")
    return data


def combine_paths(base_path: str, new_path: str) -> str:
    base = base_path.rstrip("/")
    suffix = new_path.lstrip("/")
    if not base:
        return f"/{suffix}" if not suffix.startswith("/") else suffix
    if not suffix:
        return base if base.startswith("/") else f"/{base}"
    combined = f"{base}/{suffix}"
    return combined if combined.startswith("/") else f"/{combined}"


@dataclass
class ElasticsearchConfig:
    endpoint: str
    headers: Dict[str, str]
    user: Optional[str]
    password: Optional[str]
    api_key: Optional[str]
    ssl_verify: bool
    ca_file: Optional[str]
    ca_path: Optional[str]

    @classmethod
    def from_mapping(cls, data: Dict[str, object]) -> "ElasticsearchConfig":
        endpoint = str(data.get("endpoint") or "").strip()
        if not endpoint:
            raise ValueError("The Elasticsearch config must include an 'endpoint'.")

        headers = data.get("headers") or {}
        if not isinstance(headers, dict):
            raise ValueError("'headers' must be a mapping of string keys to values.")

        def optional_str(value: object) -> Optional[str]:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        ssl_verify_value = data.get("ssl_verify", True)
        if isinstance(ssl_verify_value, bool):
            ssl_verify = ssl_verify_value
        elif isinstance(ssl_verify_value, str):
            lowered = ssl_verify_value.strip().lower()
            if lowered in {"false", "0", "no", "n"}:
                ssl_verify = False
            else:
                ssl_verify = True
        else:
            ssl_verify = True

        return cls(
            endpoint=endpoint,
            headers={str(k): str(v) for k, v in headers.items()},
            user=optional_str(data.get("user")),
            password=optional_str(data.get("password")),
            api_key=optional_str(data.get("api_key")),
            ssl_verify=ssl_verify,
            ca_file=optional_str(data.get("ca_file")),
            ca_path=optional_str(data.get("ca_path")),
        )


class ElasticsearchClient:
    def __init__(self, config: ElasticsearchConfig):
        parsed = urllib_parse.urlparse(config.endpoint)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid Elasticsearch endpoint: {config.endpoint}")

        self._base = parsed
        base_path = parsed.path or ""
        self._base_path = "" if base_path in {"", "/"} else base_path.rstrip("/")
        self._headers = dict(config.headers)
        self._user = config.user
        self._password = config.password
        self._api_key = config.api_key
        self._ssl_context = self._build_ssl_context(
            parsed.scheme, config.ssl_verify, config.ca_file, config.ca_path
        )

    @staticmethod
    def _build_ssl_context(
        scheme: str, verify: bool, ca_file: Optional[str], ca_path: Optional[str]
    ) -> Optional[ssl.SSLContext]:
        if scheme != "https":
            return None

        if not verify:
            return ssl._create_unverified_context()

        try:
            context = ssl.create_default_context(cafile=ca_file, capath=ca_path)
        except ssl.SSLError as exc:  # pragma: no cover - depends on system
            raise RuntimeError(f"Failed to build SSL context: {exc}") from exc
        return context

    def index_exists(self, name: str) -> bool:
        status, _, _ = self._request("HEAD", f"/{name}")
        return 200 <= status < 300

    def create_index(self, name: str, mapping: Dict[str, object]) -> None:
        status, body, _ = self._request(
            "PUT",
            f"/{name}",
            body=json.dumps(mapping).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        if 200 <= status < 300:
            LOGGER.info("Index '%s' created", name)
            return
        if status == 409:
            LOGGER.warning("Index '%s' already exists (conflict)", name)
            return
        raise RuntimeError(f"Index creation failed ({status}): {body.decode('utf-8', 'ignore')}")

    def bulk(self, index: str, payload: bytes, refresh: bool) -> Dict[str, object]:
        # When _index is specified in action lines, use global _bulk endpoint
        # The index parameter is kept for backward compatibility but ignored when _index is in payload
        params = {"refresh": "true" if refresh else "false"}
        status, body, _ = self._request(
            "POST",
            "/_bulk",
            body=payload,
            headers={"Content-Type": "application/x-ndjson"},
            params=params,
        )
        if not (200 <= status < 300):
            raise RuntimeError(f"Bulk request failed ({status}): {body.decode('utf-8', 'ignore')}")
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse bulk response JSON: {exc}") from exc

    def cluster_health(self) -> Dict[str, object]:
        status, body, _ = self._request("GET", "/_cluster/health")
        if not (200 <= status < 300):
            raise RuntimeError(f"Cluster health request failed ({status}): {body.decode('utf-8', 'ignore')}")
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse cluster health JSON: {exc}") from exc

    def delete_index(self, name: str) -> bool:
        status, body, _ = self._request("DELETE", f"/{name}")
        if 200 <= status < 300:
            return True
        if status == 404:
            return False
        raise RuntimeError(f"Index deletion failed ({status}): {body.decode('utf-8', 'ignore')}")

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        timeout: int = 30,
    ) -> Tuple[int, bytes, Dict[str, str]]:
        target_path = combine_paths(self._base_path, path)
        url = self._build_url(target_path, params)

        request_headers = dict(self._headers)
        if headers:
            request_headers.update(headers)

        auth_header = self._build_auth_header()
        if auth_header:
            request_headers["Authorization"] = auth_header

        data = body
        req = urllib_request.Request(url, data=data, method=method.upper())
        for key, value in request_headers.items():
            req.add_header(key, value)
        if data is not None and "Content-Length" not in req.headers:
            req.add_header("Content-Length", str(len(data)))

        try:
            with urllib_request.urlopen(req, timeout=timeout, context=self._ssl_context) as response:
                status = response.getcode()
                payload = response.read()
                return status, payload, dict(response.headers.items())
        except urllib_error.HTTPError as exc:
            return exc.code, exc.read(), dict(exc.headers.items() if exc.headers else {})
        except urllib_error.URLError as exc:
            raise RuntimeError(f"HTTP request failed: {exc}") from exc

    def _build_url(self, path: str, params: Optional[Dict[str, str]]) -> str:
        query = urllib_parse.urlencode(params or {})
        merged = self._base._replace(path=path, query=query)
        return urllib_parse.urlunparse(merged)

    def _build_auth_header(self) -> Optional[str]:
        if self._api_key:
            return f"ApiKey {self._api_key}"
        if self._user and self._password:
            token = f"{self._user}:{self._password}"
            encoded = base64.b64encode(token.encode("utf-8")).decode("ascii")
            return f"Basic {encoded}"
        return None


class AirportLookup:
    def __init__(self, airports_file: Optional[Path], logger: logging.Logger):
        self._logger = logger
        self._airports: Dict[str, Tuple[float, float]] = {}
        if airports_file and airports_file.exists():
            self._load_airports(airports_file)

    def lookup_coordinates(self, iata_code: Optional[str]) -> Optional[str]:
        if not iata_code:
            return None

        airport = self._airports.get(iata_code.upper())
        if not airport:
            return None

        lat, lon = airport
        return f"{lat},{lon}"

    def _load_airports(self, file_path: Path) -> None:
        self._logger.info("Loading airports from %s", file_path)
        count = 0

        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as handle:
                reader = csv.reader(handle)
                for row in reader:
                    # Columns: ID, Name, City, Country, IATA, ICAO, Lat, Lon, ...
                    if len(row) < 8:
                        continue

                    iata = row[4].strip() if len(row) > 4 else ""
                    if not iata or iata == "\\N":
                        continue

                    lat_str = row[6].strip() if len(row) > 6 else ""
                    lon_str = row[7].strip() if len(row) > 7 else ""
                    if not lat_str or not lon_str:
                        continue

                    try:
                        lat = float(lat_str)
                        lon = float(lon_str)
                        self._airports[iata.upper()] = (lat, lon)
                        count += 1
                    except (ValueError, TypeError):
                        continue

            self._logger.info("Loaded %s airports into lookup table", count)
        except Exception as exc:
            self._logger.warning("Failed to load airports file: %s", exc)


class CancellationLookup:
    def __init__(self, cancellations_file: Optional[Path], logger: logging.Logger):
        self._logger = logger
        self._cancellations: Dict[str, str] = {}
        if cancellations_file and cancellations_file.exists():
            self._load_cancellations(cancellations_file)

    def lookup_reason(self, code: Optional[str]) -> Optional[str]:
        if not code:
            return None

        return self._cancellations.get(code.upper())

    def _load_cancellations(self, file_path: Path) -> None:
        self._logger.info("Loading cancellations from %s", file_path)
        count = 0

        try:
            with file_path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    code = row.get("Code", "").strip()
                    description = row.get("Description", "").strip()
                    if not code or not description:
                        continue

                    self._cancellations[code.upper()] = description
                    count += 1

            self._logger.info("Loaded %s cancellation reasons into lookup table", count)
        except Exception as exc:
            self._logger.warning("Failed to load cancellations file: %s", exc)


class FlightLoader:
    def __init__(
        self,
        client: ElasticsearchClient,
        mapping: Dict[str, object],
        index: str,
        *,
        batch_size: int = BATCH_SIZE,
        refresh: bool = False,
        airports_file: Optional[Path] = None,
        cancellations_file: Optional[Path] = None,
    ):
        self._client = client
        self._mapping = mapping
        self._index = index
        self._batch_size = max(1, batch_size)
        self._refresh = refresh
        self._airport_lookup = AirportLookup(airports_file, LOGGER)
        self._cancellation_lookup = CancellationLookup(cancellations_file, LOGGER)
        self._total_records = 0
        self._loaded_records = 0

    def import_files(self, files: Iterable[Path]) -> None:
        file_list = list(files)
        LOGGER.info("Counting records in %s file(s)...", len(file_list))
        self._total_records = self._count_total_records_fast(file_list)
        LOGGER.info("Total records to import: %s", self._format_number(self._total_records))
        LOGGER.info("Importing %s file(s)...", len(file_list))
        
        for file_path in file_list:
            self._import_file(file_path)
        
        # Print newline after progress line
        sys.stdout.write("\n")
        sys.stdout.flush()
        LOGGER.info(
            "Import complete: %s of %s records loaded",
            self._format_number(self._loaded_records),
            self._format_number(self._total_records),
        )

    def _import_file(self, file_path: Path) -> None:
        if not file_path.is_file():
            LOGGER.warning("Skipping %s (not a regular file)", file_path)
            return

        # Extract index name from filename (remove .csv.gz, .csv, .zip extensions)
        index_name = self._extract_index_name_from_filename(file_path)
        LOGGER.info("Importing %s into index %s", file_path, index_name)
        
        # Ensure index exists
        self._ensure_index(index_name)

        buffered_lines: List[str] = []
        buffered_docs = 0
        indexed_docs = 0
        processed_rows = 0
        batch_number = 0

        for row in self._iter_rows(file_path):
            processed_rows += 1
            doc = self._transform_row(row)
            if not doc:
                continue

            buffered_lines.append(json.dumps({"index": {"_index": index_name}}))
            buffered_lines.append(json.dumps(doc, ensure_ascii=False))
            buffered_docs += 1

            if buffered_docs >= self._batch_size:
                batch_number += 1
                docs_in_batch = self._flush(buffered_lines, index_name)
                indexed_docs += docs_in_batch
                buffered_lines.clear()
                buffered_docs = 0

        if buffered_docs:
            batch_number += 1
            docs_in_batch = self._flush(buffered_lines, index_name)
            indexed_docs += docs_in_batch

        LOGGER.info(
            "Finished %s (rows processed: %s, documents indexed: %s)",
            file_path,
            processed_rows,
            indexed_docs,
        )

    def _iter_rows(self, file_path: Path) -> Iterator[Dict[str, str]]:
        if file_path.suffix.lower() == ".zip":
            with zipfile.ZipFile(file_path) as archive:
                entry_name = next(
                    (name for name in archive.namelist() if name.lower().endswith(".csv")), None
                )
                if entry_name is None:
                    raise RuntimeError(f"No CSV entry found in archive {file_path}")
                with archive.open(entry_name, "r") as entry:
                    with io.TextIOWrapper(entry, encoding="utf-8") as text_stream:
                        reader = csv.DictReader(text_stream)
                        for row in reader:
                            yield row
        elif file_path.suffix.lower() == ".gz":
            with gzip.open(file_path, "rt", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    yield row
        else:
            with file_path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    yield row

    def _ensure_index(self, index_name: str) -> None:
        """Ensure an index exists, creating it if necessary."""
        if self._client.index_exists(index_name):
            LOGGER.info("Index '%s' already exists", index_name)
            return
        LOGGER.info("Creating index: %s", index_name)
        self._client.create_index(index_name, self._mapping)

    def _flush(self, lines: List[str], index_name: str) -> int:
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        result = self._client.bulk(index_name, payload, self._refresh)

        if result.get("errors"):
            items = result.get("items", [])
            errors = [
                item.get("index", {}).get("error")
                for item in items
                if isinstance(item, dict) and item.get("index", {}).get("error")
            ]
            for error in errors[:5]:
                LOGGER.error("Bulk item error: %s", error)
            raise RuntimeError("Bulk indexing reported errors; aborting")

        doc_count = len(lines) // 2
        self._loaded_records += doc_count
        
        if self._total_records > 0:
            percentage = round(self._loaded_records / self._total_records * 100, 1)
            progress = "\r{} of {} records loaded ({}%)".format(
                self._format_number(self._loaded_records),
                self._format_number(self._total_records),
                percentage,
            )
        else:
            progress = "\r{} records loaded".format(self._format_number(self._loaded_records))
        
        sys.stdout.write(progress)
        sys.stdout.flush()
        
        return doc_count

    def _transform_row(self, row: Dict[str, str]) -> Dict[str, object]:
        doc: Dict[str, object] = {}

        # Get timestamp - prefer @timestamp column if it exists, otherwise use FlightDate
        timestamp = present(row.get("@timestamp")) or present(row.get("FlightDate"))
        doc["@timestamp"] = timestamp

        # Flight ID - construct from date, airline, flight number, origin, and destination
        flight_date = timestamp
        reporting_airline = present(row.get("Reporting_Airline"))
        flight_number = present(row.get("Flight_Number_Reporting_Airline"))
        origin = present(row.get("Origin"))
        dest = present(row.get("Dest"))

        if flight_date and reporting_airline and flight_number and origin and dest:
            doc["FlightID"] = f"{flight_date}_{reporting_airline}_{flight_number}_{origin}_{dest}"

        # Direct mappings from CSV to mapping field names
        doc["Reporting_Airline"] = reporting_airline
        doc["Tail_Number"] = present(row.get("Tail_Number"))
        doc["Flight_Number"] = flight_number
        doc["Origin"] = origin
        doc["Dest"] = dest

        # Time fields - convert to integers (minutes or time in HHMM format)
        doc["CRSDepTimeLocal"] = to_integer(row.get("CRSDepTime"))
        doc["DepDelayMin"] = to_integer(row.get("DepDelay"))
        doc["TaxiOutMin"] = to_integer(row.get("TaxiOut"))
        doc["TaxiInMin"] = to_integer(row.get("TaxiIn"))
        doc["CRSArrTimeLocal"] = to_integer(row.get("CRSArrTime"))
        doc["ArrDelayMin"] = to_integer(row.get("ArrDelay"))

        # Boolean fields
        doc["Cancelled"] = to_boolean(row.get("Cancelled"))
        doc["Diverted"] = to_boolean(row.get("Diverted"))

        # Cancellation code
        cancellation_code = present(row.get("CancellationCode"))
        doc["CancellationCode"] = cancellation_code

        # Cancellation reason - lookup from cancellations data
        cancellation_reason = self._cancellation_lookup.lookup_reason(cancellation_code)
        if cancellation_reason:
            doc["CancellationReason"] = cancellation_reason

        # Time duration fields (convert to minutes as integers)
        doc["ActualElapsedTimeMin"] = to_integer(row.get("ActualElapsedTime"))
        doc["AirTimeMin"] = to_integer(row.get("AirTime"))

        # Count and distance
        doc["Flights"] = to_integer(row.get("Flights"))
        doc["DistanceMiles"] = to_integer(row.get("Distance"))

        # Delay fields (with Min suffix to match mapping)
        doc["CarrierDelayMin"] = to_integer(row.get("CarrierDelay"))
        doc["WeatherDelayMin"] = to_integer(row.get("WeatherDelay"))
        doc["NASDelayMin"] = to_integer(row.get("NASDelay"))
        doc["SecurityDelayMin"] = to_integer(row.get("SecurityDelay"))
        doc["LateAircraftDelayMin"] = to_integer(row.get("LateAircraftDelay"))

        # Geo point fields - lookup from airports data
        origin_location = self._airport_lookup.lookup_coordinates(origin)
        if origin_location:
            doc["OriginLocation"] = origin_location

        dest_location = self._airport_lookup.lookup_coordinates(dest)
        if dest_location:
            doc["DestLocation"] = dest_location

        # Remove None values
        compacted = {key: value for key, value in doc.items() if value is not None}
        return compacted

    def _extract_index_name_from_filename(self, file_path: Path) -> str:
        """Extract index name from filename by removing extensions."""
        basename = file_path.stem  # Gets filename without extension
        # Handle multiple extensions like .csv.gz
        while basename.endswith(('.csv', '.gz', '.zip')):
            basename = Path(basename).stem
        return basename

    def _format_number(self, number: int) -> str:
        """Format number with thousands separators."""
        return f"{number:,}"

    def _count_total_records_fast(self, files: List[Path]) -> int:
        """Count total records across all files using fast shell commands."""
        total = 0
        for file_path in files:
            if not file_path.is_file():
                continue
            line_count = self._count_lines_fast(file_path)
            # Subtract 1 for CSV header
            total += max(line_count - 1, 0)
        return total

    def _count_lines_fast(self, file_path: Path) -> int:
        """Count lines in a file using fast shell commands."""
        try:
            if file_path.suffix.lower() == ".zip":
                with zipfile.ZipFile(file_path) as archive:
                    entry_name = next(
                        (name for name in archive.namelist() if name.lower().endswith(".csv")), None
                    )
                    if entry_name is None:
                        return 0
                    # Use unzip -p to extract and pipe to wc -l
                    cmd = f'unzip -p {shlex.quote(str(file_path))} {shlex.quote(entry_name)} | wc -l'
                    result = subprocess.run(
                        cmd, shell=True, capture_output=True, text=True, check=False
                    )
                    if result.returncode != 0:
                        LOGGER.warning("Failed to count lines in %s: %s", file_path, result.stderr)
                        return 0
                    return int(result.stdout.strip())
            elif file_path.suffix.lower() == ".gz" or str(file_path).lower().endswith(".gz"):
                # Use gunzip -c for gzip files
                cmd = f'gunzip -c {shlex.quote(str(file_path))} | wc -l'
                result = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, check=False
                )
                if result.returncode != 0:
                    LOGGER.warning("Failed to count lines in %s: %s", file_path, result.stderr)
                    return 0
                return int(result.stdout.strip())
            else:
                # Use wc -l for regular files
                result = subprocess.run(
                    ["wc", "-l", str(file_path)], capture_output=True, text=True, check=False
                )
                if result.returncode != 0:
                    LOGGER.warning("Failed to count lines in %s: %s", file_path, result.stderr)
                    return 0
                return int(result.stdout.split()[0])
        except Exception as exc:
            LOGGER.warning("Failed to count lines in %s: %s", file_path, exc)
            return 0


def present(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_float(value: Optional[str]) -> Optional[float]:
    text = present(value)
    if text is None:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def to_integer(value: Optional[str]) -> Optional[int]:
    number = to_float(value)
    if number is None:
        return None
    return int(round(number))


def to_boolean(value: Optional[str]) -> Optional[bool]:
    text = present(value)
    if text is None:
        return None

    lowered = text.lower()
    if lowered in {"true", "t", "yes", "y"}:
        return True
    if lowered in {"false", "f", "no", "n"}:
        return False

    try:
        numeric = float(text)
    except ValueError:
        return None
    return numeric > 0


def files_to_process(data_dir: Path, target_file: Optional[str], load_all: bool, glob_pattern: Optional[str]) -> List[Path]:
    if target_file:
        resolved = resolve_file_path(Path(target_file), data_dir)
        return [resolved]

    if glob_pattern:
        # Resolve glob pattern - try as-is first, then relative to data_dir
        pattern_path = Path(glob_pattern)
        if pattern_path.is_absolute():
            # For absolute paths, use glob module directly
            matched_files = glob.glob(str(pattern_path))
        else:
            # Try the pattern as-is first (in case it's relative to current directory)
            matched_files = glob.glob(glob_pattern)
            if not matched_files:
                # If no matches, try relative to data_dir
                expanded_pattern = data_dir / glob_pattern
                matched_files = glob.glob(str(expanded_pattern))
        
        files = sorted([Path(f) for f in matched_files if Path(f).is_file()])
        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {glob_pattern}")
        return files

    if load_all:
        zip_files = sorted(data_dir.glob("*.zip"))
        csv_files = sorted(data_dir.glob("*.csv"))
        files = zip_files + csv_files
        if not files:
            raise FileNotFoundError(f"No .zip or .csv files found in {data_dir}")
        return files

    raise ValueError("Please provide either --file PATH, --all, or --glob PATTERN.")


def resolve_file_path(path: Path, data_dir: Path) -> Path:
    # If path is absolute and exists, use it
    if path.is_absolute() and path.exists():
        return path.resolve()
    
    # If path exists relative to current directory, use it
    if path.exists():
        return path.resolve()

    # Try relative to data_dir
    candidate = (data_dir / path).resolve()
    if candidate.exists():
        return candidate
    
    # If path starts with "data/", try stripping that prefix
    path_str = str(path)
    if path_str.startswith("data/"):
        candidate = (data_dir / path_str[5:]).resolve()
        if candidate.exists():
            return candidate

    raise FileNotFoundError(f"File not found: {path}")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import flight data into Elasticsearch.")
    parser.add_argument(
        "-c",
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to Elasticsearch config YAML (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "-m",
        "--mapping",
        default=str(DEFAULT_MAPPING_PATH),
        help=f"Path to mappings JSON (default: {DEFAULT_MAPPING_PATH})",
    )
    parser.add_argument(
        "-d",
        "--data-dir",
        default=str(SCRIPT_DIR.parent / "data"),
        help="Directory containing data files (default: ../data relative to script)",
    )
    parser.add_argument("-f", "--file", help="Only import the specified file")
    parser.add_argument("-a", "--all", action="store_true", help="Import all files found in the data directory")
    parser.add_argument("-g", "--glob", help="Import files matching the glob pattern")
    parser.add_argument("--index", default="flights", help="Override index name (default: flights)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Number of documents per bulk request (default: 1000)")
    parser.add_argument("--refresh", action="store_true", help="Request an index refresh after each bulk request")
    parser.add_argument("--status", action="store_true", help="Test connection and print cluster health status")
    parser.add_argument("--delete-index", action="store_true", help="Delete the target index and exit")
    parser.add_argument("--airports", help="Path to airports.csv.gz file for geo-coordinate lookup")
    parser.add_argument("--cancellations", help="Path to cancellations.csv file for cancellation reason lookup")
    return parser.parse_args(argv)


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(levelname)s %(message)s")
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers[:] = [handler]


def report_status(client: ElasticsearchClient) -> None:
    status = client.cluster_health()
    LOGGER.info("Cluster status: %s", status.get("status"))
    LOGGER.info(
        "Active shards: %s, node count: %s",
        status.get("active_shards"),
        status.get("number_of_nodes"),
    )


def delete_index(client: ElasticsearchClient, index_name: str) -> None:
    if client.delete_index(index_name):
        LOGGER.info("Index '%s' deleted", index_name)
    else:
        LOGGER.warning("Index '%s' was not found", index_name)


def main(argv: List[str]) -> None:
    configure_logging()
    args = parse_args(argv)

    if args.status and args.delete_index:
        raise SystemExit("Cannot use --status and --delete-index together.")

    if not args.status and not args.delete_index:
        selection_options = [args.file, args.all, args.glob]
        if sum(1 for opt in selection_options if opt) > 1:
            raise SystemExit("Cannot use --file, --all, and --glob together (use only one).")

    # Resolve data_dir - if relative, resolve relative to script's parent (project root)
    data_dir_path = Path(args.data_dir)
    if not data_dir_path.is_absolute():
        data_dir = (SCRIPT_DIR.parent / data_dir_path).resolve()
    else:
        data_dir = data_dir_path.resolve()
    try:
        config = load_yaml(Path(args.config).resolve())
    except Exception as exc:
        raise SystemExit(f"Failed to load config: {exc}") from exc

    try:
        mapping = load_json(Path(args.mapping).resolve())
    except Exception as exc:
        raise SystemExit(f"Failed to load mapping: {exc}") from exc

    try:
        es_config = ElasticsearchConfig.from_mapping(config)
    except Exception as exc:
        raise SystemExit(f"Invalid Elasticsearch config: {exc}") from exc

    client = ElasticsearchClient(es_config)

    if args.status:
        try:
            report_status(client)
        except Exception as exc:
            raise SystemExit(f"Failed to retrieve cluster status: {exc}") from exc
        return

    if args.delete_index:
        try:
            delete_index(client, args.index)
        except Exception as exc:
            raise SystemExit(f"Failed to delete index '{args.index}': {exc}") from exc
        return

    target_files: List[Path]
    try:
        target_files = files_to_process(data_dir, args.file, args.all, args.glob)
    except Exception as exc:
        raise SystemExit(str(exc)) from exc

    # Resolve airports file path
    airports_file: Optional[Path] = None
    if args.airports:
        airports_file = Path(args.airports).resolve()
        if not airports_file.exists():
            LOGGER.warning("Airports file not found: %s", airports_file)
            airports_file = None
    else:
        # Try default location in data directory
        default_airports = data_dir / "airports.csv.gz"
        if default_airports.exists():
            airports_file = default_airports

    # Resolve cancellations file path
    cancellations_file: Optional[Path] = None
    if args.cancellations:
        cancellations_file = Path(args.cancellations).resolve()
        if not cancellations_file.exists():
            LOGGER.warning("Cancellations file not found: %s", cancellations_file)
            cancellations_file = None
    else:
        # Try default location in data directory
        default_cancellations = data_dir / "cancellations.csv"
        if default_cancellations.exists():
            cancellations_file = default_cancellations

    loader = FlightLoader(
        client=client,
        mapping=mapping,
        index=args.index,
        batch_size=args.batch_size,
        refresh=args.refresh,
        airports_file=airports_file,
        cancellations_file=cancellations_file,
    )

    try:
        loader.import_files(target_files)
    except Exception as exc:
        raise SystemExit(f"Import failed: {exc}") from exc


if __name__ == "__main__":
    main(sys.argv[1:])
