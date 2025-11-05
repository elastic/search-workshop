#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Load BTS On-Time Performance data into Elasticsearch."""

from __future__ import annotations

import argparse
import base64
import csv
import glob
import io
import json
import logging
import os
import ssl
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
        params = {"refresh": "true" if refresh else "false"}
        status, body, _ = self._request(
            "POST",
            f"/{index}/_bulk",
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


class FlightLoader:
    def __init__(
        self,
        client: ElasticsearchClient,
        mapping: Dict[str, object],
        index: str,
        *,
        batch_size: int = BATCH_SIZE,
        refresh: bool = False,
    ):
        self._client = client
        self._mapping = mapping
        self._index = index
        self._batch_size = max(1, batch_size)
        self._refresh = refresh

    def ensure_index(self) -> None:
        if self._client.index_exists(self._index):
            return
        self._client.create_index(self._index, self._mapping)

    def import_files(self, files: Iterable[Path]) -> None:
        self.ensure_index()
        for file_path in files:
            self._import_file(file_path)

    def _import_file(self, file_path: Path) -> None:
        if not file_path.is_file():
            LOGGER.warning("Skipping %s (not a regular file)", file_path)
            return

        LOGGER.info("Importing %s", file_path)

        buffered_lines: List[str] = []
        buffered_docs = 0
        indexed_docs = 0
        processed_rows = 0

        for row in self._iter_rows(file_path):
            processed_rows += 1
            doc = self._transform_row(row)
            if not doc:
                continue

            buffered_lines.append(json.dumps({"index": {}}))
            buffered_lines.append(json.dumps(doc, ensure_ascii=False))
            buffered_docs += 1

            if buffered_docs >= self._batch_size:
                indexed_docs += self._flush(buffered_lines)
                buffered_lines.clear()
                buffered_docs = 0

        if buffered_docs:
            indexed_docs += self._flush(buffered_lines)

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
        else:
            with file_path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    yield row

    def _flush(self, lines: List[str]) -> int:
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        result = self._client.bulk(self._index, payload, self._refresh)

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

        return len(lines) // 2

    def _transform_row(self, row: Dict[str, str]) -> Dict[str, object]:
        doc: Dict[str, object] = {}

        doc["Carrier"] = present(row.get("IATA_CODE_Reporting_Airline")) or present(
            row.get("Reporting_Airline")
        )
        doc["FlightNum"] = present(row.get("Flight_Number_Reporting_Airline"))
        doc["Origin"] = present(row.get("Origin"))
        doc["OriginAirportID"] = present(row.get("OriginAirportID"))
        doc["OriginCityName"] = present(row.get("OriginCityName"))
        doc["OriginRegion"] = present(row.get("OriginState"))
        doc["OriginCountry"] = present(row.get("OriginCountry")) or "US"

        doc["Dest"] = present(row.get("Dest"))
        doc["DestAirportID"] = present(row.get("DestAirportID"))
        doc["DestCityName"] = present(row.get("DestCityName"))
        doc["DestRegion"] = present(row.get("DestState"))
        doc["DestCountry"] = present(row.get("DestCountry")) or "US"

        doc["Cancelled"] = to_boolean(row.get("Cancelled"))
        doc["FlightDelay"] = to_boolean(row.get("ArrDel15"))
        doc["FlightDelayMin"] = to_integer(row.get("ArrDelayMinutes"))
        doc["FlightDelayType"] = classify_delay(row.get("ArrivalDelayGroups"))

        minutes = to_float(row.get("CRSElapsedTime"))
        if minutes is not None:
            doc["FlightTimeMin"] = minutes
            doc["FlightTimeHour"] = f"{minutes / 60.0:.2f}"

        distance_miles = to_float(row.get("Distance"))
        if distance_miles is not None:
            doc["DistanceMiles"] = distance_miles
            doc["DistanceKilometers"] = round(distance_miles * 1.60934, 3)

        if "FlightTimeHour" not in doc:
            dep_block = present(row.get("DepTimeBlk"))
            if dep_block is not None:
                doc["FlightTimeHour"] = dep_block

        doc["dayOfWeek"] = to_integer(row.get("DayOfWeek"))
        timestamp = present(row.get("FlightDate"))
        if timestamp is not None:
            doc["timestamp"] = timestamp

        compacted = {key: value for key, value in doc.items() if value is not None}
        return compacted


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


def classify_delay(value: Optional[str]) -> Optional[str]:
    group = to_integer(value)
    if group is None:
        return None

    mapping = {
        -1: "early_or_ontime",
        0: "late_0_14",
        1: "late_15_29",
        2: "late_30_44",
        3: "late_45_59",
        4: "late_60_74",
        5: "late_75_89",
        6: "late_90_104",
        7: "late_105_119",
        8: "late_120_134",
        9: "late_135_149",
        10: "late_150_plus",
    }
    return mapping.get(group, "unknown")


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
    # Allow explicit absolute/relative paths first.
    if path.exists():
        return path.resolve()

    candidate = (data_dir / path).resolve()
    if candidate.exists():
        return candidate

    raise FileNotFoundError(f"File not found: {path}")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import flight data into Elasticsearch.")
    parser.add_argument(
        "-c",
        "--config",
        default="config/elasticsearch.yml",
        help="Path to Elasticsearch config YAML (default: config/elasticsearch.yml)",
    )
    parser.add_argument("-m", "--mapping", default="mappings-flights.json", help="Path to mappings JSON (default: mappings-flights.json)")
    parser.add_argument("-d", "--data-dir", default="data", help="Directory containing data files (default: data)")
    parser.add_argument("-f", "--file", help="Only import the specified file")
    parser.add_argument("-a", "--all", action="store_true", help="Import all files found in the data directory")
    parser.add_argument("-g", "--glob", help="Import files matching the glob pattern")
    parser.add_argument("--index", default="flights", help="Override index name (default: flights)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Number of documents per bulk request (default: 1000)")
    parser.add_argument("--refresh", action="store_true", help="Request an index refresh after each bulk request")
    parser.add_argument("--status", action="store_true", help="Test connection and print cluster health status")
    parser.add_argument("--delete-index", action="store_true", help="Delete the target index and exit")
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

    data_dir = Path(args.data_dir).resolve()
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

    loader = FlightLoader(
        client=client,
        mapping=mapping,
        index=args.index,
        batch_size=args.batch_size,
        refresh=args.refresh,
    )

    try:
        loader.import_files(target_files)
    except Exception as exc:
        raise SystemExit(f"Import failed: {exc}") from exc


if __name__ == "__main__":
    main(sys.argv[1:])
