#!/usr/bin/env python3
"""Extract ticker data from portfolio JSON into a CSV file."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


CSV_COLUMNS = [
    "AS_OF_DATE",
    "SYMBOL",
    "ID_ISIN",
    "ID_TYPE",
    "POSITION",
    "COST_BASIS",
    "PERCENTAGE_NAV",
    "CURRENCY",
    "EXCHANGE",
]

EXCHANGE_MAPPER_PATH = Path(__file__).resolve().with_name(
    "IB_to_Bloomberg_exchange_mapper.json"
)

# Final portfolio CSV for sharing (default --output).
DEFAULT_OUTPUT_CSV = Path.home() / "shared" / "arr_portfolio_snapshot.csv"


def normalize_user_path(raw_path: str) -> Path:
    """
    Normalize user-provided paths.

    Supports both standard "~/" and non-standard "~GitHub/..." notation.
    """
    if raw_path.startswith("~GitHub/"):
        raw_path = raw_path.replace("~GitHub/", "~/GitHub/", 1)
    return Path(raw_path).expanduser()


def parse_as_of_date(as_of: str | None) -> str:
    """Convert datetime string to YYYYMMDD format."""
    if not as_of:
        return datetime.now().strftime("%Y%m%d")
    try:
        return datetime.fromisoformat(as_of).strftime("%Y%m%d")
    except ValueError:
        # Fallback to first 10 chars if it looks like YYYY-MM-DD...
        if len(as_of) >= 10:
            maybe_date = as_of[:10].replace("-", "")
            if maybe_date.isdigit() and len(maybe_date) == 8:
                return maybe_date
        return datetime.now().strftime("%Y%m%d")


def _normalize_cost_basis(avg_cost: Any, multiplier: Any) -> Any:
    """Return Avg Cost divided by Multiplier when both are numeric."""
    try:
        avg_cost_value = float(avg_cost)
        multiplier_value = float(multiplier)
        if multiplier_value != 0:
            return avg_cost_value / multiplier_value
    except (TypeError, ValueError):
        pass
    return avg_cost


def _normalize_exchange_candidates(exchange: str) -> List[str]:
    """Generate fallback keys for slightly variant IB exchange codes."""
    if not exchange:
        return [""]
    raw = exchange.strip()
    upper = raw.upper()
    no_space = upper.replace(" ", "")
    no_trailing_digits = no_space.rstrip("0123456789")
    candidates = [raw, upper, no_space, no_trailing_digits]
    deduped: List[str] = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def load_exchange_mapping(mapper_path: Path = EXCHANGE_MAPPER_PATH) -> Dict[str, str]:
    """Load IB exchange -> Bloomberg exchange map (bbg_code preferred)."""
    if not mapper_path.exists():
        return {}

    with mapper_path.open("r", encoding="utf-8") as infile:
        payload = json.load(infile)

    mapped: Dict[str, str] = {}
    for section, section_value in payload.items():
        if section.startswith("_") or not isinstance(section_value, dict):
            continue
        for ib_code, info in section_value.items():
            if not isinstance(info, dict):
                continue
            bbg_code = info.get("bbg_code")
            bbg_composite = info.get("bbg_composite")
            out_code = bbg_code or bbg_composite
            if out_code:
                mapped[ib_code.upper()] = str(out_code).strip()
    return mapped


def map_exchange_to_bloomberg(exchange: str, mapping: Dict[str, str]) -> str:
    """Map IB exchange code to Bloomberg exchange suffix."""
    if not exchange:
        return ""
    for key in _normalize_exchange_candidates(exchange):
        mapped = mapping.get(key.upper())
        if mapped:
            return mapped
    return exchange.strip()


def transform_rows(
    payload: Dict[str, Any], exchange_mapping: Dict[str, str] | None = None
) -> List[Dict[str, Any]]:
    as_of_date = parse_as_of_date(payload.get("as_of"))
    rows = payload.get("rows", [])
    exchange_mapping = exchange_mapping or {}

    output_rows: List[Dict[str, Any]] = []
    for row in rows:
        id_isin = (row.get("ISIN") or "").strip()
        raw_exchange = (
            row.get("eexchange")
            or row.get("eExchange")
            or row.get("Exchange")
            or ""
        )
        output_rows.append(
            {
                "AS_OF_DATE": as_of_date,
                "SYMBOL": row.get("Symbol"),
                "ID_ISIN": id_isin,
                "ID_TYPE": "ISIN" if id_isin else "",
                "POSITION": row.get("Position"),
                "COST_BASIS": _normalize_cost_basis(
                    row.get("Avg Cost"), row.get("Multiplier")
                ),
                "PERCENTAGE_NAV": row.get("Weight %"),
                "CURRENCY": row.get("Currency"),
                "EXCHANGE": map_exchange_to_bloomberg(
                    str(raw_exchange), exchange_mapping
                ),
            }
        )
    output_rows.sort(
        key=lambda item: float(item["PERCENTAGE_NAV"])
        if item["PERCENTAGE_NAV"] is not None
        else float("-inf"),
        reverse=True,
    )
    return output_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract ticker fields from a portfolio JSON file into CSV."
    )
    parser.add_argument(
        "--input",
        default="~GitHub/ExecutionIB/portfolio_table.port7496.json",
        help="Path to portfolio JSON input file.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Path to output CSV file. "
            "Default: ~/shared/arr_portfolio_snapshot_file.csv"
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_path = normalize_user_path(args.input)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path = (
        normalize_user_path(args.output)
        if args.output
        else DEFAULT_OUTPUT_CSV
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8") as infile:
        payload = json.load(infile)

    exchange_mapping = load_exchange_mapping()
    transformed = transform_rows(payload, exchange_mapping)

    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(transformed)

    print(f"Wrote {len(transformed)} rows to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
