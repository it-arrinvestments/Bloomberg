import csv
import io
import json
import time
import xml.etree.ElementTree as ET

import requests

TOKEN = "174790321327240887207054"  # from Client Portal > Settings: Flex Web Service
QUERY_ID = "1479361"  # Activity/Query ID from your saved Flex Query

BASE_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"

RAW_OUTPUT = "portfolio_snapshot_raw.csv"
SNAPSHOT_OUTPUT = "portfolio_snapshot.csv"
EXCHANGE_MAPPER_PATH = "IB_to_Bloomberg_exchange_mapper.json"

# If IBKR adds a dollar market-value column to the Flex POST section, map MARKET_VALUE from
# the first present name (no formulas). Standard POST rows only expose MarkPrice (mark per unit).
_MARKET_VALUE_COLUMN_PREFS = (
    "MarketValue",
    "PositionValue",
    "FifoMarketValue",
    "MktValue",
)


def _normalize_exchange_candidates(exchange: str) -> list[str]:
    """Generate fallback keys for slightly variant IB exchange codes."""
    if not exchange:
        return [""]
    raw = exchange.strip()
    upper = raw.upper()
    no_space = upper.replace(" ", "")
    no_trailing_digits = no_space.rstrip("0123456789")
    candidates = [raw, upper, no_space, no_trailing_digits]
    # Keep order, drop empties/duplicates.
    deduped: list[str] = []
    for c in candidates:
        if c and c not in deduped:
            deduped.append(c)
    return deduped


def load_exchange_mapping(mapper_path: str) -> dict[str, str]:
    """
    Return IB exchange -> Bloomberg exchange suffix map (bbg_code preferred).

    Falls back to bbg_composite when bbg_code is missing/null.
    """
    with open(mapper_path, encoding="utf-8") as f:
        payload = json.load(f)

    mapped: dict[str, str] = {}
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


def map_exchange_to_bloomberg(exchange: str, mapping: dict[str, str]) -> str:
    """Map IB exchange code to Bloomberg code; preserve original when unmapped."""
    if not exchange:
        return ""
    for key in _normalize_exchange_candidates(exchange):
        mapped = mapping.get(key.upper())
        if mapped:
            return mapped
    return exchange.strip()


def save_raw_flex_rows(raw_statement: str, output_path: str) -> int:
    rows = list(csv.reader(io.StringIO(raw_statement)))
    with open(output_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerows(rows)
    return len(rows)


def _load_csv_rows(path: str) -> list[list[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def parse_post_section(
    raw_path: str,
) -> tuple[dict[str, int], list[list[str]], int | None, float | None]:
    """Return HEADER/DATA POST payload and NAV if available from EOS,POST."""
    rows = _load_csv_rows(raw_path)
    post_header: list[str] | None = None
    data_rows: list[list[str]] = []
    eos_count: int | None = None
    nav_base: float | None = None
    for row in rows:
        if len(row) < 2:
            continue
        if row[0] == "HEADER" and row[1] == "POST":
            post_header = row
        elif row[0] == "DATA" and row[1] == "POST":
            data_rows.append(row)
        elif row[0] == "EOS" and row[1] == "POST" and len(row) > 2:
            try:
                eos_count = int(row[2])
            except ValueError:
                pass
            if len(row) > 3 and row[3]:
                try:
                    nav_base = float(row[3])
                except ValueError:
                    pass
    if post_header is None:
        raise ValueError(f"No HEADER,POST section in {raw_path!r}")
    col_map = {name: i for i, name in enumerate(post_header[2:], start=2)}
    if eos_count is not None and len(data_rows) != eos_count:
        raise ValueError(
            f"POST DATA row count ({len(data_rows)}) does not match EOS,POST ({eos_count})"
        )
    return col_map, data_rows, eos_count, nav_base


def _get_cell(row: list[str], col_map: dict[str, int], name: str) -> str:
    i = col_map.get(name)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _market_value_column(col_map: dict[str, int]) -> str:
    for name in _MARKET_VALUE_COLUMN_PREFS:
        if name in col_map:
            return name
    if "MarkPrice" in col_map:
        return "MarkPrice"
    raise ValueError(
        "POST header has no column for market value (expected one of "
        f"{_MARKET_VALUE_COLUMN_PREFS} or MarkPrice)"
    )


def _to_float(cell_value: str) -> float | None:
    try:
        return float(cell_value)
    except (TypeError, ValueError):
        return None


def post_rows_to_snapshot_records(
    col_map: dict[str, int],
    data_rows: list[list[str]],
    nav_base: float | None,
    exchange_mapping: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    """Map each DATA,POST row to Bloomberg-oriented fields."""
    mv_col = _market_value_column(col_map)
    exchange_mapping = exchange_mapping or {}
    position_value_col = "PositionValue" if "PositionValue" in col_map else mv_col
    out: list[dict[str, str]] = []
    for row in data_rows:
        isin = _get_cell(row, col_map, "ISIN")
        id_type = "ISIN" if isin else ""
        exchange = _get_cell(row, col_map, "ListingExchange")
        weight_pct = ""
        if nav_base:
            position_value = _to_float(_get_cell(row, col_map, position_value_col))
            fx_rate = _to_float(_get_cell(row, col_map, "FXRateToBase"))
            if position_value is not None and fx_rate is not None:
                weight_pct = f"{(position_value * fx_rate / nav_base) * 100:.6f}"
        rec = {
            "AS_OF_DATE": _get_cell(row, col_map, "ReportDate"),
            "SYMBOL": _get_cell(row, col_map, "Symbol"),
            "ID_ISIN": isin,
            "ID_TYPE": id_type,
            "POSITION": _get_cell(row, col_map, "Quantity"),
            "COST_BASIS": _get_cell(row, col_map, "CostBasisMoney"),
            "MARKET_VALUE": _get_cell(row, col_map, mv_col),
            "WEIGHT_PCT": weight_pct,
            "CURRENCY": _get_cell(row, col_map, "CurrencyPrimary"),
            "EXCHANGE": map_exchange_to_bloomberg(exchange, exchange_mapping),
        }
        out.append(rec)
    return out


def write_snapshot_csv(records: list[dict[str, str]], output_path: str) -> None:
    fieldnames = [
        "AS_OF_DATE",
        "SYMBOL",
        "ID_ISIN",
        "ID_TYPE",
        "POSITION",
        "COST_BASIS",
        "MARKET_VALUE",
        "WEIGHT_PCT",
        "CURRENCY",
        "EXCHANGE",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(records)


def build_snapshot_from_raw(raw_path: str, snapshot_path: str) -> int:
    col_map, data_rows, _, nav_base = parse_post_section(raw_path)
    exchange_mapping = load_exchange_mapping(EXCHANGE_MAPPER_PATH)
    records = post_rows_to_snapshot_records(col_map, data_rows, nav_base, exchange_mapping)
    write_snapshot_csv(records, snapshot_path)
    return len(records)


def fetch_raw_statement() -> str:
    send_resp = requests.get(f"{BASE_URL}/SendRequest", params={"t": TOKEN, "q": QUERY_ID, "v": 3})
    send_resp.raise_for_status()
    root = ET.fromstring(send_resp.text)
    status = root.findtext("Status")
    if status != "Success":
        raise RuntimeError(f"SendRequest failed: {root.findtext('ErrorMessage')}")
    ref_code = root.findtext("ReferenceCode")
    print(f"Reference code: {ref_code} — waiting for report to generate...")
    time.sleep(5)
    get_resp = requests.get(f"{BASE_URL}/GetStatement", params={"t": TOKEN, "q": ref_code, "v": 3})
    get_resp.raise_for_status()
    raw_statement = get_resp.text.strip()
    if not raw_statement:
        raise RuntimeError("GetStatement returned an empty response.")
    return raw_statement


def main() -> None:
    raw_statement = fetch_raw_statement()
    n_raw = save_raw_flex_rows(raw_statement, RAW_OUTPUT)
    print(f"Saved {n_raw} rows to {RAW_OUTPUT}")
    n_snap = build_snapshot_from_raw(RAW_OUTPUT, SNAPSHOT_OUTPUT)
    print(f"Wrote {n_snap} positions to {SNAPSHOT_OUTPUT}")


if __name__ == "__main__":
    main()
