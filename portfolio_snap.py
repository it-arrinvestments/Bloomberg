import time
import requests
import xml.etree.ElementTree as ET
import io
import csv
import pandas as pd

TOKEN    = "174790321327240887207054"    # from Client Portal > Settings > Flex Web Service
QUERY_ID = "1479361" # the Activity/Query ID from your saved Flex Query

BASE_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"


def parse_flex_statement_to_dataframe(raw_statement: str) -> pd.DataFrame:
    rows = list(csv.reader(io.StringIO(raw_statement)))
    if not rows:
        raise RuntimeError("GetStatement returned no CSV rows.")

    header_fields = None
    data_rows = []

    for row in rows:
        if not row:
            continue

        record_type = row[0]
        if record_type == "HEADER":
            # HEADER format: HEADER,<section>,<field_1>,<field_2>,...
            header_fields = row[2:]
            continue

        if record_type == "DATA":
            # DATA format: DATA,<section>,<value_1>,<value_2>,...
            data_rows.append(row[2:])

    if header_fields is None:
        raise RuntimeError("No HEADER row found in Flex CSV response.")
    if not data_rows:
        raise RuntimeError("No DATA rows found in Flex CSV response.")

    normalized_rows = []
    column_count = len(header_fields)
    for row in data_rows:
        if len(row) < column_count:
            row = row + [""] * (column_count - len(row))
        elif len(row) > column_count:
            row = row[:column_count]
        normalized_rows.append(row)

    return pd.DataFrame(normalized_rows, columns=header_fields)


def save_raw_flex_rows(raw_statement: str, output_path: str) -> None:
    rows = list(csv.reader(io.StringIO(raw_statement)))
    with open(output_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerows(rows)

# ── Step 1: Request the report ────────────────────────────────────────────────
send_resp = requests.get(f"{BASE_URL}/SendRequest", params={"t": TOKEN, "q": QUERY_ID, "v": 3})
send_resp.raise_for_status()

root = ET.fromstring(send_resp.text)
status = root.findtext("Status")

if status != "Success":
    raise RuntimeError(f"SendRequest failed: {root.findtext('ErrorMessage')}")

ref_code = root.findtext("ReferenceCode")
print(f"Reference code: {ref_code} — waiting for report to generate...")

# ── Step 2: Wait briefly, then fetch the report ───────────────────────────────
time.sleep(5)  # IBKR needs a moment to generate the report

get_resp = requests.get(f"{BASE_URL}/GetStatement", params={"t": TOKEN, "q": ref_code, "v": 3})
get_resp.raise_for_status()

# ── Step 3: Parse into a DataFrame and save as CSV ───────────────────────────
# The response is CSV text since you set Format = CSV in your Flex Query
raw_statement = get_resp.text.strip()
if not raw_statement:
    raise RuntimeError("GetStatement returned an empty response.")

try:
    df = parse_flex_statement_to_dataframe(raw_statement)
except Exception as exc:
    preview = "\n".join(raw_statement.splitlines()[:10])
    raise RuntimeError(
        f"Unable to parse Flex CSV response with Flex parser.\nPreview:\n{preview}"
    ) from exc

if df.empty:
    raise RuntimeError("Parsed CSV was empty after filtering malformed lines.")

df.to_csv("portfolio_snapshot.csv", index=False)
save_raw_flex_rows(raw_statement, "portfolio_snapshot_raw.csv")

print(f"Done! {len(df)} rows saved to portfolio_snapshot.csv")
print("Raw Flex rows saved to portfolio_snapshot_raw.csv")
if {"Symbol", "Quantity", "CostBasisPrice"}.issubset(df.columns):
    print(df[["Symbol", "Quantity", "CostBasisPrice"]].to_string(index=False))
else:
    print(df.head().to_string(index=False))