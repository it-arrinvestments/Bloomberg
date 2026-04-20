import time
import requests
import xml.etree.ElementTree as ET
import io
import csv

TOKEN    = "174790321327240887207054"    # from Client Portal > Settings: Flex Web Service
QUERY_ID = "1479361"  # Activity/Query ID from your saved Flex Query

BASE_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"


def save_raw_flex_rows(raw_statement: str, output_path: str) -> int:
    rows = list(csv.reader(io.StringIO(raw_statement)))
    with open(output_path, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerows(rows)
    return len(rows)


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

# ── Step 3: Save Flex CSV stream as-is ────────────────────────────────────────
raw_statement = get_resp.text.strip()
if not raw_statement:
    raise RuntimeError("GetStatement returned an empty response.")

OUTPUT = "portfolio_snapshot_raw.csv"
n = save_raw_flex_rows(raw_statement, OUTPUT)
print(f"Done! {n} rows saved to {OUTPUT}")
