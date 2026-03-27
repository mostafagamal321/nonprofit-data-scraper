"""
IRS EO BMF + ProPublica 990 XML Enrichment Pipeline
=====================================================
What this script does:
  1. Downloads IRS EO BMF CSV files for Regions 1–4
  2. For each EIN in those files, queries the ProPublica Nonprofit Explorer API
  3. Fetches the most recent 990 XML filing from the ProPublica S3 bucket
  4. Extracts the target financial fields from the XML
  5. Appends those fields as new columns to the original CSV rows
  6. Saves the enriched output as a single Excel file

Usage:
  pip install requests lxml pandas openpyxl tqdm
  python irs_propublica_pipeline.py

  Optional flags:
    --regions 1 2              which IRS 1 , 2 
    --limit 100              only process the first N EINs (for testing)
    --output results.xlsx    output filename (default: enriched_nonprofits.xlsx)
    --delay 0.5              seconds to wait between API calls (be polite!)
"""

import argparse
import io
import time
import sys
import logging
from pathlib import Path
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(x, **kwargs): return x


# ─────────────────────────────
# CONFIG
# ─────────────────────────────

IRS_REGION_URLS = {
    1: "https://www.irs.gov/pub/irs-soi/eo1.csv",
    2: "https://www.irs.gov/pub/irs-soi/eo2.csv",
    3: "https://www.irs.gov/pub/irs-soi/eo3.csv",
    4: "https://www.irs.gov/pub/irs-soi/eo4.csv",
}

PROPUBLICA_API_BASE = "https://projects.propublica.org/nonprofits/api/v2"

XML_FIELDS = [
    ("CYTotalRevenueAmt", "total_revenue"),
    ("CYContributionsGrantsAmt", "contributions_grants"),
    ("TotalVolunteersCnt", "total_volunteers"),
    ("CYTotalExpensesAmt", "total_expenses"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


# ─────────────────────────────
# DOWNLOAD IRS DATA
# ─────────────────────────────

def download_irs_csv(region):
    url = IRS_REGION_URLS[region]
    log.info(f"Downloading Region {region}...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.content.decode("latin-1")), dtype=str)
    df.columns = [c.strip().upper() for c in df.columns]
    return df


def load_all_regions(regions):
    frames = [download_irs_csv(r) for r in regions]
    df = pd.concat(frames, ignore_index=True)

    df["EIN"] = df["EIN"].str.replace("-", "").str.strip().str.zfill(9)

    log.info(f"Loaded {len(df):,} rows")
    return df


# ─────────────────────────────
# API + XML
# ─────────────────────────────

def get_propublica_org(ein, session):
    try:
        url = f"{PROPUBLICA_API_BASE}/organizations/{ein}.json"
        resp = session.get(url, timeout=20)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_xml_url(org_data):
    filings = org_data.get("filings_with_data", [])
    if not filings:
        return None, None

    latest = filings[0]

    xml_url = latest.get("xml_url")
    tax_year = latest.get("tax_prd_yr") or latest.get("tax_period")

    if not xml_url:
        object_id = latest.get("object_id")
        if object_id:
            xml_url = f"https://pp-990-xml.s3.amazonaws.com/{object_id}_public.xml"

    return xml_url, tax_year


def extract_xml(root, tag):
    for elem in root.iter():
        if elem.tag.endswith(tag) and elem.text:
            return elem.text.strip()
    return None


def parse_xml(xml_url, session):
    result = {col: None for _, col in XML_FIELDS}

    try:
        resp = session.get(xml_url, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)

        for tag, col in XML_FIELDS:
            result[col] = extract_xml(root, tag)

    except Exception:
        pass

    return result


# ─────────────────────────────
# WORKER
# ─────────────────────────────

def process_ein(ein, session, delay):
    result = {
        "EIN": ein,
        "_PP_FOUND": "No",
        "_PP_TAX_YEAR": None,
        "_PP_XML_URL": None,
    }
    result.update({col: None for _, col in XML_FIELDS})

    org = get_propublica_org(ein, session)
    if not org or not org.get("organization"):
        return result

    result["_PP_FOUND"] = "Yes"

    xml_url, tax_year = get_xml_url(org)
    result["_PP_TAX_YEAR"] = tax_year
    result["_PP_XML_URL"] = xml_url

    if xml_url and xml_url.endswith(".xml"):
        result.update(parse_xml(xml_url, session))

    time.sleep(delay)
    return result


# ─────────────────────────────
# ENRICH (MULTITHREADED)
# ─────────────────────────────

def enrich(df, delay=0.05, limit=None):
    if limit:
        df = df.head(limit)

    # Remove duplicates (huge speed boost)
    df = df.drop_duplicates(subset="EIN")

    eins = df["EIN"].tolist()

    session = requests.Session()
    session.headers.update({"User-Agent": "fast-pipeline/1.0"})

    results = []

    MAX_WORKERS = 10

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_ein, ein, session, delay) for ein in eins]

        for f in tqdm(as_completed(futures), total=len(futures)):
            results.append(f.result())

    result_df = pd.DataFrame(results)

    df = df.merge(result_df, on="EIN", how="left")

    return df


# ─────────────────────────────
# SAVE
# ─────────────────────────────

def save(df, path):
    log.info(f"Saving to {path}")
    df.to_excel(path, index=False)
    log.info("Done!")


# ─────────────────────────────
# MAIN
# ─────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--regions", nargs="+", type=int, default=[1,2,3,4])
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--delay", type=float, default=0.05)
    parser.add_argument("--output", default="fast_output.xlsx")
    args = parser.parse_args()

    df = load_all_regions(args.regions)
    df = enrich(df, delay=args.delay, limit=args.limit)
    save(df, args.output)


if __name__ == "__main__":
    main()