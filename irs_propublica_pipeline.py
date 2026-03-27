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
    --regions 1 2 3 4        which IRS regions to download (default: all 4)
    --limit 100              only process the first N EINs (for testing)
    --output results.xlsx    output filename (default: enriched_nonprofits.xlsx)
    --delay 0.5              seconds to wait between API calls (be polite!)
"""

import argparse
import csv
import io
import time
import sys
import logging
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
import pandas as pd
try:
    from tqdm import tqdm
except ImportError:
    # Fallback: plain iterator with no progress bar
    def tqdm(iterable, **kwargs):
        return iterable

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# IRS EO BMF regional CSV URLs
IRS_REGION_URLS = {
    1: "https://www.irs.gov/pub/irs-soi/eo1.csv",
    2: "https://www.irs.gov/pub/irs-soi/eo2.csv",
    3: "https://www.irs.gov/pub/irs-soi/eo3.csv",
    4: "https://www.irs.gov/pub/irs-soi/eo4.csv",
}

# ProPublica Nonprofit Explorer API base
PROPUBLICA_API_BASE = "https://projects.propublica.org/nonprofits/api/v2"

# XML fields to extract from the 990 filing
# Each entry: (xml_tag_name, output_column_name)
XML_FIELDS = [
    ("CYTotalRevenueAmt",        "total_revenue"),
    ("CYContributionsGrantsAmt", "contributions_grants"),
    ("MembershipDuesAmt",        "membership_dues"),
    ("GovernmentGrantsAmt",      "government_grants"),
    ("NoncashContributionsAmt",  "noncash_contributions"),
    ("TotalVolunteersCnt",       "total_volunteers"),
    # Bonus fields that are commonly useful
    ("CYProgramServiceRevenueAmt", "program_service_revenue"),
    ("CYInvestmentIncomeAmt",      "investment_income"),
    ("CYOtherRevenueAmt",          "other_revenue"),
    ("CYTotalExpensesAmt",         "total_expenses"),
    ("CYNetAssetsOrFundBalancesAmt","net_assets"),
]

# IRS CSV columns to keep (all others are kept too, these are just the key ones)
IRS_KEY_COLUMNS = [
    "EIN", "NAME", "ICO", "STREET", "CITY", "STATE", "ZIP",
    "GROUP", "SUBSECTION", "AFFILIATION", "CLASSIFICATION",
    "RULING", "DEDUCTIBILITY", "FOUNDATION", "ACTIVITY",
    "ORGANIZATION", "STATUS", "TAX_PERIOD", "ASSET_CD",
    "INCOME_CD", "FILING_REQ_CD", "PF_FILING_REQ_CD",
    "ACCT_PD", "ASSET_AMT", "INCOME_AMT", "REVENUE_AMT", "NTEE_CD",
    "SORT_NAME",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# STEP 1: Download IRS CSV files
# ─────────────────────────────────────────────

def download_irs_csv(region: int) -> pd.DataFrame:
    """Download one IRS EO BMF regional CSV and return it as a DataFrame."""
    url = IRS_REGION_URLS[region]
    log.info(f"Downloading IRS Region {region} from {url} ...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    # The IRS files use latin-1 encoding
    df = pd.read_csv(io.StringIO(resp.content.decode("latin-1")), dtype=str)
    df.columns = [c.strip().upper() for c in df.columns]
    df["_IRS_REGION"] = str(region)
    log.info(f"  → {len(df):,} rows in Region {region}")
    return df


def load_all_regions(regions: list[int]) -> pd.DataFrame:
    """Download and concatenate multiple IRS regions into one DataFrame."""
    frames = []
    for region in regions:
        frames.append(download_irs_csv(region))
    df = pd.concat(frames, ignore_index=True)
    # Normalise EIN: strip dashes, zero-pad to 9 digits
    df["EIN"] = df["EIN"].str.replace("-", "").str.strip().str.zfill(9)
    log.info(f"Total records loaded: {len(df):,}")
    return df


# ─────────────────────────────────────────────
# STEP 2: Query ProPublica API for an EIN
# ─────────────────────────────────────────────

def get_propublica_org(ein: str, session: requests.Session) -> dict | None:
    """
    Call the ProPublica Nonprofit Explorer API for one EIN.
    Returns the parsed JSON or None if not found / error.
    """
    url = f"{PROPUBLICA_API_BASE}/organizations/{ein}.json"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"ProPublica API error for EIN {ein}: {e}")
        return None


def get_latest_filing_xml_url(org_data: dict) -> tuple[str | None, str | None]:
    """
    From the ProPublica org JSON, pick the most recent 990 XML filing.
    Returns (xml_url, tax_period) or (None, None).
    """
    filings = org_data.get("filings_with_data", [])
    if not filings:
        return None, None

    # filings_with_data is sorted newest-first by ProPublica
    latest = filings[0]
    xml_url = latest.get("pdf_url") or latest.get("xml_url")

    # ProPublica API v2 exposes the S3 XML URL under different keys depending
    # on the endpoint version. Try both:
    xml_url = latest.get("xml_url") or latest.get("pdf_url")
    tax_period = latest.get("tax_prd_yr") or latest.get("tax_period")

    # If still no direct XML URL, construct from the filing object index_url
    # Format: https://pp-990-xml.s3.amazonaws.com/{object_id}_public.xml
    if not xml_url:
        object_id = latest.get("object_id")
        if object_id:
            xml_url = f"https://pp-990-xml.s3.amazonaws.com/{object_id}_public.xml"

    return xml_url, str(tax_period) if tax_period else None


# ─────────────────────────────────────────────
# STEP 3: Fetch and parse the 990 XML
# ─────────────────────────────────────────────

# The 990 XML uses a namespace — we search ignoring it with a wildcard
def _find_xml_value(root: ET.Element, tag: str) -> str | None:
    """
    Search the entire XML tree for a tag, ignoring namespace prefixes.
    Returns the text content or None.
    """
    # Try with namespace wildcard (works for any namespace)
    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if local == tag and elem.text:
            return elem.text.strip()
    return None


def parse_990_xml(xml_url: str, session: requests.Session) -> dict:
    """
    Download and parse a ProPublica 990 XML file.
    Returns a dict of {output_column_name: value}.
    """
    result = {col: None for _, col in XML_FIELDS}
    try:
        resp = session.get(xml_url, timeout=60)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for tag, col in XML_FIELDS:
            result[col] = _find_xml_value(root, tag)
    except ET.ParseError as e:
        log.warning(f"XML parse error ({xml_url}): {e}")
    except Exception as e:
        log.warning(f"Failed to fetch XML ({xml_url}): {e}")
    return result


# ─────────────────────────────────────────────
# STEP 4: Full enrichment pipeline
# ─────────────────────────────────────────────

def enrich_dataframe(df: pd.DataFrame, delay: float, limit: int | None) -> pd.DataFrame:
    """
    For each row in df, query ProPublica, fetch the XML, and append the fields.
    Returns the enriched DataFrame.
    """
    # Add output columns (all start as None)
    new_cols = ["_PP_TAX_YEAR", "_PP_XML_URL", "_PP_FOUND"] + [col for _, col in XML_FIELDS]
    for col in new_cols:
        df[col] = None

    eins = df["EIN"].tolist()
    if limit:
        eins = eins[:limit]
        log.info(f"Limiting to first {limit} EINs for testing")

    session = requests.Session()
    session.headers.update({"User-Agent": "nonprofit-research-pipeline/1.0"})

    for i, ein in enumerate(tqdm(eins, desc="Processing EINs", unit="org")):
        idx = df.index[df["EIN"] == ein][0]  # get the DataFrame row index

        # Query ProPublica
        org_data = get_propublica_org(ein, session)
        if not org_data or not org_data.get("organization"):
            df.at[idx, "_PP_FOUND"] = "No"
            time.sleep(delay)
            continue

        df.at[idx, "_PP_FOUND"] = "Yes"

        # Get XML URL of most recent filing
        xml_url, tax_year = get_latest_filing_xml_url(org_data)
        df.at[idx, "_PP_TAX_YEAR"] = tax_year
        df.at[idx, "_PP_XML_URL"] = xml_url

        if xml_url:
            xml_data = parse_990_xml(xml_url, session)
            for col, val in xml_data.items():
                df.at[idx, col] = val

        # Be polite to the API
        time.sleep(delay)

        # Progress log every 100 records
        if (i + 1) % 100 == 0:
            found = df["_PP_FOUND"].value_counts().get("Yes", 0)
            log.info(f"Progress: {i+1}/{len(eins)} | ProPublica matches so far: {found}")

    return df


# ─────────────────────────────────────────────
# STEP 5: Save output
# ─────────────────────────────────────────────

def save_output(df: pd.DataFrame, output_path: str):
    """Save the enriched DataFrame to Excel with some basic formatting."""
    path = Path(output_path)
    log.info(f"Saving to {path} ...")

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Enriched Data")

        # Auto-fit column widths (rough heuristic)
        ws = writer.sheets["Enriched Data"]
        for col_cells in ws.columns:
            max_len = max(
                (len(str(c.value)) for c in col_cells if c.value is not None),
                default=8
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 50)

    log.info(f"Done! Saved {len(df):,} rows to {path}")

    # Print a quick summary
    if "_PP_FOUND" in df.columns:
        found     = (df["_PP_FOUND"] == "Yes").sum()
        not_found = (df["_PP_FOUND"] == "No").sum()
        skipped   = df["_PP_FOUND"].isna().sum()
        log.info(f"\nSummary:")
        log.info(f"  ProPublica matches found : {found:,}")
        log.info(f"  Not found in ProPublica  : {not_found:,}")
        log.info(f"  Not processed (limit)    : {skipped:,}")


# ─────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="IRS EO BMF + ProPublica 990 Enrichment Pipeline")
    p.add_argument("--regions", nargs="+", type=int, default=[1, 2, 3, 4],
                   choices=[1, 2, 3, 4], help="IRS regions to process")
    p.add_argument("--limit", type=int, default=None,
                   help="Process only the first N EINs (useful for testing)")
    p.add_argument("--output", type=str, default="enriched_nonprofits.xlsx",
                   help="Output Excel file name")
    p.add_argument("--delay", type=float, default=0.5,
                   help="Seconds to wait between API calls (default: 0.5)")
    return p.parse_args()


def main():
    args = parse_args()

    log.info("=" * 60)
    log.info("IRS + ProPublica Nonprofit Enrichment Pipeline")
    log.info("=" * 60)
    log.info(f"Regions : {args.regions}")
    log.info(f"Limit   : {args.limit or 'None (all)'}")
    log.info(f"Output  : {args.output}")
    log.info(f"Delay   : {args.delay}s between calls")
    log.info("=" * 60)

    # Step 1: Load IRS data
    df = load_all_regions(args.regions)

    # Step 2–4: Enrich with ProPublica
    df = enrich_dataframe(df, delay=args.delay, limit=args.limit)

    # Step 5: Save
    save_output(df, args.output)


if __name__ == "__main__":
    main()
