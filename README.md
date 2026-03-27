# IRS + ProPublica Nonprofit Enrichment Pipeline

This tool automates the heavy lifting of nonprofit data research. It bridges the gap between the **IRS Business Master File (BMF)**—which tells you who exists—and **ProPublica’s Nonprofit Explorer**, which tells you how they are performing financially.

---

## Project Overview / Demo Script
[![Watch the Demo](https://img.youtube.com/vi/go54ghYSZC4/0.jpg)](https://www.youtube.com/watch?v=go54ghYSZC4)


**The Workflow:**
1.  **Ingestion:** The script first pulls the latest IRS Business Master File (BMF) CSVs, which contain foundational data like EINs, addresses, and subsection codes.
2.  **API Integration:** It iterates through those EINs and queries the ProPublica API to locate the most recent electronic Form 990 filing.
3.  **XML Parsing:** Once found, the pipeline fetches the raw XML filing from an S3 bucket. It uses a flexible, wildcard-based parsing logic to extract deep financial metrics (like total revenue, volunteer counts, and net assets) across different IRS XML versions.
4.  **Synthesis:** Finally, it joins the IRS metadata with the ProPublica financial data, outputting a single, high-fidelity Excel master list ready for analysis.

Instead of manually searching for one nonprofit at a time, this tool allows researchers to audit thousands of organizations in a single automated pass."

---

## Features

* **Regional Batching:** Download IRS EO BMF CSV files for selected regions (1–4).
* **API Querying:** Queries ProPublica API for each EIN.
* **Smart Parsing:** Fetches and parses the most recent 990 XML filings from ProPublica’s S3 bucket.
* **Deep Financial Extraction:** Extracts key financial fields such as revenue, grants, expenses, and net assets.
* **Unified Output:** Outputs a single Excel file combining all original IRS columns with new ProPublica columns.

---

##  Setup

```bash
pip install requests lxml pandas openpyxl tqdm
```
## Usage
### Full Production Run
Processes all regions and all available EINs.
```bash
python irs_propublica_pipeline.py
```
### Targeted Test Run
Perfect for verifying logic on a small sample (first 50 EINs from Region 1)
```bash
python irs_propublica_pipeline.py --regions 1 --limit 50 --output test_output.xlsx
```
###Custom Options
```bash python irs_propublica_pipeline.py \
  --regions 1 2 3 4 \
  --delay 0.5 \
  --output enriched_nonprofits.xlsx
```
Flag,Default,Description
--regions,1 2 3 4,IRS regions to include
--limit,None (all),Only process first N EINs (for testing)
--output,enriched_nonprofits.xlsx,Output Excel filename
--delay,0.5,Seconds between API calls (politeness)
#  Output Columns Added
Column,XML Source Tag,Description
_PP_FOUND,(API flag),Yes/No — was EIN found on ProPublica?
_PP_TAX_YEAR,(API metadata),Most recent filing tax year
_PP_XML_URL,(API metadata),S3 URL of the XML parsed
total_revenue,CYTotalRevenueAmt,Current year total revenue
contributions_grants,CYContributionsGrantsAmt,Contributions & grants
membership_dues,MembershipDuesAmt,Membership dues
government_grants,GovernmentGrantsAmt,Government grants
noncash_contributions,NoncashContributionsAmt,Non-cash contributions
total_volunteers,TotalVolunteersCnt,Total volunteer count
program_service_revenue,CYProgramServiceRevenueAmt,Program service revenue
investment_income,CYInvestmentIncomeAmt,Investment income
other_revenue,CYOtherRevenueAmt,Other revenue
total_expenses,CYTotalExpensesAmt,Total expenses
net_assets,CYNetAssetsOrFundBalancesAmt,Net assets / fund balances
# Running Tests (offline)
```bash
python test_pipeline.py
```
