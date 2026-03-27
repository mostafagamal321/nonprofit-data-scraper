# IRS + ProPublica Nonprofit Enrichment Pipeline

Automates the full workflow described in the Upwork job:

1. Downloads IRS EO BMF CSV files for Regions 1–4
2. Queries ProPublica Nonprofit Explorer API for each EIN
3. Downloads and parses the 990 XML filing from ProPublica's S3 bucket
4. Extracts the specified financial fields
5. Outputs one enriched Excel file with all original IRS columns + new columns

---

## Setup

```bash
pip install requests lxml pandas openpyxl tqdm
```

---

## Usage

### Full run (all 4 regions, all EINs):
```bash
python irs_propublica_pipeline.py
```

### Test run (first 50 EINs from Region 1 only):
```bash
python irs_propublica_pipeline.py --regions 1 --limit 50 --output test_output.xlsx
```

### Custom options:
```bash
python irs_propublica_pipeline.py \
  --regions 1 2 3 4 \
  --delay 0.5 \
  --output enriched_nonprofits.xlsx
```

| Flag        | Default                    | Description                              |
|-------------|----------------------------|------------------------------------------|
| `--regions` | 1 2 3 4                    | Which IRS regions to include             |
| `--limit`   | None (all)                 | Process only first N EINs (for testing)  |
| `--output`  | enriched_nonprofits.xlsx   | Output filename                          |
| `--delay`   | 0.5                        | Seconds between API calls (be polite!)   |

---

## Output Columns Added

| Column                   | XML Source Tag               | Description                     |
|--------------------------|------------------------------|---------------------------------|
| `_PP_FOUND`              | (API flag)                   | Yes/No — was EIN on ProPublica? |
| `_PP_TAX_YEAR`           | (API metadata)               | Most recent filing tax year     |
| `_PP_XML_URL`            | (API metadata)               | S3 URL of the XML parsed        |
| `total_revenue`          | CYTotalRevenueAmt            | Current year total revenue      |
| `contributions_grants`   | CYContributionsGrantsAmt     | Contributions & grants          |
| `membership_dues`        | MembershipDuesAmt            | Membership dues                 |
| `government_grants`      | GovernmentGrantsAmt          | Government grants               |
| `noncash_contributions`  | NoncashContributionsAmt      | Non-cash contributions          |
| `total_volunteers`       | TotalVolunteersCnt           | Total volunteer count           |
| `program_service_revenue`| CYProgramServiceRevenueAmt   | Program service revenue         |
| `investment_income`      | CYInvestmentIncomeAmt        | Investment income               |
| `other_revenue`          | CYOtherRevenueAmt            | Other revenue                   |
| `total_expenses`         | CYTotalExpensesAmt           | Total expenses                  |
| `net_assets`             | CYNetAssetsOrFundBalancesAmt | Net assets / fund balances      |

---

## Notes

- **Rate limiting**: The default 0.5s delay is respectful. Don't go below 0.2s.
- **Missing records**: EINs not found on ProPublica are kept in the output with `_PP_FOUND = No` and blank financial columns — they are never dropped.
- **XML namespace**: The parser uses a wildcard tag search so it works regardless of which IRS namespace version is used in the XML.
- **Scale**: The IRS BMF files contain ~1.8 million EINs across all 4 regions. A full run at 0.5s/EIN takes ~10 days. Use `--limit` to process a subset, or parallelize with `concurrent.futures` for production use.

---

## Running Tests (no internet needed)

```bash
python test_pipeline.py
```

Verifies all 11 XML field extractions and EIN normalization logic offline.
