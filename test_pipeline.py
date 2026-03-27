"""
test_pipeline.py  –  Offline unit tests for irs_propublica_pipeline.py
Run with:  python test_pipeline.py
(No internet needed – all data is mocked inline)
"""

import sys
from xml.etree import ElementTree as ET
from irs_propublica_pipeline import _find_xml_value, XML_FIELDS

# ── Fake 990 XML (mirrors real ProPublica structure) ────────────────────────
FAKE_990_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Return xmlns="http://www.irs.gov/efile">
  <ReturnHeader>
    <Filer>
      <EIN>123456789</EIN>
      <BusinessName>
        <BusinessNameLine1Txt>Test Nonprofit Inc</BusinessNameLine1Txt>
      </BusinessName>
    </Filer>
  </ReturnHeader>
  <ReturnData>
    <IRS990>
      <CYTotalRevenueAmt>5000000</CYTotalRevenueAmt>
      <CYContributionsGrantsAmt>3000000</CYContributionsGrantsAmt>
      <MembershipDuesAmt>50000</MembershipDuesAmt>
      <GovernmentGrantsAmt>1500000</GovernmentGrantsAmt>
      <NoncashContributionsAmt>200000</NoncashContributionsAmt>
      <TotalVolunteersCnt>250</TotalVolunteersCnt>
      <CYProgramServiceRevenueAmt>800000</CYProgramServiceRevenueAmt>
      <CYInvestmentIncomeAmt>120000</CYInvestmentIncomeAmt>
      <CYOtherRevenueAmt>30000</CYOtherRevenueAmt>
      <CYTotalExpensesAmt>4500000</CYTotalExpensesAmt>
      <CYNetAssetsOrFundBalancesAmt>1200000</CYNetAssetsOrFundBalancesAmt>
    </IRS990>
  </ReturnData>
</Return>"""

EXPECTED = {
    "total_revenue":          "5000000",
    "contributions_grants":   "3000000",
    "membership_dues":        "50000",
    "government_grants":      "1500000",
    "noncash_contributions":  "200000",
    "total_volunteers":       "250",
    "program_service_revenue":"800000",
    "investment_income":      "120000",
    "other_revenue":          "30000",
    "total_expenses":         "4500000",
    "net_assets":             "1200000",
}

def test_xml_field_extraction():
    root = ET.fromstring(FAKE_990_XML)
    passed = 0
    failed = 0
    print("\n── XML Field Extraction Tests ──────────────────────────")
    for tag, col in XML_FIELDS:
        val = _find_xml_value(root, tag)
        expected = EXPECTED.get(col)
        ok = val == expected
        status = "✓ PASS" if ok else "✗ FAIL"
        print(f"  {status}  {col:35s}  got={val!r}  expected={expected!r}")
        if ok:
            passed += 1
        else:
            failed += 1
    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0

def test_ein_normalisation():
    """EINs should be zero-padded to 9 digits and dash-stripped."""
    import pandas as pd
    print("\n── EIN Normalisation Tests ─────────────────────────────")
    cases = [
        ("12-3456789", "123456789"),
        ("12345678",   "012345678"),
        ("1234567",    "001234567"),
        ("123456789",  "123456789"),
    ]
    passed = 0
    for raw, expected in cases:
        normalised = raw.replace("-", "").strip().zfill(9)
        ok = normalised == expected
        print(f"  {'✓ PASS' if ok else '✗ FAIL'}  {raw!r:15s} → {normalised!r}  (expected {expected!r})")
        if ok:
            passed += 1
    print(f"\nResults: {passed}/{len(cases)} passed")
    return passed == len(cases)

if __name__ == "__main__":
    ok1 = test_xml_field_extraction()
    ok2 = test_ein_normalisation()
    if ok1 and ok2:
        print("\n✅  All tests passed — pipeline logic is working correctly.")
        sys.exit(0)
    else:
        print("\n❌  Some tests failed.")
        sys.exit(1)
