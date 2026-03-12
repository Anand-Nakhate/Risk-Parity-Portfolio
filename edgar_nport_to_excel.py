"""
EDGAR N-PORT Holdings to Excel Extractor

Usage:
    python edgar_nport_to_excel.py PBAIX
    python edgar_nport_to_excel.py PBAIX --output holdings.xlsx

Extracts all holdings from the latest N-PORT filing for a given mutual fund ticker
and writes them to a formatted Excel file.

Requirements: pip install requests openpyxl
"""

import argparse
import json
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter

import openpyxl
import requests
from openpyxl.styles import Alignment, Font, PatternFill

# SEC requires identification
USER_AGENT = "EdgarNportExtractor your.email@example.com"
HEADERS = {"User-Agent": USER_AGENT}
RATE_LIMIT_DELAY = 0.15  # seconds between requests


def sec_get(url: str) -> requests.Response:
    """GET with rate limiting and User-Agent."""
    time.sleep(RATE_LIMIT_DELAY)
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp


def lookup_ticker(ticker: str) -> dict:
    """Look up CIK, series ID, and class ID for a mutual fund ticker."""
    print(f"Looking up ticker {ticker}...")
    resp = sec_get("https://www.sec.gov/files/company_tickers_mf.json")
    data = resp.json()["data"]
    for row in data:
        if len(row) >= 4 and str(row[3]).upper() == ticker.upper():
            return {"cik": str(row[0]), "series_id": row[1], "class_id": row[2], "ticker": row[3]}
    raise ValueError(f"Ticker '{ticker}' not found in SEC mutual fund tickers")


def find_latest_nport(series_id: str) -> dict:
    """Find the most recent NPORT-P filing for a given series ID."""
    print(f"Searching for latest NPORT-P filing for series {series_id}...")
    search_url = (
        f"https://efts.sec.gov/LATEST/search-index"
        f"?q=%22{series_id}%22&forms=NPORT-P"
        f"&dateRange=custom&startdt=2020-01-01&enddt=2030-12-31"
    )
    resp = sec_get(search_url)
    data = resp.json()
    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        raise ValueError(f"No NPORT-P filings found for series {series_id}")

    # Sort by file_date descending, pick most recent
    hits.sort(key=lambda h: h["_source"]["file_date"], reverse=True)
    src = hits[0]["_source"]
    return {
        "accession": src["adsh"],
        "file_date": src["file_date"],
        "period_ending": src["period_ending"],
        "cik": src["ciks"][0].lstrip("0"),
    }


def download_and_parse_nport(cik: str, accession: str) -> tuple:
    """Download N-PORT XML and parse into holdings list + metadata."""
    accession_clean = accession.replace("-", "")
    xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/primary_doc.xml"
    print(f"Downloading {xml_url}...")
    resp = sec_get(xml_url)
    root = ET.fromstring(resp.content)
    ns = root.tag.split("}")[0] + "}" if "}" in root.tag else ""

    def find_text(path):
        el = root.find(f".//{ns}{path}")
        return el.text.strip() if el is not None and el.text else ""

    metadata = {
        "series_name": find_text("seriesName"),
        "series_id": find_text("seriesId"),
        "reporting_date": find_text("repPdDate"),  # Portfolio snapshot date
        "fiscal_year_end": find_text("repPdEnd"),  # Fiscal year end (NOT reporting date)
        "net_assets": find_text("netAssets"),
        "total_assets": find_text("totAssets"),
        "total_liabilities": find_text("totLiabs"),
    }

    holdings = []
    for inv in root.findall(f".//{ns}invstOrSec"):
        def gt(tag, parent=None):
            p = parent if parent is not None else inv
            el = p.find(f"{ns}{tag}")
            return el.text.strip() if el is not None and el.text else ""

        # ISIN stored as attribute
        isin = ""
        isin_el = inv.find(f".//{ns}isin")
        if isin_el is not None:
            isin = isin_el.get("value", "")

        # Other identifier stored as attribute
        other_id = ""
        other_desc = ""
        other_el = inv.find(f".//{ns}identifiers/{ns}other")
        if other_el is not None:
            other_id = other_el.get("value", "")
            other_desc = other_el.get("otherDesc", "")

        # Debt security details
        debt = inv.find(f"{ns}debtSec")
        maturity = coupon_kind = coupon_rate = ""
        if debt is not None:
            maturity = gt("maturityDt", debt)
            coupon_kind = gt("couponKind", debt)
            coupon_rate = gt("annualizedRt", debt)

        holdings.append({
            "Name": gt("name"),
            "Title": gt("title"),
            "CUSIP": gt("cusip"),
            "ISIN": isin,
            "LEI": gt("lei"),
            "Other_ID": other_id,
            "Other_ID_Type": other_desc,
            "Balance": gt("balance"),
            "Units": gt("units"),
            "Currency": gt("curCd"),
            "Value_USD": gt("valUSD"),
            "Pct_of_NAV (%)": gt("pctVal"),
            "Payoff_Profile": gt("payoffProfile"),
            "Asset_Category": gt("assetCat"),
            "Issuer_Category": gt("issuerCat"),
            "Country": gt("invCountry"),
            "Is_Restricted": gt("isRestrictedSec"),
            "Fair_Value_Level": gt("fairValLevel"),
            "Maturity_Date": maturity,
            "Coupon_Kind": coupon_kind,
            "Coupon_Rate (%)": coupon_rate,
        })

    return holdings, metadata


def write_excel(holdings: list, metadata: dict, ticker: str, filing_info: dict, output_path: str):
    """Write holdings to a formatted Excel workbook."""
    wb = openpyxl.Workbook()
    header_fill = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    bold = Font(bold=True, size=11)
    title_font = Font(bold=True, size=13)

    # ===== Holdings Sheet =====
    ws = wb.active
    ws.title = f"{ticker} Holdings"

    header_keys = list(holdings[0].keys())
    for col, key in enumerate(header_keys, 1):
        cell = ws.cell(row=1, column=col, value=key)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    numeric_cols = {"Balance", "Value_USD", "Pct_of_NAV (%)", "Coupon_Rate (%)"}
    for row_idx, h in enumerate(holdings, 2):
        for col_idx, key in enumerate(header_keys, 1):
            val = h[key]
            if key in numeric_cols and val:
                try:
                    val = float(val)
                except ValueError:
                    pass
            ws.cell(row=row_idx, column=col_idx, value=val if val != "" else None)

    for row in ws.iter_rows(min_row=2, max_row=len(holdings) + 1):
        for cell in row:
            col_name = header_keys[cell.column - 1]
            if col_name == "Value_USD" and isinstance(cell.value, (int, float)):
                cell.number_format = "#,##0.00"
            elif "Pct" in col_name and isinstance(cell.value, (int, float)):
                cell.number_format = "0.0000"
            elif "Rate" in col_name and isinstance(cell.value, (int, float)):
                cell.number_format = "0.0000"
            elif col_name == "Balance" and isinstance(cell.value, (int, float)):
                cell.number_format = "#,##0.00"

    for col_cells in ws.columns:
        max_len = max(len(str(c.value or "")) for c in col_cells)
        ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 40)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    # ===== Summary Sheet =====
    ws2 = wb.create_sheet("Summary")
    net_assets = float(metadata["net_assets"]) if metadata["net_assets"] else 0
    total_val = sum(float(h["Value_USD"]) for h in holdings if h["Value_USD"])
    total_pct = sum(float(h["Pct_of_NAV (%)"]) for h in holdings if h["Pct_of_NAV (%)"])

    summary_rows = [
        ("FUND INFORMATION", "", True),
        ("Fund Name", metadata["series_name"], False),
        ("Ticker", ticker, False),
        ("Series ID", metadata["series_id"], False),
        ("", "", False),
        ("REPORTING PERIOD", "", True),
        ("Portfolio Snapshot Date", metadata["reporting_date"], False),
        ("Fiscal Year End", metadata["fiscal_year_end"], False),
        ("Filing Date", filing_info["file_date"], False),
        ("Filing Accession #", filing_info["accession"], False),
        ("Source", "SEC EDGAR NPORT-P", False),
        ("", "", False),
        ("PORTFOLIO SUMMARY", "", True),
        ("Net Assets", net_assets, False),
        ("Sum of Holdings (Value_USD)", total_val, False),
        ("Holdings as % of Net Assets", f"{total_val / net_assets * 100:.2f}%" if net_assets else "N/A", False),
        ("Sum of Pct_of_NAV", f"{total_pct:.2f}%", False),
        ("Total # Holdings", len(holdings), False),
        ("", "", False),
        ("POSITION BREAKDOWN", "", True),
        ("Long Positions", sum(1 for h in holdings if h["Payoff_Profile"] == "Long"), False),
        ("Short Positions", sum(1 for h in holdings if h["Payoff_Profile"] == "Short"), False),
        ("Derivatives (N/A profile)", sum(1 for h in holdings if h["Payoff_Profile"] == "N/A"), False),
    ]

    for i, (label, val, is_title) in enumerate(summary_rows, 1):
        cell_a = ws2.cell(row=i, column=1, value=label)
        cell_b = ws2.cell(row=i, column=2, value=val)
        cell_a.font = title_font if is_title else bold
        if isinstance(val, float) and val > 1000:
            cell_b.number_format = "$#,##0.00"

    ws2.column_dimensions["A"].width = 32
    ws2.column_dimensions["B"].width = 55

    # ===== Asset Category Sheet =====
    ws3 = wb.create_sheet("By Asset Category")
    cat_map = {
        "EC": "Equity - Common Stock",
        "EP": "Equity - Preferred Stock",
        "DIR": "Derivative - Interest Rate",
        "DE": "Derivative - Equity",
        "DFE": "Derivative - Foreign Exchange",
        "DCR": "Derivative - Credit",
        "DBT": "Derivative - Other/Basket",
        "STIV": "Short-Term Investment Vehicle",
        "ABS-CBDO": "ABS - Collateralized Bond/Debt",
        "ABS-MBS": "ABS - Mortgage-Backed Security",
        "UST": "US Treasury",
    }
    cats = Counter()
    cat_vals = {}
    for h in holdings:
        cat = h["Asset_Category"] or "(Uncategorized)"
        cats[cat] += 1
        cat_vals.setdefault(cat, 0)
        cat_vals[cat] += float(h["Value_USD"]) if h["Value_USD"] else 0

    for col, name in enumerate(["Code", "Category", "# Holdings", "Value USD", "% of NAV"], 1):
        c = ws3.cell(row=1, column=col, value=name)
        c.font = header_font
        c.fill = header_fill

    for i, (cat, count) in enumerate(cats.most_common(), 2):
        ws3.cell(row=i, column=1, value=cat)
        ws3.cell(row=i, column=2, value=cat_map.get(cat, cat))
        ws3.cell(row=i, column=3, value=count)
        ws3.cell(row=i, column=4, value=cat_vals[cat]).number_format = "#,##0.00"
        if net_assets:
            ws3.cell(row=i, column=5, value=cat_vals[cat] / net_assets * 100).number_format = "0.00"

    for w, width in [("A", 15), ("B", 42), ("C", 12), ("D", 20), ("E", 12)]:
        ws3.column_dimensions[w].width = width

    # ===== Country Sheet =====
    ws4 = wb.create_sheet("By Country")
    country_counts = Counter()
    country_vals = {}
    for h in holdings:
        c = h["Country"] or "(Unknown)"
        country_counts[c] += 1
        country_vals.setdefault(c, 0)
        country_vals[c] += float(h["Value_USD"]) if h["Value_USD"] else 0

    for col, name in enumerate(["Country", "# Holdings", "Value USD", "% of NAV"], 1):
        c = ws4.cell(row=1, column=col, value=name)
        c.font = header_font
        c.fill = header_fill

    for i, (country, count) in enumerate(country_counts.most_common(), 2):
        ws4.cell(row=i, column=1, value=country)
        ws4.cell(row=i, column=2, value=count)
        ws4.cell(row=i, column=3, value=country_vals[country]).number_format = "#,##0.00"
        if net_assets:
            ws4.cell(row=i, column=4, value=country_vals[country] / net_assets * 100).number_format = "0.00"

    for w, width in [("A", 12), ("B", 12), ("C", 20), ("D", 12)]:
        ws4.column_dimensions[w].width = width

    wb.save(output_path)
    return wb


def main():
    parser = argparse.ArgumentParser(description="Extract mutual fund holdings from SEC EDGAR N-PORT filings")
    parser.add_argument("ticker", help="Mutual fund ticker symbol (e.g., PBAIX)")
    parser.add_argument("--output", "-o", help="Output Excel file path (default: <TICKER>_holdings.xlsx)")
    parser.add_argument("--user-agent", help="Your name and email for SEC (required by SEC fair access policy)")
    args = parser.parse_args()

    global USER_AGENT, HEADERS
    if args.user_agent:
        USER_AGENT = args.user_agent
        HEADERS = {"User-Agent": USER_AGENT}

    ticker = args.ticker.upper()
    output_path = args.output or f"{ticker}_holdings.xlsx"

    # Step 1: Look up ticker
    info = lookup_ticker(ticker)
    print(f"Found: CIK={info['cik']}, Series={info['series_id']}, Class={info['class_id']}")

    # Step 2: Find latest filing
    filing = find_latest_nport(info["series_id"])
    print(f"Latest filing: {filing['file_date']}, Period: {filing['period_ending']}")

    # Step 3: Download and parse
    holdings, metadata = download_and_parse_nport(filing["cik"], filing["accession"])
    print(f"Parsed {len(holdings)} holdings")

    # Step 4: Write Excel
    write_excel(holdings, metadata, ticker, filing, output_path)
    print(f"\nSaved to {output_path}")
    print(f"  Fund: {metadata['series_name']}")
    print(f"  As of: {metadata['reporting_date']}")
    net = float(metadata["net_assets"]) if metadata["net_assets"] else 0
    print(f"  Net Assets: ${net:,.2f}")
    print(f"  Holdings: {len(holdings)}")


if __name__ == "__main__":
    main()
