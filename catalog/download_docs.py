#!/usr/bin/env python3
"""Download UNCTAD report documentation pages.

Usage:
    python catalog/download_docs.py              # Download all active reports
    python catalog/download_docs.py --all        # Include pending/backlog
    python catalog/download_docs.py --report US.CreativeGoodsValue  # Single report

Saves to data/docs/<report_id>.txt
"""
import argparse
import html
import json
import re
import sys
import time
from pathlib import Path

CATALOG_DIR = Path(__file__).parent
STATUS_FILE = CATALOG_DIR / "status.json"
DATA_DIR = CATALOG_DIR.parent / "data"
DOCS_DIR = DATA_DIR / "docs"
API_BASE = "https://unctadstat-api.unctad.org/api/datacenter/reports"


# Add src to path for subsets_utils
sys.path.insert(0, str(CATALOG_DIR.parent / "src"))
from subsets_utils import get


def load_status() -> dict:
    with open(STATUS_FILE) as f:
        return json.load(f)


def html_to_text(html_content: str) -> str:
    """Convert HTML to plain text."""
    if not html_content:
        return ""

    text = html_content

    # Replace <br>, </p>, </li> with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)

    # Add bullet points for list items
    text = re.sub(r'<li[^>]*>', '  - ', text, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities
    text = html.unescape(text)

    # Clean up whitespace
    lines = []
    for line in text.split('\n'):
        line = line.strip()
        if line:
            lines.append(line)

    return '\n'.join(lines)


def download_report_doc(report_id: str) -> tuple[bool, str]:
    """Download documentation for a single report. Returns (success, message)."""
    url = f"{API_BASE}/{report_id}/info/en"
    output_file = DOCS_DIR / f"{report_id}.txt"

    try:
        response = get(url)
        data = response.json()

        # Build text content
        parts = []

        # Title
        if data.get("Title"):
            parts.append(f"# {data['Title']}")

        # Category/Theme
        if data.get("Category"):
            parts.append(f"Theme: {data['Category']}")

        # Keywords
        if data.get("Keywords"):
            parts.append(f"Keywords: {data['Keywords']}")

        parts.append("")  # blank line

        # Notes (main documentation - contains HTML)
        if data.get("Notes"):
            notes_text = html_to_text(data["Notes"])
            parts.append(notes_text)

        parts.append("")

        # Measures
        if data.get("measures"):
            parts.append("## Measures")
            for m in data["measures"]:
                parts.append(f"  - {m['code']}: {m['label']}")

        # Dimensions
        if data.get("dims"):
            parts.append("")
            parts.append("## Dimensions")
            for d in data["dims"]:
                parts.append(f"  - {d['name']} ({d['label']})")

        content = '\n'.join(parts)

        if len(content) < 50:
            return False, "No content"

        output_file.write_text(content, encoding="utf-8")
        return True, f"{len(content):,} chars"

    except Exception as e:
        return False, str(e)[:50]


def main():
    parser = argparse.ArgumentParser(description="Download UNCTAD report docs")
    parser.add_argument("--all", action="store_true", help="Include pending/backlog reports")
    parser.add_argument("--report", help="Download single report by ID")
    args = parser.parse_args()

    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    status = load_status()
    reports = status.get("reports", {})

    # Determine which reports to process
    if args.report:
        if args.report not in reports:
            print(f"Report {args.report} not found in status.json")
            return 1
        target_reports = [args.report]
    elif args.all:
        target_reports = list(reports.keys())
    else:
        # Only active reports
        target_reports = [
            k for k, v in reports.items()
            if v.get("status") == "active"
        ]

    print(f"Downloading docs for {len(target_reports)} reports...")
    print(f"Output: {DOCS_DIR}/")
    print()

    success_count = 0
    fail_count = 0

    for i, report_id in enumerate(sorted(target_reports), 1):
        print(f"[{i}/{len(target_reports)}] {report_id}...", end=" ", flush=True)

        success, msg = download_report_doc(report_id)

        if success:
            print(f"OK ({msg})")
            success_count += 1
        else:
            print(f"FAIL ({msg})")
            fail_count += 1

        # Be nice to the server
        time.sleep(0.2)

    print()
    print(f"Done: {success_count} success, {fail_count} failed")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    exit(main())
