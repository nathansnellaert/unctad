#!/usr/bin/env python3
"""Sync UNCTAD catalog and detect drift.

Usage:
    python catalog/sync.py

Status values:
    active   - Has node file, running in production
    backlog  - Want it, not yet implemented
    pending  - New from catalog, needs triage
    ignored  - Deliberately excluded (with reason)
"""
import json
import sys
from datetime import datetime
from pathlib import Path

CATALOG_URL = "https://unctadstat-api.unctad.org/api/datacenter/en"
CATALOG_DIR = Path(__file__).parent
STATUS_FILE = CATALOG_DIR / "status.json"


def fetch_catalog() -> list:
    """Fetch catalog from UNCTAD API."""
    sys.path.insert(0, str(CATALOG_DIR.parent / "src"))
    from subsets_utils import get

    print(f"Fetching {CATALOG_URL}...")
    response = get(CATALOG_URL)
    return response.json()


def extract_reports(catalog_data: list) -> dict:
    """Extract report name -> title mapping from catalog."""
    reports = {}

    def walk(folders):
        for folder in folders:
            for report in folder.get("reports", []):
                reports[report["reportName"]] = report["reportTitle"]
            if folder.get("subFolders"):
                walk(folder["subFolders"])

    walk(catalog_data)
    return reports


def load_status() -> dict:
    """Load status.json."""
    if STATUS_FILE.exists():
        with open(STATUS_FILE) as f:
            return json.load(f)
    return {"_meta": {}, "reports": {}}


def save_status(status: dict):
    """Save status.json."""
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)


def sync():
    """Sync catalog and detect drift."""
    catalog_data = fetch_catalog()
    catalog_reports = extract_reports(catalog_data)
    print(f"Found {len(catalog_reports)} reports in catalog")

    status = load_status()
    known_reports = set(status.get("reports", {}).keys())

    # Detect changes
    new_reports = set(catalog_reports.keys()) - known_reports
    removed_reports = known_reports - set(catalog_reports.keys())

    # Add new reports as pending
    if new_reports:
        for name in new_reports:
            status["reports"][name] = {"status": "pending"}
        status["_meta"]["last_synced"] = datetime.now().isoformat() + "Z"
        save_status(status)

    # Count by status
    counts = {"active": 0, "backlog": 0, "pending": 0, "ignored": 0}
    for info in status["reports"].values():
        s = info.get("status", "unknown")
        counts[s] = counts.get(s, 0) + 1

    # Print report
    total = len(catalog_reports)
    print()
    print("=" * 60)
    print(f"UNCTAD Coverage: {counts['active']}/{total} ({100*counts['active']/total:.0f}%)")
    print("=" * 60)
    print(f"  Active:  {counts['active']}")
    print(f"  Backlog: {counts['backlog']}")
    print(f"  Pending: {counts['pending']}")
    print(f"  Ignored: {counts['ignored']}")

    warnings = []

    if new_reports:
        print()
        print(f"⚠️  {len(new_reports)} NEW REPORTS:")
        for name in sorted(new_reports):
            print(f"    + {name}")
            print(f"      {catalog_reports[name][:70]}")
        warnings.append(f"{len(new_reports)} new reports need triage")

    if removed_reports:
        print()
        print(f"⚠️  {len(removed_reports)} REMOVED from catalog:")
        for name in sorted(removed_reports):
            print(f"    - {name}")
        warnings.append(f"{len(removed_reports)} reports removed")

    # Show pending
    pending = [k for k, v in status["reports"].items() if v.get("status") == "pending"]
    if pending:
        print()
        print(f"⚠️  {len(pending)} PENDING triage:")
        for name in sorted(pending)[:10]:
            title = catalog_reports.get(name, name)
            print(f"    ? {name}")
            print(f"      {title[:70]}")
        if len(pending) > 10:
            print(f"    ... and {len(pending) - 10} more")

    # Show backlog
    backlog = [k for k, v in status["reports"].items() if v.get("status") == "backlog"]
    if backlog:
        print()
        print(f"📋 {len(backlog)} in BACKLOG:")
        for name in sorted(backlog)[:5]:
            title = catalog_reports.get(name, name)
            print(f"    ○ {name}")
            print(f"      {title[:70]}")
        if len(backlog) > 5:
            print(f"    ... and {len(backlog) - 5} more")

    if warnings:
        print()
        print("WARNINGS:")
        for w in warnings:
            print(f"  - {w}")
        return 1

    if pending:
        return 1

    print()
    print("✓ All reports triaged")
    return 0


if __name__ == "__main__":
    sys.exit(sync())
