"""Sync UNCTAD catalog metadata to status.json."""
from pathlib import Path

from subsets_utils import get
from subsets_utils.catalog import sync_catalog

CATALOG_URL = "https://unctadstat-api.unctad.org/api/datacenter/en"
STATUS_FILE = Path(__file__).parent / "status.json"


def fetch_catalog() -> dict:
    data = get(CATALOG_URL).json()
    reports = {}

    def walk(folders):
        for folder in folders:
            for report in folder.get("reports", []):
                reports[report["reportName"]] = {
                    "title": report.get("reportTitle", ""),
                    "metadata": report,
                }
            if folder.get("subFolders"):
                walk(folder["subFolders"])

    walk(data)
    return reports


def sync():
    sync_catalog(fetch_catalog(), CATALOG_URL, STATUS_FILE)


if __name__ == "__main__":
    sync()
