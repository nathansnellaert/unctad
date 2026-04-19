#!/usr/bin/env python3
"""Generate node scripts for all active UNCTAD datasets."""

import json
from pathlib import Path

BASE_DIR = Path(__file__).parent
CATALOG_STATUS = BASE_DIR / "catalog" / "status.json"
SRC_DIR = BASE_DIR / "src"

TEMPLATE = '''from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "{unctad_id}"
SUBSET_DATASET_ID = "{subset_id}"

METADATA = {{
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD {title}",
    "description": "{description}",
    "column_descriptions": {{
        "_year": "Year of observation",
        "economy": "Reporting economy",
    }},
}}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {{
    download: [],
    transform: [download],
}}

if __name__ == "__main__":
    download()
    transform()
'''

TEMPLATE_NO_FILTER = '''from connector_utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "{unctad_id}"
SUBSET_DATASET_ID = "{subset_id}"

METADATA = {{
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD {title}",
    "description": "{description}",
    "column_descriptions": {{
        "_year": "Year of observation",
        "economy": "Reporting economy or aggregate",
    }},
}}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {{
    download: [],
    transform: [download],
}}

if __name__ == "__main__":
    download()
    transform()
'''

def derive_subset_id(path: str) -> str:
    """Derive subset dataset ID from path.

    e.g., "nodes/commodity_prices_annual.py" -> "unctad_commodity_prices_annual"
    """
    filename = Path(path).stem  # e.g., "commodity_prices_annual"
    return f"unctad_{filename}"

def main():
    with open(CATALOG_STATUS) as f:
        catalog = json.load(f)

    reports = catalog.get("reports", {})

    generated = 0
    skipped = 0

    for unctad_id, info in reports.items():
        if info.get("status") != "active":
            skipped += 1
            continue

        path = info.get("path")
        if not path:
            print(f"WARNING: No path for active dataset {unctad_id}")
            continue

        subset_id = derive_subset_id(path)
        template = TEMPLATE_NO_FILTER if info.get("filter") is False else TEMPLATE
        content = template.format(
            unctad_id=unctad_id,
            subset_id=subset_id,
            title=derive_subset_id(path).replace("unctad_", "").replace("_", " ").title(),
            description=f"Data from UNCTAD dataset {unctad_id}.",
        )

        output_path = SRC_DIR / path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content)

        print(f"Generated: {output_path.relative_to(BASE_DIR)}")
        generated += 1

    print(f"\nDone: {generated} generated, {skipped} skipped (not active)")

if __name__ == "__main__":
    main()
