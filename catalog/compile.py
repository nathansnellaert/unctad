#!/usr/bin/env python3
"""Compile status.json into node files and main.py.

Usage:
    python catalog/compile.py           # Generate nodes + main.py
    python catalog/compile.py --dry-run # Preview without writing
"""
import argparse
import json
from pathlib import Path

CATALOG_DIR = Path(__file__).parent
STATUS_FILE = CATALOG_DIR / "status.json"
NODES_DIR = CATALOG_DIR.parent / "src" / "nodes"
MAIN_FILE = CATALOG_DIR.parent / "src" / "main.py"


def load_status() -> dict:
    with open(STATUS_FILE) as f:
        return json.load(f)


NODE_TEMPLATE = '''"""Download and transform {title}."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "{report_name}"
DATASET_ID = "{dataset_id}"


def download():
    """Download {report_name} from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "{node_name}")
    print(f"  Downloaded {{REPORT}}: {{table.num_rows:,}} rows")


def transform():
    """Transform and upload {dataset_id}."""
    table = load_raw_parquet("{node_name}")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {{DATASET_ID}}: {{table.num_rows:,}} rows")
'''


MAIN_TEMPLATE = '''"""UNCTAD connector - Generated from catalog/status.json.

Do not edit directly. Run: python catalog/compile.py
"""
from subsets_utils import DAG, validate_environment
{imports}

workflow = DAG({{
{dag_entries}
}})


def main():
    validate_environment()
    workflow.run()


if __name__ == "__main__":
    main()
'''


def compile(dry_run: bool = False):
    status = load_status()

    # Get active reports with their existing paths
    active_reports = []
    for report_name, info in status["reports"].items():
        if info.get("status") == "active" and info.get("path"):
            # Extract node name from path like "nodes/biotrade_concentration.py"
            path = info["path"]
            node_name = Path(path).stem  # biotrade_concentration
            active_reports.append((report_name, node_name, info))

    print(f"Compiling {len(active_reports)} active reports...")

    # Generate node files
    generated_nodes = []
    for report_name, node_name, info in sorted(active_reports, key=lambda x: x[1]):
        dataset_id = f"unctad_{node_name}"

        content = NODE_TEMPLATE.format(
            title=report_name.replace("US.", ""),
            report_name=report_name,
            dataset_id=dataset_id,
            node_name=node_name,
        )

        node_file = NODES_DIR / f"{node_name}.py"
        generated_nodes.append(node_name)

        if dry_run:
            print(f"  {node_name}.py")
        else:
            node_file.write_text(content)
            print(f"  Wrote: {node_name}.py")

    # Generate __init__.py
    init_content = f'"""UNCTAD nodes - Generated."""\n'
    init_file = NODES_DIR / "__init__.py"
    if not dry_run:
        init_file.write_text(init_content)

    # Generate main.py
    imports = "\n".join(f"from nodes import {name}" for name in sorted(generated_nodes))

    dag_entries = []
    for name in sorted(generated_nodes):
        dag_entries.append(f"    {name}.download: [],")
    dag_entries.append("")
    for name in sorted(generated_nodes):
        dag_entries.append(f"    {name}.transform: [{name}.download],")

    main_content = MAIN_TEMPLATE.format(
        imports=imports,
        dag_entries="\n".join(dag_entries),
    )

    if dry_run:
        print(f"\nmain.py preview:\n")
        lines = main_content.split('\n')
        for line in lines[:30]:
            print(line)
        print("...")
    else:
        MAIN_FILE.write_text(main_content)
        print(f"\nWrote: main.py")

    print(f"\nDone! {len(generated_nodes)} nodes")


def main():
    parser = argparse.ArgumentParser(description="Compile status.json to nodes")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    compile(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
