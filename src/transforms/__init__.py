"""UNCTAD transforms orchestrator."""

import importlib
from pathlib import Path


def get_all_datasets() -> list[str]:
    """Return all dataset names (directory names under transforms/)."""
    transforms_dir = Path(__file__).parent
    return sorted([
        d.name for d in transforms_dir.iterdir()
        if d.is_dir() and not d.name.startswith("_")
    ])


def run_one(dataset_name: str):
    """Run a single transform."""
    module = importlib.import_module(f"transforms.{dataset_name}.main")
    module.run()


def run_all():
    """Run all transforms."""
    datasets = get_all_datasets()
    completed = 0
    skipped = 0
    failed = 0

    for i, dataset_name in enumerate(datasets, 1):
        print(f"[{i}/{len(datasets)}] {dataset_name}...")
        try:
            run_one(dataset_name)
            completed += 1
        except NotImplementedError:
            print(f"  Skipped (not implemented)")
            skipped += 1
        except Exception as e:
            print(f"  Failed: {e}")
            failed += 1

    print(f"\nTransforms complete: {completed} succeeded, {skipped} skipped, {failed} failed")
