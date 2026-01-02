import argparse

from subsets_utils import validate_environment
from ingest import catalogue as ingest_catalogue
from ingest import datasets as ingest_datasets
import transforms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from UNCTAD API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    parser.add_argument("--dataset", type=str, help="Run single dataset transform")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_catalogue.run()
        ingest_datasets.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        if args.dataset:
            transforms.run_one(args.dataset)
        else:
            transforms.run_all()

    print("\n=== UNCTAD connector complete ===")


if __name__ == "__main__":
    main()
