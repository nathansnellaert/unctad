#!/usr/bin/env python3
"""
Process a single UNCTAD dataset by URL.
Designed to be called as a subprocess to avoid memory issues.
"""
import os
import sys
import argparse
import json
from datetime import datetime
from utils import upload_data, save_state, validate_environment
from assets.datasets.datasets import process_single_dataset

def main():
    parser = argparse.ArgumentParser(description='Process a single UNCTAD dataset')
    parser.add_argument('dataset_url', type=str, help='UNCTAD dataset URL')
    parser.add_argument('dataset_name', type=str, help='Dataset name for storage')
    
    args = parser.parse_args()
    
    # Set up environment
    os.environ['CONNECTOR_NAME'] = 'unctad'
    if not os.environ.get('RUN_ID'):
        os.environ['RUN_ID'] = f'dataset-{args.dataset_name}-{datetime.now().strftime("%Y%m%d%H%M%S")}'
    
    validate_environment()
    
    # Process the dataset
    print(f"Processing {args.dataset_name} from {args.dataset_url}...")
    data = process_single_dataset(args.dataset_url, args.dataset_name)
    
    if data.num_rows == 0:
        print(f"No data found for {args.dataset_name}")
        sys.exit(0)
    
    # Upload the data
    print(f"Uploading {data.num_rows:,} rows...")
    upload_data(data, args.dataset_name)
    
    # Save state
    save_state(args.dataset_name, {
        "last_updated": datetime.now().isoformat(),
        "row_count": data.num_rows,
        "url": args.dataset_url
    })
    
    print(f"✓ Successfully processed {args.dataset_name}: {data.num_rows:,} rows")

if __name__ == "__main__":
    main()