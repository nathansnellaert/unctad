import os

# Set environment variables for this run
os.environ['CONNECTOR_NAME'] = 'unctad'
os.environ['RUN_ID'] = 'local-dev'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['STORAGE_BACKEND'] = 'local'
os.environ['DATA_DIR'] = 'data'

# Test discovering all datasets
from assets.datasets.datasets import get_all_datasets

print("Testing UNCTAD dataset discovery...")
print("="*50)

try:
    datasets = get_all_datasets()
    print(f"✓ Successfully discovered {len(datasets)} datasets")
    
    # Show first 10 datasets
    print("\nFirst 10 datasets:")
    for i, (report_name, dataset_id, title) in enumerate(datasets[:10], 1):
        print(f"{i:3}. {dataset_id:30} | {title[:50]}")
    
    if len(datasets) > 10:
        print(f"     ... and {len(datasets) - 10} more datasets")
    
    # Test downloading one dataset
    if datasets:
        print("\n" + "="*50)
        print("Testing download of first dataset...")
        from general import download_dataset
        
        report_name, dataset_id, title = datasets[0]
        print(f"Downloading: {dataset_id} ({title})")
        
        try:
            data = download_dataset(report_name)
            print(f"✓ Successfully downloaded {data.num_rows:,} rows")
        except Exception as e:
            print(f"✗ Failed to download: {e}")
            
except Exception as e:
    print(f"✗ Error discovering datasets: {e}")
    import traceback
    traceback.print_exc()