import os
os.environ['CONNECTOR_NAME'] = 'unctad'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

import subprocess
import sys
import platform
import resource
from pathlib import Path
from datetime import datetime
from utils import validate_environment, upload_data, load_state, save_state
from assets.catalogue.catalogue import process_catalogue

def get_memory_limit():
    """
    Get memory limit for subprocesses from MAX_PROCESS_MEMORY env var.
    Defaults to 5GB if not set.
    """
    # Check for environment variable (expects value in GB)
    max_memory_gb = os.getenv('MAX_PROCESS_MEMORY', '5')
    
    try:
        memory_limit_gb = float(max_memory_gb)
        memory_limit = int(memory_limit_gb * 1024 * 1024 * 1024)  # Convert GB to bytes
        return memory_limit
    except ValueError:
        # Default to 5GB if parsing fails
        print(f"Warning: Invalid MAX_PROCESS_MEMORY value '{max_memory_gb}', using 5GB default")
        return 5 * 1024 * 1024 * 1024

def process_dataset_subprocess(dataset_url: str, dataset_name: str) -> bool:
    """
    Process a dataset in a subprocess with memory constraints.
    
    Returns:
        True if successful, False otherwise
    """
    memory_limit = get_memory_limit()
    memory_limit_gb = memory_limit / (1024 * 1024 * 1024)
    
    cmd = [
        sys.executable,
        "process_dataset.py",
        dataset_url,
        dataset_name
    ]
    
    try:
        # Set up environment with memory limit info
        env = os.environ.copy()
        env['MEMORY_LIMIT_BYTES'] = str(memory_limit)
        
        # Platform-specific memory limiting
        if platform.system() == 'Linux':
            # On Linux, use RLIMIT_AS (address space) to enforce memory limit
            preexec_fn = lambda: resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
        elif platform.system() == 'Darwin':
            # On macOS, RLIMIT_AS doesn't work well, use RLIMIT_DATA instead
            # Note: This is less strict but prevents most OOM situations
            preexec_fn = lambda: resource.setrlimit(resource.RLIMIT_DATA, (memory_limit, memory_limit))
        else:
            preexec_fn = None
        
        # Run in subprocess with memory constraints
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=1200,  # 20 minute timeout per dataset
            env=env,
            preexec_fn=preexec_fn
        )
        
        # Print subprocess output
        if result.stdout:
            print(result.stdout.strip())
        if result.stderr and result.returncode != 0:
            # Check for memory-related errors
            if "MemoryError" in result.stderr or "Cannot allocate memory" in result.stderr:
                print(f"✗ Out of memory processing {dataset_name} (limit: {memory_limit_gb:.1f}GB)")
            else:
                print(f"Error: {result.stderr.strip()}", file=sys.stderr)
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout processing {dataset_name}")
        return False
    except Exception as e:
        print(f"✗ Error processing {dataset_name}: {e}")
        return False

def main():
    validate_environment()
    
    # Display memory configuration
    memory_limit = get_memory_limit()
    memory_limit_gb = memory_limit / (1024 * 1024 * 1024)
    max_memory_env = os.getenv('MAX_PROCESS_MEMORY', '5')
    print(f"🧮 Memory limit per dataset: {memory_limit_gb:.1f}GB (MAX_PROCESS_MEMORY={max_memory_env})")
    
    # Process catalogue to get list of all datasets
    print("Fetching UNCTAD catalogue...")
    catalogue_data = process_catalogue()
    upload_data(catalogue_data, "unctad_catalogue")
    save_state("catalogue", {
        "last_updated": datetime.now().isoformat(),
        "dataset_count": len(catalogue_data)
    })
    
    print(f"Found {len(catalogue_data)} datasets in catalogue")
    
    # Check state for each dataset
    datasets_to_process = []
    up_to_date_datasets = []
    
    # Convert catalogue to dict for easier processing
    datasets_dict = {row['dataset_id']: row for row in catalogue_data.to_pylist()}
    
    for _, row in datasets_dict.items():
        dataset_url = row['dataset_url']
        dataset_name = row['dataset_name']
        
        state = load_state(dataset_name)
        if state and 'last_updated' in state:
            last_updated = datetime.fromisoformat(state['last_updated'])
            days_ago = (datetime.now() - last_updated).days
            if days_ago < 30:
                up_to_date_datasets.append((dataset_name, days_ago))
                continue
        
        datasets_to_process.append((dataset_url, dataset_name))
    
    # Print summary
    print(f"\n📊 Dataset Status Summary:")
    print(f"  ✓ Up-to-date (< 30 days): {len(up_to_date_datasets)}")
    print(f"  ⏳ To process: {len(datasets_to_process)}")
    
    if not datasets_to_process:
        print("\n✅ All datasets are up to date!")
        return
    
    print(f"\n🚀 Processing {len(datasets_to_process)} datasets...")
    
    # Process each dataset in a subprocess
    successful = []
    failed = []
    
    for i, (dataset_url, dataset_name) in enumerate(datasets_to_process, 1):
        print(f"\n[{i}/{len(datasets_to_process)}] Processing {dataset_name}...")
        
        success = process_dataset_subprocess(dataset_url, dataset_name)
        
        if success:
            successful.append(dataset_name)
        else:
            failed.append(dataset_name)
    
    # Print final summary
    print("\n" + "="*50)
    print("📊 UNCTAD Connector Summary")
    print("="*50)
    
    if successful:
        print(f"\n✅ Successfully processed {len(successful)} datasets")
        for name in successful[:10]:
            print(f"  - {name}")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more")
    
    if failed:
        print(f"\n❌ Failed to process {len(failed)} datasets:")
        for name in failed[:20]:
            print(f"  - {name}")
        if len(failed) > 20:
            print(f"  ... and {len(failed) - 20} more")
    
    if up_to_date_datasets:
        print(f"\n⏭️ Skipped {len(up_to_date_datasets)} up-to-date datasets")
    
    print("\n✨ UNCTAD connector run complete!")

if __name__ == "__main__":
    main()