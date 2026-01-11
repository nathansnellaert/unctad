"""Download and transform ConcentStructIndices."""
from utils import download_dataset
from subsets_utils import save_raw_parquet, load_raw_parquet, sync_data

REPORT = "US.ConcentStructIndices"
DATASET_ID = "unctad_merchandise_structural_indices"


def download():
    """Download US.ConcentStructIndices from UNCTAD API."""
    table = download_dataset(REPORT)
    save_raw_parquet(table, "merchandise_structural_indices")
    print(f"  Downloaded {REPORT}: {table.num_rows:,} rows")


def transform():
    """Transform and upload unctad_merchandise_structural_indices."""
    table = load_raw_parquet("merchandise_structural_indices")

    # TODO: Add custom transform logic here

    sync_data(table, DATASET_ID)
    print(f"  Uploaded {DATASET_ID}: {table.num_rows:,} rows")
