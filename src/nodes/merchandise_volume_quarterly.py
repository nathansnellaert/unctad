"""Fetch Merchandise Volume Quarterly from UNCTAD."""
from utils import download_dataset
from subsets_utils import sync_data, save_state

REPORT = "US.MerchVolumeQuarterly"
DATASET_ID = "unctad_merchandise_volume_quarterly"


def run():
    table = download_dataset(REPORT)
    sync_data(table, DATASET_ID)
    save_state("merchandise_volume_quarterly", {"rows": len(table)})
    print(f"  {DATASET_ID}: {len(table):,} rows")
