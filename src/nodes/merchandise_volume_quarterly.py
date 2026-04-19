from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.MerchVolumeQuarterly"
SUBSET_DATASET_ID = "unctad_merchandise_volume_quarterly"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Merchandise Volume (Quarterly)",
    "description": "Quarterly merchandise trade volume indices by economy from UNCTAD.",
    "column_descriptions": {
        "_quarter": "Quarter of observation",
        "quarter_label": "Quarter label",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "flow": "Trade flow direction code",
        "flow_label": "Trade flow direction",
        "growth_rate_over_previous_period": "Growth rate over previoUS period",
        "growth_rate_yearonyear": "Growth rate year-on-year",
        "volume_index_2005100": "Volume index (base 2005=100)",
        "volume_index_seasonally_adjusted_2005100": "Volume index seasonally adjusted (base 2005=100)",
        "growth_rate_over_previous_period_seasonally_adjusted": "Growth rate over previoUS period seasonally adjusted",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_quarter', 'economy', 'flow'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
