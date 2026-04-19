from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.IctUseEconActivity_Isic4"
SUBSET_DATASET_ID = "unctad_ict_use_by_activity_isic4"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD ICT Use by Economic Activity (ISIC Rev.4)",
    "description": "ICT usage by economic activity classified under ISIC Rev.4 from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "useofict": "ICT usage type code",
        "useofict_label": "ICT usage type name",
        "economicactivity": "Economic activity code",
        "economicactivity_label": "Economic activity name",
        "percentage": "Percentage",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'useofict', 'economicactivity'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
