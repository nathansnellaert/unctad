from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.Gender_DomesticValueAdded"
SUBSET_DATASET_ID = "unctad_gender_domestic_value_added"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "license": "UNCTAD Terms of Use",
    "title": "UNCTAD Gender and Domestic Value Added",
    "description": "Domestic value added in trade by gender, industry, and component from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "sex": "Sex code",
        "sex_label": "Sex",
        "industry": "Industry code",
        "industry_label": "Industry name",
        "components": "Component code",
        "components_label": "Component name",
        "percentage_of_domestic_value_added": "Percentage of domestic value added",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'sex', 'industry', 'components'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
