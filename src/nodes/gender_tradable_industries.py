from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.Gender_TradableIndustries"
SUBSET_DATASET_ID = "unctad_gender_tradable_industries"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Gender in Tradable Industries",
    "description": "Gender participation in tradable industries by economy from UNCTAD.",
    "column_descriptions": {
        "_year": "Year of observation",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "sex": "Sex code",
        "sex_label": "Sex",
        "industry": "Industry code",
        "industry_label": "Industry name",
        "average_monthly_earnings_of_employees_us": "Average monthly earnings of employees us",
        "average_monthly_earnings_of_employees_2017_ppp": "Average monthly earnings of employees 2017 PPP",
        "number_of_employees_in_thousands": "Number of employees in thousands",
        "percentage_of_employees": "Percentage of employees",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['_year', 'economy', 'sex', 'industry'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
