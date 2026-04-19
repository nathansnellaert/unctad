from connector_utils import download_dataset, filter_countries
from subsets_utils import save_raw_parquet, load_raw_parquet, merge, publish

UNCTAD_DATASET_ID = "US.PortCalls_S"
SUBSET_DATASET_ID = "unctad_port_calls_ships"

METADATA = {
    "id": SUBSET_DATASET_ID,
    "title": "UNCTAD Port Calls (Ships)",
    "description": "Port call statistics for ships by economy and commercial market from UNCTAD.",
    "column_descriptions": {
        "period": "Period of observation",
        "period_label": "Period label",
        "economy": "Reporting economy code (UN M49)",
        "economy_label": "Reporting economy name",
        "commercialmarket": "Commercial market segment code",
        "commercialmarket_label": "Commercial market segment name",
        "median_time_in_port_days": "Median time in port days",
        "average_age_of_vessels_years": "Average age of vessels years",
        "average_size_gt_of_vessels": "Average size GT of vessels",
        "average_cargo_carrying_capacity_dwt_per_vessel": "Average cargo carrying capacity DWT per vessel",
        "average_container_carrying_capacity_teu_per_container_ship": "Average container carrying capacity TEU per container ship",
        "maximum_size_gt_of_vessels": "Maximum size GT of vessels",
        "maximum_cargo_carrying_capacity_dwt_of_vessels": "Maximum cargo carrying capacity DWT of vessels",
        "maximum_container_carrying_capacity_teu_of_container_ships": "Maximum container carrying capacity TEU of container ships",
    },
}

def download():
    table = download_dataset(UNCTAD_DATASET_ID)
    save_raw_parquet(table, UNCTAD_DATASET_ID)

def transform():
    table = load_raw_parquet(UNCTAD_DATASET_ID)
    table = filter_countries(table)
    merge(table, SUBSET_DATASET_ID, key=['period', 'economy', 'commercialmarket'])
    publish(SUBSET_DATASET_ID, METADATA)

NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
