from datetime import datetime
from utils import upload_data, save_state
from general import download_dataset

def process_output_and_income():
    """Process output and income datasets."""
    
    datasets = [
        ('US_GDPTotal', 'gdp_total_and_per_capita'),
        ('US_GDPGR', 'gdp_growth_rates'),
        ('US_GDPComponent', 'gdp_by_expenditure_and_activity'),
        ('US_GNI', 'gni_total_and_per_capita')
    ]
    
    for dataset_id, dataset_name in datasets:
        try:
            data = download_dataset(dataset_id)
            if data.num_rows > 0:
                upload_data(data, dataset_name)
                print(f"Uploaded {data.num_rows} rows to {dataset_name}")
        except Exception as e:
            print(f"Failed to process {dataset_name}: {e}")
    
    save_state("output_and_income", {
        "last_updated": datetime.now().isoformat()
    })