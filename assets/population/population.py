from datetime import datetime
from utils import upload_data, save_state
from general import download_dataset

def process_population():
    """Process population datasets."""
    
    datasets = [
        ('US_PopTotal', 'population_total_and_urban'),
        ('US_PopGR', 'population_growth_rates'),
        ('US_PopAgeStruct', 'population_structure_by_age_gender')
    ]
    
    for dataset_id, dataset_name in datasets:
        try:
            data = download_dataset(dataset_id)
            if data.num_rows > 0:
                upload_data(data, dataset_name)
                print(f"Uploaded {data.num_rows} rows to {dataset_name}")
        except Exception as e:
            print(f"Failed to process {dataset_name}: {e}")
    
    save_state("population", {
        "last_updated": datetime.now().isoformat()
    })