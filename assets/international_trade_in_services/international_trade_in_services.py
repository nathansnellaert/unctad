from datetime import datetime
from utils import upload_data, save_state
from general import download_dataset

def process_international_trade_in_services():
    """Process international trade in services datasets."""
    
    datasets = [
        ('US_TotAndComServicesQuarterly', 'services_trade_quarterly'),
        ('US_TradeServCatTotal', 'services_by_category'),
        ('US_TradeServCatByPartner', 'services_by_category_and_partner')
    ]
    
    for dataset_id, dataset_name in datasets:
        try:
            data = download_dataset(dataset_id)
            if data.num_rows > 0:
                upload_data(data, dataset_name)
                print(f"Uploaded {data.num_rows} rows to {dataset_name}")
        except Exception as e:
            print(f"Failed to process {dataset_name}: {e}")
    
    save_state("international_trade_in_services", {
        "last_updated": datetime.now().isoformat()
    })