from datetime import datetime
from utils import upload_data, save_state
from general import download_dataset

def process_balance_of_payments():
    """Process balance of payments datasets."""
    
    # Current account balance
    data = download_dataset('US_CurrAccBalance')
    if data.num_rows > 0:
        upload_data(data, "balance_of_payments_current_account")
        print(f"Uploaded {data.num_rows} rows to balance_of_payments_current_account")
    
    # Personal remittances
    data = download_dataset('US_Remittances')
    if data.num_rows > 0:
        upload_data(data, "personal_remittances")
        print(f"Uploaded {data.num_rows} rows to personal_remittances")
    
    # Goods and Services BPM6
    data = download_dataset('US_GoodsAndServicesBpm6')
    if data.num_rows > 0:
        upload_data(data, "goods_and_services_bpm6")
        print(f"Uploaded {data.num_rows} rows to goods_and_services_bpm6")
    
    save_state("balance_of_payments", {
        "last_updated": datetime.now().isoformat()
    })