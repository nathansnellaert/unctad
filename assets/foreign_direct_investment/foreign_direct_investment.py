from datetime import datetime
from utils import upload_data, save_state
from general import download_dataset

def process_foreign_direct_investment():
    """Process foreign direct investment datasets."""
    
    # FDI flows and stock
    data = download_dataset('US_FdiFlowsStock')
    if data.num_rows > 0:
        upload_data(data, "fdi_flows_and_stock")
        print(f"Uploaded {data.num_rows} rows to fdi_flows_and_stock")
    
    save_state("foreign_direct_investment", {
        "last_updated": datetime.now().isoformat()
    })