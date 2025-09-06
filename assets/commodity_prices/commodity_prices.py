from datetime import datetime
from utils import upload_data, save_state
from general import download_dataset

def process_commodity_prices():
    """Process commodity prices datasets."""
    
    # Commodity price indices annual
    data = download_dataset('US_CommodityPriceIndices_A')
    if data.num_rows > 0:
        upload_data(data, "commodity_price_indices_annual")
        print(f"Uploaded {data.num_rows} rows to commodity_price_indices_annual")
    
    # Commodity prices annual
    data = download_dataset('US_CommodityPrice_A')
    if data.num_rows > 0:
        upload_data(data, "commodity_prices_annual")
        print(f"Uploaded {data.num_rows} rows to commodity_prices_annual")
    
    # Commodity price indices monthly
    data = download_dataset('US_CommodityPriceIndices_M')
    if data.num_rows > 0:
        upload_data(data, "commodity_price_indices_monthly")
        print(f"Uploaded {data.num_rows} rows to commodity_price_indices_monthly")
    
    # Commodity prices monthly
    data = download_dataset('US_CommodityPrice_M')
    if data.num_rows > 0:
        upload_data(data, "commodity_prices_monthly")
        print(f"Uploaded {data.num_rows} rows to commodity_prices_monthly")
    
    save_state("commodity_prices", {
        "last_updated": datetime.now().isoformat()
    })