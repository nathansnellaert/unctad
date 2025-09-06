from datetime import datetime
from utils import upload_data, save_state
from general import download_dataset

def process_international_merchandise_trade():
    """Process international merchandise trade datasets."""
    
    datasets = [
        ('US_MerchVolumeQuarterly', 'merchandise_volume_quarterly'),
        ('US_TradeMerchTotal', 'merchandise_total_trade'),
        ('US_TradeMerchGR', 'merchandise_trade_growth_rates'),
        ('US_TradeMerchBalance', 'merchandise_trade_balance'),
        ('US_IntraTrade', 'merchandise_intra_trade'),
        ('US_TermsOfTrade', 'merchandise_terms_of_trade'),
        ('US_ConcentDiversIndices', 'merchandise_concentration_indices'),
        ('US_ConcentStructIndices', 'merchandise_structural_indices'),
        ('US_RCA', 'revealed_comparative_advantage'),
        ('US_Tariff', 'import_tariff_rates')
    ]
    
    for dataset_id, dataset_name in datasets:
        try:
            data = download_dataset(dataset_id)
            if data.num_rows > 0:
                upload_data(data, dataset_name)
                print(f"Uploaded {data.num_rows} rows to {dataset_name}")
        except Exception as e:
            print(f"Failed to process {dataset_name}: {e}")
    
    save_state("international_merchandise_trade", {
        "last_updated": datetime.now().isoformat()
    })