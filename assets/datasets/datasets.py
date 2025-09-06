"""
UNCTAD datasets - process individual datasets
"""
import pyarrow as pa
from general import download_dataset

def process_single_dataset(dataset_url: str, dataset_name: str):
    """
    Process a single UNCTAD dataset.
    
    Args:
        dataset_url: The original dataset URL/report name (e.g., "US.CurrAccBalance")
        dataset_name: The storage-friendly dataset name (e.g., "US_CurrAccBalance")
    
    Returns:
        PyArrow Table with the dataset data
    """
    try:
        # Use the general function to download the dataset
        return download_dataset(dataset_url)
    except Exception as e:
        # Return empty table on error to avoid crashing
        print(f"Error processing dataset {dataset_name}: {e}")
        return pa.Table.from_pylist([])