"""
UNCTAD catalogue - dynamically discover all available datasets from the API
"""
import pyarrow as pa
from utils import get

def process_catalogue():
    """
    Dynamically fetch all available UNCTAD datasets from their API.
    
    Returns:
        PyArrow Table with columns: dataset_id, dataset_name, dataset_url, title
    """
    url = "https://unctadstat-api.unctad.org/api/datacenter/en"
    response = get(url)
    response.raise_for_status()
    data = response.json()
    
    datasets = []
    
    # Process each category in the data center
    for category in data:
        # Add reports directly in this category
        for report in category.get("reports", []):
            report_name = report.get("reportName", "")
            dataset_id = f"unctad_{report.get('id', '')}"
            dataset_name = report_name.replace(".", "_")
            title = report.get("title", report_name)
            datasets.append({
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "dataset_url": report_name,  # Original format for API calls
                "title": title
            })
        
        # Process subfolders
        for subfolder in category.get("subFolders", []):
            for report in subfolder.get("reports", []):
                report_name = report.get("reportName", "")
                dataset_id = f"unctad_{report.get('id', '')}"
                dataset_name = report_name.replace(".", "_")
                title = report.get("title", report_name)
                datasets.append({
                    "dataset_id": dataset_id,
                    "dataset_name": dataset_name,
                    "dataset_url": report_name,  # Original format for API calls
                    "title": title
                })
    
    return pa.Table.from_pylist(datasets)