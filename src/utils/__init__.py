"""UNCTAD connector utilities."""
import io
import os
import tempfile
import py7zr
import pyarrow.csv as pac
from subsets_utils import get


def download_dataset(report_name: str):
    """Download a dataset from UNCTAD API.

    Args:
        report_name: UNCTAD report name (e.g., 'US.TradeMerchTotal')

    Returns:
        PyArrow table with the dataset
    """
    # Get file metadata
    metadata_url = f"https://unctadstat-api.unctad.org/api/reportMetadata/{report_name}/bulkfiles/en"
    metadata_resp = get(metadata_url)
    file_metadata = metadata_resp.json()[0]
    file_id = file_metadata["fileId"]

    # Download the 7z archive
    download_url = f"https://unctadstat-api.unctad.org/api/reportMetadata/{report_name}/bulkfile/{file_id}/en"
    download_resp = get(download_url)
    archive_data = io.BytesIO(download_resp.content)

    # Extract CSV from archive to temp directory
    with tempfile.TemporaryDirectory() as tmpdir:
        with py7zr.SevenZipFile(archive_data, mode="r") as archive:
            csv_files = [f for f in archive.getnames() if f.endswith(".csv")]
            archive.extractall(path=tmpdir)

        csv_path = os.path.join(tmpdir, csv_files[0])
        table = pac.read_csv(csv_path)
        # Normalize column names
        new_names = [c.lower().replace(" ", "_") for c in table.column_names]
        table = table.rename_columns(new_names)

    return table
