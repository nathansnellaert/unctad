"""UNCTAD connector utilities."""
import io
import os
import tempfile
import py7zr
import duckdb
import pyarrow as pa
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

        # Use DuckDB for memory-efficient CSV parsing (streams from disk)
        conn = duckdb.connect()
        table = conn.execute(f"""
            SELECT * FROM read_csv('{csv_path}', normalize_names=true, null_padding=true)
        """).arrow()
        conn.close()

    # Drop columns with null type (all values are null)
    cols_to_keep = []
    for i, field in enumerate(table.schema):
        if field.type != pa.null():
            cols_to_keep.append(table.column_names[i])
    if len(cols_to_keep) < len(table.column_names):
        table = table.select(cols_to_keep)

    return table
