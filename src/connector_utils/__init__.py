"""UNCTAD connector utilities."""
import io
import os
import tempfile
import py7zr
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
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

    # Extract only the CSV file from archive (not all files)
    with tempfile.TemporaryDirectory() as tmpdir:
        with py7zr.SevenZipFile(archive_data, mode="r") as archive:
            csv_files = [f for f in archive.getnames() if f.endswith(".csv")]
            # Only extract the CSV file we need, not everything
            archive.extract(path=tmpdir, targets=csv_files[:1])

        csv_path = os.path.join(tmpdir, csv_files[0])

        # Use DuckDB for memory-efficient CSV parsing (streams from disk)
        conn = duckdb.connect()
        table = conn.execute(f"""
            SELECT * FROM read_csv('{csv_path}', normalize_names=true, null_padding=true)
        """).arrow()
        conn.close()

    cols_to_keep = []
    for i, field in enumerate(table.schema):
        name = table.column_names[i]
        if field.type == pa.null():
            continue
        if name.endswith("_footnote") or name.endswith("_missing_value"):
            continue
        cols_to_keep.append(name)
    if len(cols_to_keep) < len(table.column_names):
        table = table.select(cols_to_keep)

    return table


def format_year_range(table, column):
    """Convert 8-digit int year ranges (e.g. 19801981) to 'YYYY-YYYY' strings."""
    col = table[column]
    if pa.types.is_integer(col.type):
        s = pc.cast(col, pa.utf8())
        start = pc.utf8_slice_codeunits(s, 0, 4)
        end = pc.utf8_slice_codeunits(s, 4)
        formatted = pc.binary_join_element_wise(start, end, "-")
        idx = table.column_names.index(column)
        table = table.set_column(idx, column, formatted)
    return table


def filter_countries(table, column="economy"):
    """Keep only country rows (3-digit economy codes), remove aggregates (4-digit).

    UNCTAD uses 3-digit codes for countries/territories (UN M49 standard)
    and 4-digit codes for regional/development aggregates.
    """
    if column not in table.column_names:
        return table
    return table.filter(pc.equal(pc.utf8_length(table[column]), 3))
