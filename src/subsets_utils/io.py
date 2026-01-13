"""Data I/O operations for raw data, state, and Delta tables.

In cloud mode, uses local disk cache with LRU eviction to handle large datasets
without running out of memory or disk space. Writes go to local disk first,
then upload to R2, with automatic eviction when disk space is low.
"""

import io
import json
import gzip
import shutil
import hashlib
from datetime import datetime
from pathlib import Path
import os
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable

from . import debug
from .config import (
    is_cloud,
    get_storage_options,
    subsets_uri,
    raw_key,
    raw_path,
    state_key,
    state_path,
    cache_path,
    get_cache_dir,
    CACHE_MIN_FREE_GB,
)
from .r2 import upload_bytes, upload_file, download_bytes


# =============================================================================
# Disk Cache Management (cloud mode)
# =============================================================================

def _evict_if_needed(required_bytes: int = 0):
    """Evict oldest cached files if disk space is low."""
    cache_dir = Path(get_cache_dir())
    if not cache_dir.exists():
        return

    min_free = CACHE_MIN_FREE_GB * 1024 * 1024 * 1024  # Convert to bytes

    # Check current free space
    stat = shutil.disk_usage(cache_dir)
    if stat.free - required_bytes >= min_free:
        return  # Enough space

    # Get all cached files sorted by mtime (oldest first)
    files = []
    for f in cache_dir.rglob('*'):
        if f.is_file():
            files.append((f.stat().st_mtime, f.stat().st_size, f))
    files.sort()  # Oldest first

    # Delete until we have enough space
    for _, _, path in files:
        stat = shutil.disk_usage(cache_dir)
        if stat.free - required_bytes >= min_free:
            break
        path.unlink()
        # Clean empty parent dirs
        try:
            path.parent.rmdir()
        except OSError:
            pass


def _cache_lookup(key: str) -> Path | None:
    """Check if a file is in cache. Returns path if exists, None otherwise."""
    path = Path(cache_path(key))
    if path.exists():
        path.touch()  # Update access time for LRU
        return path
    return None


# =============================================================================
# Utility
# =============================================================================

def data_hash(table: pa.Table) -> str:
    """Fast hash based on row count + schema. Use with state to detect changes."""
    h = hashlib.md5()
    h.update(f"{len(table)}".encode())
    h.update(str(table.schema).encode())
    return h.hexdigest()[:16]


# =============================================================================
# Delta Table Operations
# =============================================================================

def upload_data(data: pa.Table, dataset_name: str, metadata: dict = None, mode: str = "append", merge_key: str = None) -> str:
    """Upload a PyArrow table to a Delta table."""
    if mode not in ("append", "overwrite", "merge"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'append', 'overwrite', or 'merge'.")
    if mode == "merge" and not merge_key:
        raise ValueError("merge_key is required when mode='merge'")
    if mode == "overwrite":
        print(f"Warning: Overwriting {dataset_name} - all existing data will be replaced")
    if len(data) == 0:
        print(f"No data to upload for {dataset_name}")
        return ""

    size_mb = round(data.nbytes / 1024 / 1024, 2)
    columns = ', '.join([f.name for f in data.schema])
    mode_label = {"append": "Appending to", "overwrite": "Overwriting", "merge": "Merging into"}[mode]
    print(f"{mode_label} {dataset_name}: {len(data)} rows, {len(data.schema)} cols ({columns}), {size_mb} MB")

    table_name = metadata.get("title") if metadata else None
    table_description = json.dumps(metadata) if metadata else None
    uri = subsets_uri(dataset_name)
    storage_opts = get_storage_options()

    if mode == "merge":
        try:
            dt = DeltaTable(uri, storage_options=storage_opts) if storage_opts else DeltaTable(uri)
            updates = {col: f"source.{col}" for col in data.column_names}
            dt.merge(source=data, predicate=f"target.{merge_key} = source.{merge_key}",
                     source_alias="source", target_alias="target") \
              .when_matched_update(updates=updates) \
              .when_not_matched_insert(updates=updates) \
              .execute()
            print(f"Merged: table now has {len(dt.to_pyarrow_table())} total rows")
        except Exception:
            write_deltalake(uri, data, storage_options=storage_opts, name=table_name, description=table_description)
            print(f"Created new table {dataset_name}")
    else:
        write_deltalake(uri, data, mode=mode, storage_options=storage_opts,
                        name=table_name, description=table_description,
                        schema_mode="merge" if mode == "append" else "overwrite")

    null_counts = {col: data[col].null_count for col in data.column_names if data[col].null_count > 0}
    debug.log_data_output(dataset_name=dataset_name, row_count=len(data), size_bytes=data.nbytes,
                          columns=data.column_names, column_count=len(data.schema), null_counts=null_counts, mode=mode)
    return uri


def load_asset(asset_name: str) -> pa.Table:
    """Load a Delta table as PyArrow table."""
    uri = subsets_uri(asset_name)
    storage_opts = get_storage_options()
    try:
        dt = DeltaTable(uri, storage_options=storage_opts) if storage_opts else DeltaTable(uri)
        return dt.to_pyarrow_table()
    except Exception as e:
        raise FileNotFoundError(f"No Delta table found at {uri}") from e


# =============================================================================
# State Operations
# =============================================================================

def load_state(asset: str) -> dict:
    """Load state for an asset."""
    if is_cloud():
        data = download_bytes(state_key(asset))
        return json.loads(data.decode('utf-8')) if data else {}
    else:
        path = Path(state_path(asset))
        return json.load(open(path)) if path.exists() else {}


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset."""
    old_state = load_state(asset)
    state_data = {**state_data, '_metadata': {'updated_at': datetime.now().isoformat(), 'run_id': os.environ.get('RUN_ID', 'unknown')}}

    if is_cloud():
        uri = upload_bytes(json.dumps(state_data, indent=2).encode('utf-8'), state_key(asset))
        debug.log_state_change(asset, old_state, state_data)
        return uri
    else:
        path = state_path(asset)
        json.dump(state_data, open(path, 'w'), indent=2)
        debug.log_state_change(asset, old_state, state_data)
        return path


# =============================================================================
# Raw Data Operations
# =============================================================================

def save_raw_file(content: str | bytes, asset_id: str, extension: str = "txt") -> str:
    """Save raw file (CSV, XML, ZIP, etc.)."""
    data = content.encode('utf-8') if isinstance(content, str) else content

    if is_cloud():
        key = raw_key(asset_id, extension)

        # Evict if needed and write to cache
        _evict_if_needed(len(data))
        cpath = cache_path(key)
        Path(cpath).write_bytes(data)

        # Upload to R2
        uri = upload_bytes(data, key)
        print(f"  -> R2: {asset_id}.{extension}")
        return uri
    else:
        path = raw_path(asset_id, extension)
        if isinstance(content, str):
            Path(path).write_text(content, encoding='utf-8')
        else:
            Path(path).write_bytes(content)
        print(f"  -> {asset_id}.{extension}")
        return path


def load_raw_file(asset_id: str, extension: str = "txt") -> str | bytes:
    """Load raw file."""
    if is_cloud():
        key = raw_key(asset_id, extension)

        # Check cache first
        cached = _cache_lookup(key)
        if cached:
            print(f"  <- Cache: {asset_id}.{extension}")
            try:
                return cached.read_text(encoding='utf-8')
            except UnicodeDecodeError:
                return cached.read_bytes()

        # Download from R2
        data = download_bytes(key)
        if data is None:
            raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found in R2.")

        # Save to cache for next time
        _evict_if_needed(len(data))
        cpath = cache_path(key)
        Path(cpath).write_bytes(data)

        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return data
    else:
        path = Path(raw_path(asset_id, extension))
        if not path.exists():
            raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found.")
        try:
            return path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            return path.read_bytes()


def save_raw_json(data, asset_id: str, compress: bool = False) -> str:
    """Save raw JSON data."""
    ext = "json.gz" if compress else "json"
    if compress:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
            gz.write(json.dumps(data).encode('utf-8'))
        content = buffer.getvalue()
    else:
        content = json.dumps(data, indent=2).encode('utf-8')

    if is_cloud():
        key = raw_key(asset_id, ext)

        # Evict if needed and write to cache
        _evict_if_needed(len(content))
        cpath = cache_path(key)
        Path(cpath).write_bytes(content)

        # Upload to R2
        uri = upload_bytes(content, key)
        print(f"  -> R2: {asset_id}.{ext}")
        return uri
    else:
        path = raw_path(asset_id, ext)
        Path(path).write_bytes(content)
        print(f"  -> {asset_id}.{ext}")
        return path


def load_raw_json(asset_id: str):
    """Load raw JSON data. Auto-detects compression."""
    if is_cloud():
        # Check cache first (both compressed and uncompressed)
        for ext in ("json", "json.gz"):
            key = raw_key(asset_id, ext)
            cached = _cache_lookup(key)
            if cached:
                print(f"  <- Cache: {asset_id}.{ext}")
                if ext == "json.gz":
                    with gzip.open(cached, 'rt', encoding='utf-8') as f:
                        return json.load(f)
                return json.loads(cached.read_text(encoding='utf-8'))

        # Try R2 (uncompressed first)
        key = raw_key(asset_id, "json")
        data = download_bytes(key)
        if data:
            _evict_if_needed(len(data))
            cpath = cache_path(key)
            Path(cpath).write_bytes(data)
            return json.loads(data.decode('utf-8'))

        # Try compressed
        key = raw_key(asset_id, "json.gz")
        data = download_bytes(key)
        if data:
            _evict_if_needed(len(data))
            cpath = cache_path(key)
            Path(cpath).write_bytes(data)
            with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as gz:
                return json.load(gz)

        raise FileNotFoundError(f"Raw asset '{asset_id}' not found in R2.")
    else:
        path = Path(raw_path(asset_id, "json"))
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
        path = Path(raw_path(asset_id, "json.gz"))
        if path.exists():
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                return json.load(f)
        raise FileNotFoundError(f"Raw asset '{asset_id}' not found.")


def save_raw_parquet(data: pa.Table, asset_id: str, metadata: dict = None) -> str:
    """Save raw PyArrow table as Parquet."""
    if metadata:
        existing = data.schema.metadata or {}
        existing[b'asset_metadata'] = json.dumps(metadata).encode('utf-8')
        data = data.replace_schema_metadata(existing)

    if is_cloud():
        key = raw_key(asset_id, "parquet")
        cpath = cache_path(key)

        # Evict BEFORE writing - estimate compressed size as 50% of in-memory size
        estimated_size = max(data.nbytes // 2, 100 * 1024 * 1024)  # At least 100MB headroom
        _evict_if_needed(estimated_size)

        # Write to local cache first (streams to disk, memory efficient)
        pq.write_table(data, cpath, compression='snappy')

        # Upload to R2
        uri = upload_file(cpath, key)
        print(f"  -> R2: {asset_id}.parquet ({data.num_rows:,} rows)")
        return uri
    else:
        path = raw_path(asset_id, "parquet")
        pq.write_table(data, path, compression='snappy')
        print(f"  -> {asset_id}.parquet ({data.num_rows:,} rows)")
        return path


def load_raw_parquet(asset_id: str) -> pa.Table:
    """Load raw Parquet file as PyArrow table."""
    if is_cloud():
        key = raw_key(asset_id, "parquet")

        # Check cache first
        cached = _cache_lookup(key)
        if cached:
            print(f"  <- Cache: {asset_id}.parquet")
            return pq.read_table(cached)

        # Download from R2
        data = download_bytes(key)
        if data is None:
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found in R2")

        # Save to cache for next time
        _evict_if_needed(len(data))
        cpath = cache_path(key)
        Path(cpath).write_bytes(data)

        return pq.read_table(io.BytesIO(data))
    else:
        path = raw_path(asset_id, "parquet")
        if not Path(path).exists():
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found at {path}")
        return pq.read_table(path)
