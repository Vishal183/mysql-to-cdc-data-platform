"""Lightweight Delta Lake reader using delta-rs (no JVM required)."""

import os
import time
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

DELTA_ROOT = os.environ.get("DELTA_ROOT", "/opt/api-data")
CACHE_TTL_SECONDS = 30

_cache: dict[str, tuple[float, pd.DataFrame]] = {}


def read_delta_table(table_name: str) -> Optional[pd.DataFrame]:
    """Read a Delta table into a Pandas DataFrame with TTL caching.

    Returns None if the table does not exist yet.
    """
    now = time.time()

    if table_name in _cache:
        cached_time, cached_df = _cache[table_name]
        if now - cached_time < CACHE_TTL_SECONDS:
            return cached_df

    path = os.path.join(DELTA_ROOT, "delta", table_name)
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(path)
        df = dt.to_pandas()
        _cache[table_name] = (now, df)
        logger.info(f"Loaded {table_name}: {len(df)} rows")
        return df
    except Exception as e:
        logger.warning(f"Could not read Delta table '{table_name}': {e}")
        return None
