#!/usr/bin/env python3
"""
Example DuckDB queries against the local Parquet dataset.
Run after simulate.py has generated some data.

    python query.py
"""

import os
from pathlib import Path

import duckdb

DATA_DIR = Path(os.getenv("DATA_DIR", "../data"))
SENSORS  = str(DATA_DIR / "sensors" / "**" / "*.parquet")

con = duckdb.connect()

print("=== Per-device summary ===")
print(con.execute(f"""
    SELECT
        device_id,
        COUNT(*)                AS readings,
        ROUND(AVG(temp_c),  2) AS avg_temp_c,
        ROUND(MIN(temp_c),  2) AS min_temp_c,
        ROUND(MAX(temp_c),  2) AS max_temp_c,
        ROUND(MIN(battery), 2) AS min_battery
    FROM read_parquet('{SENSORS}', hive_partitioning = true)
    GROUP BY device_id
    ORDER BY device_id
""").df().to_string(index=False))

print("\n=== Last 10 readings ===")
print(con.execute(f"""
    SELECT device_id, ts, temp_c, humidity, battery
    FROM read_parquet('{SENSORS}', hive_partitioning = true)
    ORDER BY ts DESC
    LIMIT 10
""").df().to_string(index=False))

print("\n=== Hourly average temperature — sensor-01 ===")
print(con.execute(f"""
    SELECT
        DATE_TRUNC('hour', ts) AS hour,
        ROUND(AVG(temp_c), 2)  AS avg_temp_c
    FROM read_parquet('{SENSORS}', hive_partitioning = true)
    WHERE device_id = 'sensor-01'
    GROUP BY hour
    ORDER BY hour DESC
    LIMIT 24
""").df().to_string(index=False))
