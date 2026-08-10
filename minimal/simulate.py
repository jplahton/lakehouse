#!/usr/bin/env python3
"""
Generates synthetic sensor data identical to SensorGeneratorJob and writes
it as partitioned Parquet files to data/sensors/dt=YYYY-MM-DD/.

Config (env vars):
    SIM_DEVICES       number of virtual devices    (default: 5)
    SIM_RATE_PER_SEC  records per device per second (default: 1)
    SIM_DURATION_SEC  run duration, 0 = forever     (default: 0)
    DATA_DIR          output root directory          (default: ../data)
"""

import math
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

NUM_DEVICES   = int(os.getenv("SIM_DEVICES",       "5"))
RATE_PER_SEC  = float(os.getenv("SIM_RATE_PER_SEC", "1"))
DURATION_SEC  = float(os.getenv("SIM_DURATION_SEC", "0"))
DATA_DIR      = Path(os.getenv("DATA_DIR",          "../data"))
FLUSH_EVERY   = 30  # seconds between Parquet file writes

SCHEMA = pa.schema([
    pa.field("device_id", pa.string()),
    pa.field("ts",        pa.timestamp("us", tz="UTC")),
    pa.field("temp_c",    pa.float64()),
    pa.field("humidity",  pa.float64()),
    pa.field("battery",   pa.float64()),
    pa.field("topic",     pa.string()),
])


def generate(tick: int, slot: int) -> dict:
    device_id = f"sensor-{slot + 1:02d}"
    # Same formulas as SensorGeneratorJob — sine wave per device with unique phase
    temp_c   = 20.0 + 4.0 * math.sin(2 * math.pi * tick / 600.0  + slot) + random.gauss(0, 0.3)
    humidity = 55.0 + 5.0 * math.sin(2 * math.pi * tick / 1200.0 + slot) + random.gauss(0, 0.5)
    battery  = max(0.0, 100.0 - tick * 0.005 + random.gauss(0, 0.05))
    return {
        "device_id": device_id,
        "ts":        datetime.now(timezone.utc),
        "temp_c":    round(temp_c,   2),
        "humidity":  round(humidity, 2),
        "battery":   round(battery,  2),
        "topic":     f"simulated/{device_id}",
    }


def flush(buffer: list) -> None:
    if not buffer:
        return
    now     = datetime.now(timezone.utc)
    out_dir = DATA_DIR / "sensors" / f"dt={now:%Y-%m-%d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{now:%H%M%S_%f}.parquet"

    table = pa.table(
        {col: pa.array([r[col] for r in buffer], SCHEMA.field(col).type)
         for col in SCHEMA.names},
        schema=SCHEMA,
    )
    pq.write_table(table, out_path, compression="snappy")
    print(f"[{now:%H:%M:%S}] Wrote {len(buffer):>5} rows → {out_path}")
    buffer.clear()


def main() -> None:
    interval  = 1.0 / RATE_PER_SEC
    print(f"Simulating {NUM_DEVICES} device(s) at {RATE_PER_SEC} rec/s → {DATA_DIR}/sensors/")
    print("Ctrl+C to stop.\n")

    buffer, tick = [], 0
    last_flush   = time.monotonic()
    start        = time.monotonic()

    try:
        while True:
            t0 = time.monotonic()

            if DURATION_SEC > 0 and (t0 - start) >= DURATION_SEC:
                break

            for slot in range(NUM_DEVICES):
                buffer.append(generate(tick, slot))
            tick += 1

            if time.monotonic() - last_flush >= FLUSH_EVERY:
                flush(buffer)
                last_flush = time.monotonic()

            sleep = interval - (time.monotonic() - t0)
            if sleep > 0:
                time.sleep(sleep)

    except KeyboardInterrupt:
        print()
    finally:
        flush(buffer)
        print("Done.")


if __name__ == "__main__":
    main()
