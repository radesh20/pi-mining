"""Definitive OCEL Cache Warmer: Populates the Triple-Bridge JSON Cache."""
import time
import os
import logging
import pandas as pd
from app.services.celonis_service import CelonisService

# Configure logging to see bridge results
logging.basicConfig(level=logging.INFO)

start = time.time()

c = CelonisService()
c._event_log_cache = None  # Force fresh extraction

cache_path = c._JSON_CACHE_PATH
if os.path.exists(cache_path):
    os.remove(cache_path)
    print(f"Cleared old cache: {cache_path}")

print("Starting Triple-Bridge extraction (this may take a few minutes)...")
df = c.get_event_log()
elapsed = time.time() - start

print(f"\n{'='*50}")
print(f"TRIPLE-BRIDGE EXTRACTION COMPLETE in {elapsed:.1f}s")
print(f"{'='*50}")
print(f"Total Events: {len(df)}")
print(f"Unique Cases: {df['case_id'].nunique()}")
print(f"Unique Vendors: {df['vendor_id'].nunique()}")
print(f"Vendors Found: {df['vendor_id'].dropna().unique().tolist()}")
print(f"Cache saved to: {cache_path}")
print(f"Cache size: {os.path.getsize(cache_path) / 1024:.1f} KB")

# Verification: Try loading it back
print("\nVerifying JSON load speed...")
v_start = time.time()
import json
with open(cache_path, "r") as f:
    v_data = json.load(f)
v_df = pd.DataFrame(v_data["events"])
v_elapsed = time.time() - v_start
print(f"JSON Load Time: {v_elapsed:.4f}s (vs {elapsed:.1f}s for Celonis)")
print(f"Stats found: {len(v_data.get('vendor_stats', []))} vendors pre-calculated.")
