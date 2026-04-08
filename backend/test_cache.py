import os
import sys
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

load_dotenv('d:/pi-mining-1/backend/.env')

sys.path.insert(0, 'd:/pi-mining-1/backend')
from app.services.celonis_service import CelonisService
from app.services.process_insight_service import ProcessInsightService
from app.services.data_cache_service import DataCacheService

try:
    print("Testing DataCacheService refresh...")
    cache = DataCacheService()
    cache._refresh_all_data_impl()
    print("Success. Total cases:", cache.get_cache_status()['total_cases'])
except Exception as e:
    import traceback
    traceback.print_exc()
