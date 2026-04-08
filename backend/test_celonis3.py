import os
import sys
from pycelonis import get_celonis
from dotenv import load_dotenv

load_dotenv('d:/pi-mining-1/backend/.env')
try:
    c = get_celonis(base_url=os.getenv('CELONIS_URL'), api_token=os.getenv('CELONIS_API_KEY'), key_type='USER_KEY')
    pool = c.data_integration.get_data_pool(os.getenv('CELONIS_DATA_POOL_ID'))
    model = pool.get_data_model(os.getenv('CELONIS_DATA_MODEL_ID'))
    print(f"Data Model: {model.name}")
    tables = model.get_tables()
    for t in tables:
        print(f"Table: {t.name}")
except Exception as e:
    print("ERROR:", str(e))
