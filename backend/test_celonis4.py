import os
from pycelonis import get_celonis
from dotenv import load_dotenv

load_dotenv('d:/pi-mining-1/backend/.env')
c = get_celonis(base_url=os.getenv('CELONIS_URL'), api_token=os.getenv('CELONIS_API_KEY'), key_type='USER_KEY')
pools = c.data_integration.get_data_pools()
for p in pools:
    print(f"Pool: {p.name} - ID: {p.id}")
    for m in p.get_data_models():
        print(f"  Model: {m.name} - ID: {m.id}")
