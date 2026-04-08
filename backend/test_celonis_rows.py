import os
from pycelonis import get_celonis
from dotenv import load_dotenv

load_dotenv('d:/pi-mining-1/backend/.env')
c = get_celonis(base_url=os.getenv('CELONIS_URL'), api_token=os.getenv('CELONIS_API_KEY'), key_type='USER_KEY')
pool = c.data_integration.get_data_pool(os.getenv('CELONIS_DATA_POOL_ID'))
model = pool.get_data_model(os.getenv('CELONIS_DATA_MODEL_ID'))

from pycelonis.pql import PQL, PQLColumn
query1 = PQL()
query1 += PQLColumn(name="cnt", query='COUNT("t_o_custom_VimHeader"."ACTIVITYEN")')
df1 = model.export_data_frame(query1)
print("VimHeader count:", df1.iloc[0,0])

query2 = PQL()
query2 += PQLColumn(name="cnt", query='COUNT("t_o_custom_AccountingDocumentHeader"."ACTIVITYEN")')
df2 = model.export_data_frame(query2)
print("AccountingDocumentHeader count:", df2.iloc[0,0])
