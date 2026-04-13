import sys
from app.services.celonis_service import CelonisService

try:
    c = CelonisService()
    
    from pycelonis.pql import PQL, PQLColumn
    query = PQL()
    query += PQLColumn(query='"t_e_custom_PostInvoice"."AccountingDocumentHeader_ID"', name='CASEKEY')
    query += PQLColumn(query="'PostInvoice'", name='ACTIVITYEN')
    query += PQLColumn(query='"t_e_custom_PostInvoice"."Time"', name='EVENTTIME')
    
    res = c.data_model.export_data_frame(query)
    print(f'Rows extracted for PostInvoice: {len(res)}')
    if len(res) > 0:
        print(res.head())
except Exception as e:
    import traceback
    traceback.print_exc()
