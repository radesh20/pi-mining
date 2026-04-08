"""Build a second bridge for PurchasingDocumentItem-linked events."""
from pycelonis.pql import PQL, PQLColumn
from app.services.celonis_service import CelonisService

c = CelonisService()

# Bridge 2: PurchasingDocumentItem -> PurchasingDocumentHeader
print("=== Bridge 2: PurchDocItem -> PurchDocHeader ===")
q = PQL()
q += PQLColumn(query='"o_custom_PurchasingDocumentItem"."ID"', name='item_id')
q += PQLColumn(query='"o_custom_PurchasingDocumentHeader"."EBELN"', name='ebeln')
q += PQLColumn(query='"o_custom_PurchasingDocumentHeader"."LIFNR"', name='vendor')

try:
    df = c._run_pql(q, 'pdi bridge')
    print(f'SUCCESS! Rows: {len(df)}, Vendors: {df["vendor"].nunique()}')
    print(f'Vendor list: {df["vendor"].unique().tolist()}')
except Exception as e:
    print(f'FAILED: {e}')

# Bridge 3: PurchasingDocumentHeader direct
print("\n=== Bridge 3: PurchDocHeader direct ===")
q2 = PQL()
q2 += PQLColumn(query='"o_custom_PurchasingDocumentHeader"."ID"', name='pdh_id')
q2 += PQLColumn(query='"o_custom_PurchasingDocumentHeader"."EBELN"', name='ebeln')
q2 += PQLColumn(query='"o_custom_PurchasingDocumentHeader"."LIFNR"', name='vendor')

try:
    df2 = c._run_pql(q2, 'pdh direct')
    print(f'SUCCESS! Rows: {len(df2)}, Vendors: {df2["vendor"].nunique()}')
    print(f'Vendor list: {df2["vendor"].unique().tolist()}')
except Exception as e2:
    print(f'FAILED: {e2}')
