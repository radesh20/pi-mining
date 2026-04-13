import sys
import pandas as pd
from app.services.celonis_service import CelonisService

def test_joins():
    try:
        c = CelonisService()
        event_tables = [t.name for t in c.data_model.get_tables() if t.name.startswith('t_e_custom_')]
        case_table = "t_o_custom_PurchasingDocumentHeader"
        
        results = []
        for t_name in event_tables:
            # Query: count of events joined with case table
            pql = f'COUNT_TABLE("{t_name}")'
            # We add a dimension from the case table to force a join
            query = f'SELECT "{case_table}"."EBELN", {pql} AS "CNT" FROM "{t_name}" GROUP BY 1 LIMIT 10'
            
            from pycelonis.pql import PQL, PQLColumn
            q = PQL()
            q += PQLColumn(query=f'"{case_table}"."EBELN"', name="EBELN")
            q += PQLColumn(query=f'"{t_name}"."Time"', name="TIME")
            
            try:
                # If this succeeds, there is a path in the DM
                df = c.data_model.export_data_frame(q)
                results.append({"table": t_name, "status": "SUCCESS", "rows": len(df)})
            except Exception as e:
                results.append({"table": t_name, "status": "FAILED", "error": str(e)[:100]})
        
        for res in results:
            print(f"{res['table']}: {res['status']} {res.get('rows', '')} {res.get('error', '')}")
            
    except Exception as e:
        print(f"Global Error: {str(e)}")

if __name__ == "__main__":
    test_joins()
