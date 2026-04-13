import sys
import json
from app.services.celonis_service import CelonisService

def get_mapping():
    try:
        c = CelonisService()
        tables = c.data_model.get_tables()
        fks = c.data_model.get_foreign_keys()
        
        result = []
        for fk in fks:
            try:
                src_table = tables.find(fk.source_table_id)
                tgt_table = tables.find(fk.target_table_id)
                
                cols = []
                for col_map in fk.columns:
                    src_col = next((c.name for c in src_table.get_columns() if c.id == col_map.source_column_id), "id:" + str(col_map.source_column_id))
                    tgt_col = next((c.name for c in tgt_table.get_columns() if c.id == col_map.target_column_id), "id:" + str(col_map.target_column_id))
                    cols.append({"source": src_col, "target": tgt_col})
                
                result.append({
                    "source_table": src_table.name,
                    "target_table": tgt_table.name,
                    "columns": cols
                })
            except Exception as inner:
                result.append({"error": str(inner)})
                
        with open("dm_mapping.json", "w") as f:
            json.dump(result, f, indent=2)
        print("Mapping saved to dm_mapping.json")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    get_mapping()
