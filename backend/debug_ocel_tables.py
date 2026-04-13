import sys
from app.services.celonis_service import CelonisService

try:
    c = CelonisService()
    tables = list(c.activity_tables)
    print(f"Checking {len(tables)} tables...")
    for t_name in tables:
        try:
            # Use raw column check
            cols = c._table_columns_safe(t_name)
            if not cols:
                print(f"Table {t_name}: NOT FOUND or EMPTY COLS")
                continue
            
            # Simple count query
            q = f'COUNT(TABLE("{t_name}"))'
            # Note: _run_pql uses self.data_model.export_data_frame internally
            # For simplicity, just check row count via SDK
            t_obj = c.data_model.get_tables().find(t_name)
            count = t_obj.get_row_count()
            print(f"Table {t_name}: {count} rows. Columns: {cols}")
        except Exception as e:
            print(f"Table {t_name}: ERROR - {e}")
except Exception as e:
    import traceback
    traceback.print_exc()
