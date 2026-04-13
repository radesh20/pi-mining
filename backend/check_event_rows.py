from app.services.celonis_service import CelonisService
c = CelonisService()
print("=== OCEL Event Table Row Counts ===")
for t in c.activity_tables:
    try:
        table = c.data_model.get_tables().find(t)
        if table:
            rows = table.get_row_count()
            print(f"{t}: {rows} rows")
        else:
            print(f"{t}: NOT FOUND")
    except Exception as e:
        print(f"{t}: ERROR - {e}")
