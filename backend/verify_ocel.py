import sys
import time
from app.services.celonis_service import CelonisService

try:
    print("Testing OCEL extraction...")
    c = CelonisService()
    df = c.get_event_log()
    print(f"Total rows extracted: {len(df)}")
    print(f"Unique activities: {df['activity'].unique() if 'activity' in df.columns else 'None'}")
    print(f"Unique cases: {df['case_id'].nunique() if 'case_id' in df.columns else 'None'}")
    if not df.empty:
        print("Sample data:")
        print(df.head())
except Exception as e:
    import traceback
    traceback.print_exc()
