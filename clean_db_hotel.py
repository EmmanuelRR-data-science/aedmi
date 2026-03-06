import sys
import os

# Add project root to path
sys.path.append(r"c:\Users\EmmanuelRamírez\OneDrive - PhiQus\Escritorio\EDMI-APP-VPS")

from services.db import db_connection

def clean_hotel_activity_data():
    """Deletes all records from hotel activity tables."""
    print("Starting database cleanup for Hotel Activity data...")
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            
            # Table name confirmed via services/db.py: actividad_hotelera_estatal
            tables_to_clean = ["actividad_hotelera_estatal"]
            
            for table in tables_to_clean:
                try:
                    # Check if table exists first? No, DELETE from non-existent throws error, caught below.
                    cur.execute(f"DELETE FROM {table}")
                    print(f"Deleted records from {table}.")
                except Exception as e:
                    print(f"could not delete from {table}: {e}")
                    conn.rollback() 
            
            print("Cleanup finished.")
            return True
            
    except Exception as e:
        print(f"Database error: {e}")
        return False

if __name__ == "__main__":
    clean_hotel_activity_data()
