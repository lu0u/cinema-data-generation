import psycopg2
from psycopg2.extras import execute_values
import random

# Database configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "changeme12345",
    "host": "localhost",
    "port": "5432",
    "options": "-c search_path=al_sinama,public"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def backfill_missing_transactions():
    # Get all cinema IDs for offline transactions
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT cinema_id FROM dim_cinema")
            cinema_ids = [row[0] for row in cursor.fetchall()]
    
    # Batch settings
    BATCH_SIZE = 10000
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Process in batches
            offset = 0
            while True:
                # Fetch a batch of missing transactions
                cursor.execute("""
                    SELECT ft.transaction_id
                    FROM fact_transaction ft
                    LEFT JOIN (
                        SELECT transaction_id FROM fact_online_transaction
                        UNION ALL
                        SELECT transaction_id FROM fact_offline_transaction
                    ) AS existing ON ft.transaction_id = existing.transaction_id
                    WHERE existing.transaction_id IS NULL
                    LIMIT %s OFFSET %s
                """, (BATCH_SIZE, offset))
                batch = [row[0] for row in cursor.fetchall()]
                
                if not batch:
                    break  # No more missing transactions
                
                # Split into online/offline (30%/70%)
                online = []
                offline = []
                
                for trans_id in batch:
                    if random.random() < 0.3:  # 30% online
                        online.append((
                            trans_id,
                            random.choice(['Windows', 'MacOS', 'Linux']),
                            random.choice(['Chrome', 'Firefox', 'Safari'])
                        ))
                    else:  # 70% offline
                        offline.append((
                            trans_id,
                            random.choice(cinema_ids)
                        ))
                
                # Insert online transactions
                if online:
                    execute_values(
                        cursor,
                        """INSERT INTO fact_online_transaction 
                           (transaction_id, system_used, browser) VALUES %s""",
                        online,
                        page_size=1000
                    )
                
                # Insert offline transactions
                if offline:
                    execute_values(
                        cursor,
                        """INSERT INTO fact_offline_transaction 
                           (transaction_id, cinema_id) VALUES %s""",
                        offline,
                        page_size=1000
                    )
                
                conn.commit()
                print(f"Processed batch {offset//BATCH_SIZE + 1}")
                offset += BATCH_SIZE

if __name__ == "__main__":
    backfill_missing_transactions()
    print("Backfill complete!")