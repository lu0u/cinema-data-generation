import psycopg2
from psycopg2.extras import execute_values
import random
import time

# Database configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "changeme12345",
    "host": "localhost",
    "port": "5432",
    "options": "-c search_path=al_sinama,public"
}

def fix_missing_tickets_batched(batch_size=10000):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            # Step 1: Get all ticket IDs once
            cursor.execute("SELECT ticket_id FROM dim_ticket")
            ticket_ids = [row[0] for row in cursor.fetchall()]
            print(f"Loaded {len(ticket_ids)} ticket IDs")

            # Step 2: Process in batches
            offset = 0
            total_fixed = 0
            
            while True:
                # Fetch a batch of missing transactions
                cursor.execute("""
                    SELECT ft.transaction_id
                    FROM fact_transaction ft
                    LEFT JOIN fact_transaction_ticket ftt 
                        ON ft.transaction_id = ftt.transaction_id
                    WHERE ftt.transaction_id IS NULL
                    ORDER BY ft.transaction_id
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
                
                batch = [row[0] for row in cursor.fetchall()]
                if not batch:
                    break  # Exit loop when no more results
                
                # Step 3: Generate ticket associations for this batch
                ticket_data = []
                for trans_id in batch:
                    num_tickets = random.randint(1, 5)
                    selected_tickets = random.sample(ticket_ids, num_tickets)
                    ticket_data.extend([(trans_id, tid) for tid in selected_tickets])
                
                # Step 4: Bulk insert for this batch
                execute_values(
                    cursor,
                    """INSERT INTO fact_transaction_ticket 
                       (transaction_id, ticket_id) VALUES %s
                       ON CONFLICT DO NOTHING""",  # Skip duplicates
                    ticket_data,
                    page_size=1000
                )
                conn.commit()
                
                # Step 5: Update progress
                total_fixed += len(batch)
                print(f"Processed batch {offset//batch_size + 1}: "
                      f"Fixed {len(batch)} transactions "
                      f"(Total: {total_fixed}/~700,000)")
                
                offset += batch_size

if __name__ == "__main__":
    start_time = time.time()
    fix_missing_tickets_batched(batch_size=10000)  # Adjust batch size as needed
    print(f"Total time: {time.time()-start_time:.2f} seconds")