# hybrid_join.py
from collections import deque
import time
import mysql.connector

def run_hybrid_join(connection, transaction_stream, master_data):
    """Simulates the HYBRIDJOIN algorithm and loads data into the FactSales table."""
    if not connection:
        print("Cannot run HYBRIDJOIN. No database connection.")
        return

    # --- HYBRIDJOIN Components ---
    hash_table = {}  # Hash Table (H)
    queue = deque()  # Queue to track arrival order
    
    # Master data (simulating the large, disk-based relation R)
    customer_master = master_data['customer']
    product_master = master_data['product']
    
    cursor = connection.cursor()
    print("\n--- Starting HYBRIDJOIN Simulation ---")
    
    # Process each transaction from the stream
    for transaction in transaction_stream:
        # Step 1: Incoming tuple from the stream S arrives
        print(f"\n[Stream] New transaction arrived: Order_ID {transaction['Order_ID']}")
        
        # Step 2: Add the tuple to the hash table and its key to the queue
        key = transaction['Customer_ID']
        if key not in hash_table:
            hash_table[key] = []
        hash_table[key].append(transaction)
        queue.append(key)
        print(f"[Queue/Hash] Added Customer_ID {key} to queue and hash table.")
        print(f"          Queue size: {len(queue)}, Hash table keys: {len(hash_table)}")

        # Step 3: Use the oldest key from the queue to probe the relation R
        if queue:
            oldest_key = queue.popleft()
            print(f"[Probe] Processing oldest key from queue: Customer_ID {oldest_key}")
            
            if oldest_key in hash_table:
                # Simulate loading a partition from R (Master Data) into the "disk buffer"
                customer_data = customer_master[customer_master['Customer_ID'] == oldest_key]
                
                # Step 4: Probe the hash table with data from R
                for trans in hash_table.get(oldest_key, []):
                    product_data = product_master[product_master['Product_ID'] == trans['Product_ID']]
                    
                    if not customer_data.empty and not product_data.empty:
                        # --- Join Success ---
                        print(f"  [Join] SUCCESS: Found match for Order_ID {trans['Order_ID']} with Customer_ID {oldest_key}")
                        
                        # Enrich the transaction with master data
                        enriched_data = {**trans, **customer_data.iloc[0].to_dict(), **product_data.iloc[0].to_dict()}
                        
                        # Get Time_ID from the pre-populated DimTime table
                        cursor.execute("SELECT Time_ID FROM DimTime WHERE Date = %s", (enriched_data['Date'].date(),))
                        time_id_result = cursor.fetchone()
                        
                        if time_id_result:
                           time_id = time_id_result[0]
                           revenue = enriched_data['Quantity'] * enriched_data['Price']
                           
                           # Load the enriched record into the FactSales table
                           fact_sales_data = (
                               enriched_data['Order_ID'], enriched_data['Customer_ID'], enriched_data['Product_ID'],
                               enriched_data['Store_ID'], enriched_data['Supplier_ID'], time_id,
                               enriched_data['Quantity'], revenue
                           )
                           cursor.execute("INSERT INTO FactSales VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", fact_sales_data)
                           print(f"  [Load] Inserted enriched data for Order_ID {trans['Order_ID']} into FactSales.")
                    else:
                        print(f"  [Join] MISS: No master data found for Order_ID {trans['Order_ID']}.")

                # Remove the processed entry from the hash table
                if oldest_key in hash_table:
                    del hash_table[oldest_key]
                    print(f"[Cleanup] Removed Customer_ID {oldest_key} from hash table.")
        
        time.sleep(0.005) # Small delay to simulate a real-time stream

    connection.commit()
    print("\n--- HYBRIDJOIN Simulation Finished ---")
    print("FactSales table populated successfully.")
    cursor.close()