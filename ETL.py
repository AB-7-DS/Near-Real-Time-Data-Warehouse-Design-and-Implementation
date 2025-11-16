
import threading
import queue as thread_queue
import pandas as pd
from typing import Optional, Dict, List, Any
import hashlib
from collections import deque
from datetime import datetime
import time
import mysql.connector

class QueueNode:
    """Doubly-linked list node for the queue with pointers to hash table entries."""
    def __init__(self, key, hash_slot: int, hash_index: int):
        self.key = key
        self.hash_slot = hash_slot
        self.hash_index = hash_index
        self.prev: Optional['QueueNode'] = None
        self.next: Optional['QueueNode'] = None

class FixedSizeHashTable:
    """Fixed-size hash table with 10,000 slots (hS = 10,000)."""
    def __init__(self, num_slots: int = 10000):
        self.num_slots = num_slots
        self.slots: List[List[Dict]] = [[] for _ in range(num_slots)]
        self.lock = threading.Lock()

    def hash_function(self, key) -> int:
        key_str = str(key)
        hash_value = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
        return hash_value % self.num_slots

    def get_available_slots(self) -> int:
        with self.lock:
            return sum(1 for slot in self.slots if not slot)

    def add_entry(self, key, value: Dict, queue_node: QueueNode) -> tuple:
        slot = self.hash_function(key)
        with self.lock:
            self.slots[slot].append({'value': value, 'queue_node': queue_node})
            index = len(self.slots[slot]) - 1
        return slot, index

    def get_entries(self, key) -> List[Dict]:
        slot = self.hash_function(key)
        with self.lock:
            return self.slots[slot].copy()

    def remove_entry(self, slot: int, index: int):
        with self.lock:
            if 0 <= slot < self.num_slots and 0 <= index < len(self.slots[slot]):
                self.slots[slot].pop(index)

class StreamBuffer:
    """Thread-safe buffer to temporarily hold incoming stream tuples."""
    def __init__(self, max_size: int = 100000):
        self.buffer = thread_queue.Queue(maxsize=max_size)

    def add(self, item: Dict):
        self.buffer.put(item, block=True, timeout=1.0)

    def get_batch(self, max_count: int) -> List[Dict]:
        items = []
        while len(items) < max_count:
            try:
                items.append(self.buffer.get_nowait())
            except thread_queue.Empty:
                break
        return items

    def is_empty(self) -> bool:
        return self.buffer.empty()

class DiskBuffer:
    """Memory buffer that holds a loaded partition p of size vP (500 tuples) from R."""
    def __init__(self, partition_size: int = 500):
        self.partition_size = partition_size
        self.current_partition: Optional[pd.DataFrame] = None
        self.lock = threading.Lock()

    def load_partition(self, master_data: pd.DataFrame, key, key_column: str) -> bool:
        with self.lock:
            matching = master_data[master_data[key_column] == key]
            if matching.empty:
                self.current_partition = None
                return False
            self.current_partition = matching.head(self.partition_size)
            return True

    def get_partition(self) -> Optional[pd.DataFrame]:
        with self.lock:
            return self.current_partition.copy() if self.current_partition is not None else None

class HybridJoinQueue:
    """Doubly-linked list queue for tracking arrival order."""
    def __init__(self):
        self.head: Optional[QueueNode] = None
        self.tail: Optional[QueueNode] = None
        self.lock = threading.Lock()

    def append(self, node: QueueNode):
        with self.lock:
            if self.tail is None:
                self.head = self.tail = node
            else:
                node.prev = self.tail
                self.tail.next = node
                self.tail = node

    def popleft(self) -> Optional[QueueNode]:
        with self.lock:
            if self.head is None:
                return None
            node = self.head
            self.head = self.head.next
            if self.head is None:
                self.tail = None
            else:
                self.head.prev = None
            return node

    def remove_node(self, node: QueueNode):
        with self.lock:
            if node.prev:
                node.prev.next = node.next
            else:
                self.head = node.next
            if node.next:
                node.next.prev = node.prev
            else:
                self.tail = node.prev

    def get_oldest_key(self):
        with self.lock:
            return self.head.key if self.head else None

    def is_empty(self) -> bool:
        return self.head is None

def _ensure_datetime(value):
    """Normalize time values from the stream to Python datetime objects."""
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"Unsupported date value type: {type(value)}")

def csv_reader_thread(csv_file_path: str, stream_buffer: StreamBuffer, stop_event: threading.Event):
    """Thread to continuously read from CSV and add to stream buffer."""
    while not stop_event.is_set():
        try:
            # In a real-world scenario, this would monitor a file for new lines.
            # For this simulation, we'll read it once and then stop.
            chunk = pd.read_csv(csv_file_path)
            chunk.rename(
                columns={'orderID': 'Order_ID', 'quantity': 'Quantity', 'date': 'Date'},
                inplace=True,
            )
            chunk['Date'] = pd.to_datetime(chunk['Date'])
            records = chunk.to_dict('records')
            for record in records:
                stream_buffer.add(record)
            print(f"Loaded {len(records)} records from CSV into stream buffer.")
            stop_event.set() # Stop after one full read for this simulation
        except Exception as e:
            print(f"Error reading CSV: {e}")
            time.sleep(5)

def hybrid_join_worker(conn_params, stream_buffer, hash_table, join_queue, master_data, stop_event, disk_buffer):
    """The main worker thread for the HYBRIDJOIN algorithm. Joins stream data with master data and inserts it into a staging table."""
    connection = mysql.connector.connect(**conn_params)
    cursor = connection.cursor()
    customer_master = master_data['customer']
    product_master = master_data['product']

    while not stop_event.is_set() or not stream_buffer.is_empty():
        w = hash_table.get_available_slots()
        if w > 0:
            batch = stream_buffer.get_batch(w)
            for trans in batch:
                key = trans['Customer_ID']
                queue_node = QueueNode(key, -1, -1)
                slot, index = hash_table.add_entry(key, trans, queue_node)
                queue_node.hash_slot = slot
                queue_node.hash_index = index
                join_queue.append(queue_node)

        oldest_node = join_queue.popleft()
        if oldest_node:
            oldest_key = oldest_node.key
            if disk_buffer.load_partition(customer_master, oldest_key, 'Customer_ID'):
                partition = disk_buffer.get_partition()
                if partition is not None:
                    hash_entries = hash_table.get_entries(oldest_key)
                    for _, customer_row in partition.iterrows():
                        for entry in hash_entries:
                            trans = entry['value']
                            queue_node_to_remove = entry['queue_node']
                            product_data = product_master[product_master['Product_ID'] == trans['Product_ID']]
                            
                            if not product_data.empty:
                                product_row = product_data.iloc[0]
                                
                                revenue = int(trans['Quantity']) * float(product_row['Price'])
                                staged_data = (
                                    int(trans['Order_ID']), _ensure_datetime(trans['Date']), int(trans['Quantity']), revenue,
                                    int(customer_row['Customer_ID']), customer_row['Gender'], customer_row['Age'],
                                    int(customer_row['Occupation']), customer_row['City_Category'],
                                    customer_row['Stay_In_Current_City_Years'], int(customer_row['Marital_Status']),
                                    product_row['Product_ID'], product_row.get('Product_Category'),
                                    float(product_row['Price']),
                                    int(product_row['Store_ID']) if pd.notna(product_row.get('Store_ID')) else None,
                                    product_row.get('Store_Name'),
                                    int(product_row['Supplier_ID']) if pd.notna(product_row.get('Supplier_ID')) else None,
                                    product_row.get('Supplier_Name'),
                                )
                                cursor.execute(
                                    """
                                    INSERT INTO StagingSales (
                                        Order_ID, `Date`, Quantity, Revenue,
                                        Customer_ID, Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status,
                                        Product_ID, Product_Category, Price,
                                        Store_ID, Store_Name, Supplier_ID, Supplier_Name
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                    staged_data
                                )
                                hash_table.remove_entry(queue_node_to_remove.hash_slot, queue_node_to_remove.hash_index)
                                join_queue.remove_node(queue_node_to_remove)
                    connection.commit()
        
        if oldest_node is None and stream_buffer.is_empty():
            time.sleep(0.1)

    connection.commit() # Final commit to ensure any remaining data is saved.
    cursor.close()
    connection.close()
    print("Hybrid join worker thread has stopped.")

def load_from_staging_to_warehouse_once(conn_params):
    """Loads data from the staging table to the final star schema in a single batch."""
    connection = mysql.connector.connect(**conn_params)
    cursor = connection.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM StagingSales")
        if cursor.fetchone()[0] == 0:
            print("Staging table is empty. Nothing to load.")
            return

        print("Starting batch load from staging to data warehouse...")

        cursor.execute("""
            INSERT IGNORE INTO DimCustomer (Customer_ID, Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status)
            SELECT DISTINCT Customer_ID, Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status
            FROM StagingSales WHERE Customer_ID IS NOT NULL
        """)
        print(f"Loaded {cursor.rowcount} new records into DimCustomer.")

        cursor.execute("""
            INSERT IGNORE INTO DimProduct (Product_ID, Product_Category, Price, Store_ID, Supplier_ID)
            SELECT DISTINCT Product_ID, Product_Category, Price, Store_ID, Supplier_ID
            FROM StagingSales WHERE Product_ID IS NOT NULL
        """)
        print(f"Loaded {cursor.rowcount} new records into DimProduct.")
        
        cursor.execute("""
            INSERT IGNORE INTO DimStore (Store_ID, Store_Name, City_Category)
            SELECT DISTINCT Store_ID, Store_Name, City_Category
            FROM StagingSales WHERE Store_ID IS NOT NULL
        """)
        print(f"Loaded {cursor.rowcount} new records into DimStore.")

        cursor.execute("""
            INSERT IGNORE INTO DimSupplier (Supplier_ID, Supplier_Name)
            SELECT DISTINCT Supplier_ID, Supplier_Name
            FROM StagingSales WHERE Supplier_ID IS NOT NULL
        """)
        print(f"Loaded {cursor.rowcount} new records into DimSupplier.")

        cursor.execute("""
            INSERT IGNORE INTO DimTime (Date, Day, Month, Quarter, Year, Weekday_Name, Is_Weekend)
            SELECT DISTINCT
                CAST(`Date` AS DATE), DAY(`Date`), MONTH(`Date`), QUARTER(`Date`), YEAR(`Date`),
                DAYNAME(`Date`), (CASE WHEN DAYOFWEEK(`Date`) IN (1, 7) THEN 1 ELSE 0 END)
            FROM StagingSales
        """)
        print(f"Loaded {cursor.rowcount} new records into DimTime.")

        cursor.execute("""
            INSERT IGNORE INTO FactSales (Order_ID, Customer_ID, Product_ID, Store_ID, Supplier_ID, Time_ID, Quantity, Revenue)
            SELECT s.Order_ID, s.Customer_ID, s.Product_ID, s.Store_ID, s.Supplier_ID, t.Time_ID, s.Quantity, s.Revenue
            FROM StagingSales s JOIN DimTime t ON CAST(s.`Date` AS DATE) = t.Date
        """)
        print(f"Loaded {cursor.rowcount} new records into FactSales.")

        cursor.execute("TRUNCATE TABLE StagingSales")
        print("Staging table truncated.")
        connection.commit()
        print("Batch load complete.")

    except mysql.connector.Error as err:
        print(f"Database error during staging load: {err}")
        if connection.is_connected(): connection.rollback()
    except Exception as e:
        print(f"An unexpected error occurred during staging load: {e}")
        if connection.is_connected(): connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print("Staging loader has finished.")

# In ETL.py, replace the entire run_etl function with this one.

def run_etl(connection, master_data, csv_file_path='data/transactional_data.csv'): # Corrected path
    """Initializes and runs the HYBRIDJOIN ETL process with a staging table."""
    conn_params = {
        'host': connection._host, 'user': connection._user,
        'password': connection._password, 'database': connection._database
    }

    # --- (Component initialization is the same) ---
    hash_table = FixedSizeHashTable(num_slots=10000)
    join_queue = HybridJoinQueue()
    stream_buffer = StreamBuffer(max_size=100000) # Increased size for bulk load
    disk_buffer = DiskBuffer(partition_size=500)

    producer_stop_event = threading.Event()
    join_worker_stop_event = threading.Event()
    
    producer_thread = threading.Thread(target=csv_reader_thread, args=(csv_file_path, stream_buffer, producer_stop_event))
    join_worker_thread = threading.Thread(target=hybrid_join_worker, args=(conn_params, stream_buffer, hash_table, join_queue, master_data, join_worker_stop_event, disk_buffer))

    print("Starting ETL process to populate staging table...")
    producer_thread.start()
    join_worker_thread.start()

    # --- LOGIC CHANGE 1: Wait for the producer to finish loading the buffer ---
    producer_thread.join()
    print("CSV reader has finished. All transactions are now in the stream buffer.")

    # --- LOGIC CHANGE 2: The Critical Fix ---
    # Instead of a race-condition loop, we now wait until the buffer is empty,
    # then we signal the worker to stop, and then we wait for it to finish gracefully.
    while not stream_buffer.is_empty():
        print(f"Waiting for join worker to process stream buffer... Records remaining: ~{stream_buffer.buffer.qsize()}")
        time.sleep(5)
    
    print("Stream buffer is empty. Signaling the join worker that no new data is coming.")
    join_worker_stop_event.set()

    # Now, we wait for the worker thread to completely finish processing everything in its internal queue.
    join_worker_thread.join()
    print("Join worker has finished processing all data. Staging table is fully populated.")

    # Now that the worker is PROPERLY finished, load from staging to the warehouse.
    print("\nLoading data from staging to the final data warehouse...")
    load_from_staging_to_warehouse_once(conn_params)

    print("\nETL process completed successfully.")