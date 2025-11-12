import pandas as pd
import mysql.connector

def load_csv_files():
    """Loads and prepares the datasets from CSV files."""
    print("Loading data from CSV files...")
    customer_df = pd.read_csv('customer_master_data.csv')
    product_df = pd.read_csv('product_master_data.csv')
    transaction_df = pd.read_csv('transactional_data.csv')
    
    # Rename columns for schema consistency
    product_df.rename(columns={'price$': 'Price', 'storeID': 'Store_ID', 'supplierID': 'Supplier_ID', 
                               'storeName': 'Store_Name', 'supplierName': 'Supplier_Name'}, inplace=True)
    transaction_df.rename(columns={'orderID': 'Order_ID', 'quantity': 'Quantity', 'date': 'Date'}, inplace=True)
    print("CSV data loaded and prepared.")
    return customer_df, product_df, transaction_df

def populate_dimension_tables(connection, customer_df, product_df, transaction_df):
    """Populates all dimension tables in the data warehouse."""
    if not connection:
        print("Cannot populate dimensions. No database connection.")
        return

    cursor = connection.cursor()
    try:
        print("Populating DimCustomer...")
        for _, row in customer_df.iterrows():
            cursor.execute("INSERT IGNORE INTO DimCustomer VALUES (%s, %s, %s, %s, %s, %s, %s)", 
                           tuple(row))

        print("Populating DimProduct...")
        for _, row in product_df.iterrows():
            cursor.execute("INSERT IGNORE INTO DimProduct (Product_ID, Product_Category, Price, Store_ID, Supplier_ID) VALUES (%s, %s, %s, %s, %s)",
                           (row['Product_ID'], row['Product_Category'], row['Price'], row['Store_ID'], row['Supplier_ID']))
        
        print("Populating DimStore...")
        stores = product_df[['Store_ID', 'Store_Name']].drop_duplicates()
        for _, row in stores.iterrows():
            cursor.execute("INSERT IGNORE INTO DimStore (Store_ID, Store_Name, City_Category) VALUES (%s, %s, NULL)",
                           (row['Store_ID'], row['Store_Name']))

        print("Populating DimSupplier...")
        suppliers = product_df[['Supplier_ID', 'Supplier_Name']].drop_duplicates()
        for _, row in suppliers.iterrows():
            cursor.execute("INSERT IGNORE INTO DimSupplier VALUES (%s, %s)", tuple(row))
        
        print("Populating DimTime...")
        transaction_df['Date'] = pd.to_datetime(transaction_df['Date'])
        time_df = transaction_df[['Date']].drop_duplicates()
        for _, row in time_df.iterrows():
            date = row['Date']
            is_weekend = 1 if date.weekday() >= 5 else 0
            cursor.execute("INSERT IGNORE INTO DimTime (Date, Day, Month, Quarter, Year, Weekday_Name, Is_Weekend) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (date.date(), date.day, date.month, date.quarter, date.year, date.strftime('%A'), is_weekend))
        
        connection.commit()
        print("All dimension tables populated successfully.")
    except mysql.connector.Error as err:
        print(f"Error populating dimension tables: {err}")
    finally:
        cursor.close()