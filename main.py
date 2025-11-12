# main.py
import db_connector
import data_loader
import hybrid_join

def main():
    """Main function to run the ETL process for the data warehouse."""
    
    # 1. Get DB credentials and connect directly to the existing database
    host, user, password = db_connector.get_db_credentials()
    db_name = "Wal_DW"
    
    print(f"Attempting to connect to database '{db_name}'...")
    connection = db_connector.connect_to_db(host, user, password, database=db_name)
    
    # Exit if the connection could not be established
    if not connection:
        print("Failed to connect to the database. Please check credentials and ensure the database exists.")
        return

    print("Successfully connected to the database.")
    
    # 2. Load data from local CSV files
    customer_df, product_df, transaction_df = data_loader.load_csv_files()

    # 3. Populate dimension tables
    # This step will add new dimension entries if they don't exist
    data_loader.populate_dimension_tables(connection, customer_df, product_df, transaction_df)

    # 4. Run the HYBRIDJOIN simulation to enrich and load the fact table
    # You might want to clear the FactSales table before running this if you want fresh data
    # cursor = connection.cursor()
    # print("Clearing FactSales table for a fresh run...")
    # cursor.execute("DELETE FROM FactSales")
    # connection.commit()
    # cursor.close()
    
    transaction_stream = transaction_df.to_dict('records')
    master_data = {'customer': customer_df, 'product': product_df}
    hybrid_join.run_hybrid_join(connection, transaction_stream, master_data)

    # 5. Close the database connection
    if connection.is_connected():
        connection.close()
        print("\nProcess completed. Database connection closed.")

if __name__ == "__main__":
    main()