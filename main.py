# main.py
import DB_connect as db_connector
import data_loader
import ETL

def main():
    """Main function to run the ETL process for the data warehouse."""
    
    # 1. Get DB credentials and connect directly to the existing database
    host, user, password = db_connector.get_db_credentials()
    db_name = "wal_DW"
    
    print(f"Attempting to connect to database '{db_name}'...")
    connection = db_connector.connect_to_db(host, user, password, database=db_name)
    
    # Exit if the connection could not be established
    if not connection:
        print("Failed to connect to the database. Please check credentials and ensure the database exists.")
        return

    print("Successfully connected to the database.")
    
    # 2. Load data from local CSV files
    customer_df, product_df, _ = data_loader.load_csv_files()
    
    master_data = {'customer': customer_df, 'product': product_df}
    
    # 3. Run the ETL process
    ETL.run_etl(
        connection, 
        master_data
    )

    # 4. Close the database connection
    if connection.is_connected():
        connection.close()
        print("\nProcess completed. Database connection closed.")

if __name__ == "__main__":
    main()

