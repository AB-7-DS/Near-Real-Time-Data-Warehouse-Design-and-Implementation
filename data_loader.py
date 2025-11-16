import pandas as pd


def load_csv_files():
    """Loads and prepares the datasets from CSV files for staging."""
    print("Loading data from CSV files...")
    customer_df = pd.read_csv('data/customer_master_data.csv')
    product_df = pd.read_csv('data/product_master_data.csv')
    transaction_df = pd.read_csv('data/transactional_data.csv')

    # Rename columns for schema consistency before staging/HYBRIDJOIN
    product_df.rename(
        columns={
            'price$': 'Price',
            'storeID': 'Store_ID',
            'supplierID': 'Supplier_ID',
            'storeName': 'Store_Name',
            'supplierName': 'Supplier_Name',
        },
        inplace=True,
    )
    transaction_df.rename(
        columns={'orderID': 'Order_ID', 'quantity': 'Quantity', 'date': 'Date'},
        inplace=True,
    )
    transaction_df['Date'] = pd.to_datetime(transaction_df['Date'])

    print("CSV data loaded, cleansed, and staged.")
    return customer_df, product_df, transaction_df