# db_connector.py
import mysql.connector
import getpass

def get_db_credentials():
    """Collects database credentials from the user."""
    print("Please enter your MySQL database credentials:")
    host = input("Host: ")
    user = input("User: ")
    password = getpass.getpass("Password: ")
    return host, user, password

def connect_to_db(host, user, password, database=None):
    """Establishes a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None