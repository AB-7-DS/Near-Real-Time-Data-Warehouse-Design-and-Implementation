# db_connector.py
import configparser
import mysql.connector

def get_db_credentials():
    """Reads database credentials from config.ini."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    host = config['database']['host']
    user = config['database']['user']
    password = config['database']['password']
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