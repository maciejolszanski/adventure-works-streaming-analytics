import os
import pyodbc
from dotenv import load_dotenv


def get_conn_str_from_env_file() -> str:
    load_dotenv()

    sql_server_host = os.getenv('SQL_SERVER_HOST')
    sql_server_database = os.getenv('SQL_SERVER_DATABASE')
    sql_server_user = os.getenv('SQL_SERVER_USER')
    sql_server_password = os.getenv('SQL_SERVER_PASSWORD')

    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server_host};DATABASE={sql_server_database};UID={sql_server_user};PWD={sql_server_password}"

    return connection_string


def execute_query(connection: pyodbc.Connection, query: str) -> str:
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    return results

def get_all_tables(connection: pyodbc.Connection) -> list[str]:
    query = """
        SELECT
            CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) as table_full_name
        FROM
            INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_TYPE = 'BASE TABLE'
        """
    query_result = execute_query(connection, query)
    tables_list = [table[0] for table in query_result]

    return tables_list

connection_string = get_conn_str_from_env_file()

with pyodbc.connect(connection_string) as connection:
    print("Connection successful!")

    tables = get_all_tables(connection)
    print(tables)
