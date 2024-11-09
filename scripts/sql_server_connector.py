import os
import pyodbc
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

class SQLServerConnector():

    def __init__(self, connection_string: str=''):
        if not connection_string:
            connection_string = self._get_conn_str_from_env_file()

        self.connection = self._connect(connection_string)

    def _get_conn_str_from_env_file(self ) -> str:
        load_dotenv()

        sql_server_host = os.getenv('SQL_SERVER_HOST')
        sql_server_database = os.getenv('SQL_SERVER_DATABASE')
        sql_server_user = os.getenv('SQL_SERVER_USER')
        sql_server_password = os.getenv('SQL_SERVER_PASSWORD')

        connection_string = connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={sql_server_host};"
            f"DATABASE={sql_server_database};"
            f"UID={sql_server_user};"
            f"PWD={sql_server_password}"
        )

        return connection_string
    
    def _connect(self, connection_string):
        try:
            connection = pyodbc.connect(connection_string)
            logger.info("Connected to SQL Server Database")
            return connection
        except pyodbc.Error as e:
            logger.error(
                "Couldn't establish connection with SQL Server",
                exc_info=e
            )
            raise ConnectionError("Failed to connect to SQL Server") from e
    
    def close_connection(self) -> None:
        try:
            self.connection.close()
            logger.info("Succesfully closed connection")
        except Exception as e:
            logger.error(
                f"Couldn't close the connection due to an error: {e}")

    def __del__(self):
        # This function is called when the object is about to be destroyed
        self.close_connection()
        
    def execute_query(self, query: str) -> str:
        cursor = self.connection.cursor()
        cursor.execute(query)

        if query.strip().upper().startswith("SELECT"):
            results = cursor.fetchall()
            return results
        else:
            self.connection.commit()
            return "Commited"
