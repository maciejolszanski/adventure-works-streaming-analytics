import os
import pyodbc
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

class SQLServerConnector:
    """
    A class to manage the connection to a SQL Server database and execute SQL queries.

    This class provides functionality to connect to a SQL Server database, execute SQL queries 
    (including SELECT and non-SELECT queries), and close the database connection. It can automatically 
    load the connection string from environment variables if not provided. The class supports 
    safe connection handling by closing the connection when the object is destroyed.

    Attributes:
        connection (pyodbc.Connection): The connection object used to interact with the SQL Server database.
    
    Methods:
        execute_query(query: str) -> tuple[list[str], list[pyodbc.Row]]:
            Executes a SQL query on the connected SQL Server database, returns column names and rows for SELECT queries,
            or commits the transaction for non-SELECT queries.
        
        close_connection() -> None:
            Closes the connection to the SQL Server database.
    """
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
        
    def execute_query(self, query: str) -> tuple[list[str], list[pyodbc.Row]]:
        """
        Executes a SQL query on the connected SQL Server database.

        If the query is a `SELECT` statement, it fetches the column names and 
        the resulting rows. If the query is not a `SELECT` statement (e.g., an 
        `INSERT`, `UPDATE`, or `DELETE`), it commits the transaction but does 
        not return any results.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            tuple:
                - A list of column names (if it's a `SELECT` query) or an empty list (if not).
                - A list of rows returned from the query (if it's a `SELECT` query) or an empty list (if not).
                The rows are returned as `pyodbc.Row` objects, which allow access by column name or index.

        Raises:
            Exception: If an error occurs during query execution, an exception is logged and re-raised.

        Example:
            For a `SELECT` query:
            ```
            column_names, rows = execute_query("SELECT * FROM table_name")
            print(column_names)  # List of column names
            for row in rows:
                print(row)  # Each row is a pyodbc.Row object
            ```

            For a non-`SELECT` query:
            ```
            execute_query("INSERT INTO table_name (col1, col2) VALUES (val1, val2)")
            ```
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)

            if query.strip().upper().startswith("SELECT"):
                column_names = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                results = (column_names, rows) 
            else:
                self.connection.commit()
                results = ([], [])

            logger.debug(f"Executed query: {query}")
            return results
        
        except Exception as e:
            logger.error(f"Error executing query: {query} - {e}")
            raise
