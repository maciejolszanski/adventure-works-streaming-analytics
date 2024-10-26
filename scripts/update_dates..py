from sql_server_connector import SQLServerConnector
import logging

logger = logging.getLogger(__name__)

class DatesUpdater:

    def __init__(self):
        self.sql_server = SQLServerConnector()

    def get_all_tables(self) -> list[str]:
        query = """
            SELECT
                CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) as table_full_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            """
        query_result = self.sql_server.execute_query(query)
        tables_list = [table[0] for table in query_result]

        return tables_list

    def get_all_datetime_columns(self, tables:list) -> dict[str, list[str]]:
        query = """
        SELECT
            *
        FROM INFORMATION_SCHEMA.COLUMNS
        """

if __name__ == "__main__":
    logging.basicConfig(level="INFO")


