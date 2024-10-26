from sql_server_connector import SQLServerConnector
import logging
import datetime

logger = logging.getLogger(__name__)

class DatesUpdater:

    def __init__(self, sql_server: SQLServerConnector):
        self.sql_server = sql_server
        self.all_tables: list[str] = []
        self.column_config: dict[str, list[str]] = {}

    def get_tables(self):
        query = """
            SELECT
                CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) as table_full_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_SCHEMA <> 'dbo'
            """
        query_result = self.sql_server.execute_query(query)
        all_tables = [table[0] for table in query_result]

        logger.info("Succesfully read all tables")

        return all_tables

    def get_datetime_columns(self):
        query = f"""
            SELECT
                CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) as table_full_name
               ,STRING_AGG(COLUMN_NAME, ',') as columns
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE DATA_TYPE IN ('datetime', 'date')
            AND CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) in {tuple(self.all_tables)}
            GROUP BY CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME)
        """
        date_columns = self.sql_server.execute_query(query)
        logger.info("Succesfully read datetimes columns")

        self.column_config = self._generate_column_config(date_columns)

    def _generate_column_config(
            self,
            table_columns: list[tuple[str, str]]
        ) -> dict[str, list[str]]:
        """Converts list of tuples into dict,
        splits second elem of the tuple by ','.
        """

        config = {table: cols.split(',') for table, cols in table_columns}
        logger.info("Succesfully prepared table-columns config")

        return config

    def get_transaction_max_date(self) -> datetime.date:

        query = """
            SELECT MAX(TransactionDate) 
            FROM AdventureWorks.Production.TransactionHistory
        """
        query_result = self.sql_server.execute_query(query)
        max_transaction_date = query_result[0][0]
        logging.info(f"Latest transaction date found: {max_transaction_date}")

        return max_transaction_date
    
    def update_dates(self):

        max_date = datetime.date(1970, 1, 1)

        for table, columns in self.column_config.items():
            for column in columns:
                logging.debug(f"table: {table}, column: {column}")

                query = f"""SELECT MAX({column}) FROM {table}"""
                query_result = self.sql_server.execute_query(query)

                logging.debug(f"result: {query_result}")

                # query result is a list of tuples
                max_col_date = query_result[0][0]

                if max_col_date is None:
                    continue

                if isinstance(max_col_date, datetime.datetime):
                    max_col_date = max_col_date.date()

                if max_col_date > max_date:
                    max_date = max_col_date

        print(max_date)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")

    sql_server = SQLServerConnector()
    dates_updater = DatesUpdater(sql_server)
    # dates_updater.get_tables()
    # dates_updater.get_datetime_columns()
    dates_updater.get_transaction_max_date()
    
