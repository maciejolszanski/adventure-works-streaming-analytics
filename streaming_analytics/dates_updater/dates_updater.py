from ..connectors import SQLServerConnector
import logging
import datetime

logger = logging.getLogger(__name__)

class DatesUpdater:

    def __init__(self, sql_server: SQLServerConnector):
        self.sql_server = sql_server
        self.all_tables: list[str] = self._get_tables()
        self.column_config: dict[str, list[str]] = self._get_datetime_columns()
        self.max_transaction_date: datetime.date = self._get_transaction_max_date()

    def _execute_query(self, query: str) -> list:
        """Executes a query and returns results, with logging."""
        try:
            result = self.sql_server.execute_query(query)
            logger.debug(f"Executed query: {query}")
            return result
        except Exception as e:
            logger.error(f"Error executing query: {query} - {e}")
            raise

    def _get_tables(self) ->list[str]:
        query = """
            SELECT
                CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) as table_full_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_SCHEMA <> 'dbo'
            """
        query_result = self._execute_query(query)
        all_tables = [table[0] for table in query_result]
        logger.info("Succesfully retrieved all tables")

        return all_tables

    def _get_datetime_columns(self) -> dict[str, list[str]]:
        query = f"""
            SELECT
                CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) as table_full_name
               ,STRING_AGG(COLUMN_NAME, ',') as columns
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE DATA_TYPE IN ('datetime', 'date')
            AND CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) in {tuple(self.all_tables)}
            GROUP BY CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME)
        """
        date_columns = self._execute_query(query)
        logger.info("Succesfully read datetimes columns")

        column_config = self._generate_column_config(date_columns)
        return column_config

    @staticmethod
    def _generate_column_config(
            table_columns: list[tuple[str, str]]
        ) -> dict[str, list[str]]:
        """Converts list of tuples into dict,
        splits second elem of the tuple by ','.
        """
        config = {table: cols.split(',') for table, cols in table_columns}

        return config

    def _get_transaction_max_date(self) -> datetime.date:
        
        table = "AdventureWorks.Production.TransactionHistory"
        column = "TransactionDate"
        max_transaction_date = self._get_col_max_date(table, column)
        logging.info(f"Latest transaction date found: {max_transaction_date}")

        return max_transaction_date
    
    def _get_col_max_date(self, table: str, column: str) -> datetime.date | None:
        query = f"""
            SELECT MAX({column}) 
            FROM {table}
        """
        query_result = self._execute_query(query)
        max_date = query_result[0][0]

        if isinstance(max_date, datetime.datetime):
            return max_date.date()
        elif isinstance(max_date, datetime.date):
            return max_date
        else:
            logger.warning(f"{table}.{column} max_date is of type {type(max_date)}, not a date or datetime.")
            return None

    @staticmethod
    def _get_diff_days_from_now(date: datetime.date) -> int:
        date_diff = datetime.date.today() - date
        
        return date_diff.days

    def update_dates(self):
        """Main function to update dates based on transaction and current date."""
        
        logger.info("updating days based on max transaction date.")
        self._updates_dates_based_on_transaction_max_date()
        logger.info("updating days if later than today")
        self._update_dates_if_later_than_today()

    def _updates_dates_based_on_transaction_max_date(self):
        """Updates dates by adding the difference from the max transaction date to today."""

        days_to_add = self._get_diff_days_from_now(self.max_transaction_date)
        for table, columns in self.column_config.items():
            self._add_days_to_columns(table, columns, days_to_add)

    def _update_dates_if_later_than_today(self):
        """Adjusts dates for each table if a column's max date is later than today."""

        today_date = datetime.date.today()
        for table, columns in self.column_config.items():
            
            if not columns:
                logger.error(f"No columns provided for table {table}")
                continue

            max_table_date = max(
                (self._get_col_max_date(table, column) or today_date for column in columns),
            )

            if max_table_date > today_date:
                # days_to_add should be negative here
                days_to_add = self._get_diff_days_from_now(max_table_date)
                logger.debug(f"{table}, max_table_date: {max_table_date}, days_diff: {days_to_add}")
                self._add_days_to_columns(table, columns, days_to_add)

    def _add_days_to_columns(
            self,
            table: str,
            columns: list[str],
            days_to_add: int
        ):
        """Updates each column in the specified table by adding a given number of days."""

        dateadd_strings = []
        for column in columns:
            dateadd_strings.append(
                f"{column} = DATEADD(DAY, {days_to_add}, {column})"
            )

        dateadd_string = ', '.join(dateadd_strings)

        query = f"""UPDATE {table} SET {dateadd_string}"""
        logger.debug(f"Updating table with query: {query}")
        self._execute_query(query)

