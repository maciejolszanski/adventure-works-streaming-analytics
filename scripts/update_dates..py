from sql_server_connector import SQLServerConnector
import logging

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    sql_server = SQLServerConnector()
    tables = sql_server.get_all_tables()
    logger.debug(tables)

    sql_server.close_connection()
