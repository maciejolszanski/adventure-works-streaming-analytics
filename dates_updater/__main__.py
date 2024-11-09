from sql_server_connector import SQLServerConnector
from dates_updater import DatesUpdater
import logging

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    sql_server = SQLServerConnector()
    dates_updater = DatesUpdater(sql_server)
    dates_updater.update_dates()