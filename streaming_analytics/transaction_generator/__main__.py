from ..connectors import SQLServerConnector

class TransactionGenerator:

    def __init__(self, sql_server: SQLServerConnector):
        self.sql_server = sql_server
        self.transaction_table = "AdventureWorks.Production.TransactionHistory"
        self.max_transaction_id = self._get_max_transaction_id()

    def _get_max_transaction_id(self):
        id_column = "TransactionID"
        query = f"SELECT MAX({id_column}) FROM {self.transaction_table}"
        _, max_id = self.sql_server.execute_query(query)

        return max_id[0][0]


if __name__ == "__main__":
    sql_server = SQLServerConnector()

    tr_gen = TransactionGenerator(sql_server)

