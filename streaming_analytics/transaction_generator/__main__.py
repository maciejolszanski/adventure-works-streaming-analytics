from ..connectors import SQLServerConnector

class TransactionGenerator:

    def __init__(self, sql_server: SQLServerConnector):
        self.sql_server = sql_server

    def get_max_transaction_id(self):
        

if __name__ == "__main__":
    sql_server = SQLServerConnector()

    query = "SELECT TOP 1 * FROM AdventureWorks.Production.TransactionHistory"
    print(sql_server.execute_query(query))

