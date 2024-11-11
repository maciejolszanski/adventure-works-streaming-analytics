from ..connectors import SQLServerConnector
import random

class TransactionGenerator:
    """Class to generate transactions.
    
    Transaction can be of type P, W or S that corresponds to:
    - W - Production.WorkOrder
    - S - Sales.SalesOrderDetail
    - P - Purchasing.PurchaseOrderDetail
    """

    def __init__(self, sql_server: SQLServerConnector):
        self.sql_server = sql_server
        self.transaction_table = "AdventureWorks.Production.TransactionHistory"
        self.product_ids = self._get_product_ids()
        self.transaction_types = ['W', 'S', 'P']
        self._get_max_ids()

    
    def _get_max_ids(self):
        self.max_transaction_id = self._get_max_column_value(
            "TransactionID", self.transaction_table
        )
        self.max_reference_order_id = self._get_max_column_value(
            "ReferenceOrderID", self.transaction_table
        )
    
    def _get_product_ids(self) -> list[int]:
        product_tables = "AdventureWorks.Production.Product"
        id_column = "ProductID"

        self._get_distinct_column_values(product_tables, id_column)

    def _get_max_column_value(self, table, column) -> int:
        query = f"SELECT MAX({column}) FROM {table}"
        _, result = self.sql_server.execute_query(query)

        return result[0][0]

    def _get_distinct_column_values(self, table, column) -> list:
        query = f"""SELECT DISTINCT {column} FROM {table}"""
        _, results = self.sql_server.execute_query(query)
        results = [res[0] for res in results]
        
        return results
    
    def generate_transaction(self):
        new_transaction_id = self.max_transaction_id + 1
        new_order_id = self.max_reference_order_id + 1
        transaction_type = random.choice(self.transaction_types)

        self.max_transaction_id = new_transaction_id
        self.max_reference_order_id = new_order_id



if __name__ == "__main__":
    sql_server = SQLServerConnector()

    tr_gen = TransactionGenerator(sql_server)
    tr_gen._get_product_ids()

