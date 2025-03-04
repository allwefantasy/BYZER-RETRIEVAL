# Arrow Flight Client
import pyarrow.flight as flight

class ArrowFlightClient:
    def __init__(self, host="localhost", port=33333):
        self.client = flight.connect(f"grpc://{host}:{port}")
        
    def create_table(self, table_settings):
        action = flight.Action("CreateTable", table_settings.encode())
        result = self.client.do_action(action).next()
        return result.body.to_pybytes().decode() == "true"
    
    def search(self, query):
        ticket = flight.Ticket(query.encode())
        reader = self.client.do_get(ticket)
        return reader.read_all().to_pandas()

# # Py4J Client
# from py4j.java_gateway import JavaGateway
#
# class Py4JClient:
#     def __init__(self):
#         self.gateway = JavaGateway()
#         self.retrieval = self.gateway.entry_point
#
#     def create_table(self, table_settings):
#         return self.retrieval.createTable(table_settings)
#
#     def search(self, query):
#         return self.retrieval.search(query)
#
# # Usage Example
# if __name__ == "__main__":
#     # Using Arrow Flight
#     arrow_client = ArrowFlightClient()
#     arrow_client.create_table('{"database": "db1", "table": "table1"}')
#     results = arrow_client.search('{"query": "..."}')
#     print(results)
#
#     # Using Py4J
#     py4j_client = Py4JClient()
#     py4j_client.create_table('{"database": "db1", "table": "table1"}')
#     results = py4j_client.search('{"query": "..."}')
#     print(results)