import pyarrow.flight as flight
import pyarrow as pa
import json

class RetrievalClient:
    def __init__(self, host="localhost", port=33333):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.client = flight.FlightClient(self.location)
        
    def create_table(self, database, table, schema, location, num_shards=1):
        table_settings = {
            "database": database,
            "table": table,
            "schema": schema,
            "location": location,
            "num_shards": num_shards
        }
        action = flight.Action("CreateTable", json.dumps(table_settings).encode())
        result = self.client.do_action(action)
        return json.loads(next(result).body.to_pybytes().decode())
    
    def build_from_local(self, database, table, data):
        params = [database, table, "\u0000".join(data)]
        action = flight.Action("BuildFromLocal", "\n".join(params).encode())
        result = self.client.do_action(action)
        return json.loads(next(result).body.to_pybytes().decode())
    
    def search(self, database, table, keyword=None, vector=None, vector_field=None, limit=10):
        search_query = {
            "database": database,
            "table": table,
            "filters": {},
            "sorts": [],
            "keyword": keyword,
            "fields": [],
            "vector": vector,
            "vectorField": vector_field,
            "limit": limit
        }
        ticket = flight.Ticket(json.dumps(search_query).encode())
        flight_info = self.client.get_flight_info(ticket)
        reader = self.client.do_get(ticket)
        table = reader.read_all()
        return table.to_pydict()
    
    def commit(self, database, table):
        params = [database, table]
        action = flight.Action("Commit", "\n".join(params).encode())
        result = self.client.do_action(action)
        return json.loads(next(result).body.to_pybytes().decode())
    
    def shutdown(self):
        action = flight.Action("Shutdown", b"")
        result = self.client.do_action(action)
        return json.loads(next(result).body.to_pybytes().decode())

if __name__ == "__main__":
    client = RetrievalClient()
    
    # 1. Create table
    schema = '''
    {
        "type": "struct",
        "fields": [
            {"name": "_id", "type": "long"},
            {"name": "text", "type": "string", "analyze": true},
            {"name": "vector", "type": "array", "elementType": "float"}
        ]
    }
    '''
    print("Creating table...")
    print(client.create_table("test_db", "test_table", schema, "/tmp/test_table"))
    
    # 2. Build data
    print("Building data...")
    data = [
        json.dumps({
            "_id": 0,
            "text": "This is a test document",
            "vector": [0.1, 0.2, 0.3]
        }),
        json.dumps({
            "_id": 1, 
            "text": "Another test document",
            "vector": [0.4, 0.5, 0.6]
        })
    ]
    print(client.build_from_local("test_db", "test_table", data))
    
    # 3. Commit
    print("Committing...")
    print(client.commit("test_db", "test_table"))
    
    # 4. Search by keyword
    print("Searching by keyword...")
    results = client.search("test_db", "test_table", keyword="test")
    print(results)
    
    # 5. Search by vector
    print("Searching by vector...")
    results = client.search("test_db", "test_table", vector=[0.1, 0.2, 0.3], vector_field="vector")
    print(results)
    
    # 6. Shutdown
    print("Shutting down...")
    print(client.shutdown())