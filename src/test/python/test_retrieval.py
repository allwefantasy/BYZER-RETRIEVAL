import pyarrow.flight as flight
import pyarrow as pa
import json

class RetrievalClient:
    def __init__(self, host="localhost", port=33333):
        self.location = flight.Location.for_grpc_tcp(host, port)
        self.client = flight.FlightClient(self.location)
        
    def create_table(self, database, table, schema, location, num_shards=1):
        database_arr = pa.array([database], type=pa.string())
        table_arr = pa.array([table], type=pa.string())
        schema_arr = pa.array([schema], type=pa.string())
        location_arr = pa.array([location], type=pa.string())
        num_shards_arr = pa.array([num_shards], type=pa.int32())

        batch = pa.RecordBatch.from_arrays(
            [database_arr, table_arr, schema_arr, location_arr, num_shards_arr],
            names=["database", "table", "schema", "location", "numShards"]
        )
        
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        arrow_data = sink.getvalue()

        action = flight.Action("CreateTable", arrow_data)
        result = self.client.do_action(action)
        return json.loads(next(result).body.to_pybytes().decode())
    
    def delete_by_filter(self, database, table, condition):
        database_arr = pa.array([database], type=pa.string())
        table_arr = pa.array([table], type=pa.string())
        condition_arr = pa.array([condition], type=pa.string())

        batch = pa.RecordBatch.from_arrays(
            [database_arr, table_arr, condition_arr],
            names=["database", "table", "condition"]
        )
        
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        arrow_data = sink.getvalue()

        action = flight.Action("DeleteByFilter", arrow_data)
        result = self.client.do_action(action)
        return json.loads(next(result).body.to_pybytes().decode())
    
    def build_from_local(self, database, table, data):
        database_arr = pa.array([database], type=pa.string())
        table_arr = pa.array([table], type=pa.string())
        data_arr = pa.array([data], type=pa.list_(pa.string()))

        batch = pa.RecordBatch.from_arrays(
            [database_arr, table_arr, data_arr],
            names=["database", "table", "data"]
        )
        
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        arrow_data = sink.getvalue()

        action = flight.Action("BuildFromLocal", arrow_data)
        result = self.client.do_action(action)
        return json.loads(next(result).body.to_pybytes().decode())
    
    def search(self, query_json):
        """
        Execute a search with the provided query JSON
        
        Args:
            query_json: JSON string or dictionary containing the search query
        """
        if isinstance(query_json, dict):
            query_json = json.dumps(query_json)
            
        ticket = flight.Ticket(query_json.encode())
        flight_info = self.client.get_flight_info(ticket)
        reader = self.client.do_get(flight_info.endpoints[0].ticket)
        table = reader.read_all()
        return table.to_pydict()
    
    def search_by_keyword(self, database, table, keyword, limit=10):
        """Helper method to search by keyword"""
        query = {
            "database": database,
            "table": table,
            "keyword": keyword,
            "limit": limit
        }
        return self.search(query)
    
    def search_by_vector(self, database, table, vector, vector_field, limit=10):
        """Helper method to search by vector"""
        query = {
            "database": database,
            "table": table,
            "vector": vector,
            "vectorField": vector_field,
            "limit": limit
        }
        return self.search(query)
    
    def commit(self, database, table):
        """
        Commit changes to a table
        
        Note: This implementation assumes the server has a specific action for commit
        If not implemented on the server side, this would need to be modified
        """
        database_arr = pa.array([database], type=pa.string())
        table_arr = pa.array([table], type=pa.string())

        batch = pa.RecordBatch.from_arrays(
            [database_arr, table_arr],
            names=["database", "table"]
        )
        
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        arrow_data = sink.getvalue()

        action = flight.Action("Commit", arrow_data)
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
    results = client.search_by_keyword("test_db", "test_table", "test")
    print(results)
    
    # 5. Search by vector
    print("Searching by vector...")
    results = client.search_by_vector("test_db", "test_table", [0.1, 0.2, 0.3], "vector")
    print(results)
    
    # 6. Delete by filter
    print("Deleting documents with filter...")
    print(client.delete_by_filter("test_db", "test_table", "_id = 0"))
    
    # 7. Shutdown
    print("Shutting down...")
    print(client.shutdown())