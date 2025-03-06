import pyarrow as pa
import pyarrow.flight as flight
import json
from typing import List, Dict, Any, Optional, Union
from byzerllm.apps.byzer_storage.simple_api import (
    SchemaBuilder,
    DataType,
    FieldOption,
    SortOption,
    QueryBuilder,
    SearchQuery
)

class RetrievalClient:
    def __init__(self, host: str = "localhost", port: int = 33333):
        """Initialize the retrieval client with connection to Arrow Flight server.
        
        Args:
            host: The hostname of the Arrow Flight server
            port: The port of the Arrow Flight server
        """
        self.client = flight.FlightClient(f"grpc://{host}:{port}")
    
    def create_table(self, database: str, table: str, schema: str, 
                     location: str, num_shards: int) -> bool:
        """Create a new table in the retrieval system.
        
        Args:
            database: The database name
            table: The table name
            schema: The schema definition in string format
            location: The location to store the table data
            num_shards: Number of shards for the table
        
        Returns:
            True if successful, False otherwise
        """
        # Create a record batch with the table settings
        batch_data = [
            pa.array([database]),
            pa.array([table]),
            pa.array([schema]),
            pa.array([location]),
            pa.array([num_shards], type=pa.int32())
        ]
        
        batch = pa.RecordBatch.from_arrays(
            batch_data,
            names=['database', 'table', 'schema', 'location', 'numShards']
        )
        
        # Convert to IPC message
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        
        # Send action to server
        action = flight.Action("CreateTable", sink.getvalue().to_pybytes())
        results = list(self.client.do_action(action))
        
        if results and len(results) > 0:
            return results[0].body.to_pybytes().decode('utf-8') == "true"
        return False
    
    def build_from_local(self, database: str, table: str, data: List[Dict[str, Any]]) -> bool:
        """Build the table from local data.
        
        Args:
            database: The database name
            table: The table name
            data: List of dictionaries containing the data
        
        Returns:
            True if successful, False otherwise
        """
        # Convert data to JSON strings
        json_data = [json.dumps(item) for item in data]
        
        # Create a record batch with the data
        batch_data = [
            pa.array([database]),
            pa.array([table]),
            pa.array([json_data])
        ]
        
        batch = pa.RecordBatch.from_arrays(
            batch_data,
            names=['database', 'table', 'data']
        )
        
        # Convert to IPC message
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        
        # Send action to server
        action = flight.Action("BuildFromLocal", sink.getvalue().to_pybytes())
        results = list(self.client.do_action(action))
        
        if results and len(results) > 0:
            return results[0].body.to_pybytes().decode('utf-8') == "true"
        return False
    
    def search(self, database: str, table: str, 
               keyword: Optional[str] = None,
               vector: Optional[List[float]] = None,
               vector_field: Optional[str] = None,
               filters: Optional[List[Dict[str, Any]]] = None,
               sorts: Optional[List[Dict[str, str]]] = None,
               fields: Optional[List[str]] = None,
               limit: int = 10) -> List[Dict[str, Any]]:
        """使用给定的查询参数搜索表。
        
        参数:
            database: 数据库名称
            table: 表名称
            keyword: 可选的文本搜索关键词
            vector: 可选的向量搜索值
            vector_field: 向量搜索的字段名
            filters: 可选的过滤条件
            sorts: 可选的排序条件
            fields: 可选的返回字段列表
            limit: 返回结果的最大数量
        
        返回:
            匹配文档的列表
        """
        # 构建搜索查询        
        search_query = SearchQuery(
            database=database,
            table=table,
            keyword=keyword,
            vector=vector or [],
            vectorField=vector_field,
            filters=filters or {},
            fields=fields or [],
            sorts=sorts,
            limit=limit,
        )
        # 将查询转换为JSON
        query_json = f"[{','.join([x.json() for x in [search_query]])}]"
        
        # 创建包含查询的记录批次
        batch_data = [
            pa.array([database]),
            pa.array([table]),
            pa.array([query_json])
        ]
        
        batch = pa.RecordBatch.from_arrays(
            batch_data,
            names=['database', 'table', 'query']
        )
        
        # 转换为IPC消息
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        
        # 向服务器发送操作
        action = flight.Action("Search", sink.getvalue().to_pybytes())
        results = list(self.client.do_action(action))
        
        if results and len(results) > 0:
            result_json = results[0].body.to_pybytes().decode('utf-8')
            return json.loads(result_json)
        return []
    
    def commit(self, database: str, table: str) -> bool:
        """Commit changes to the table.
        
        Args:
            database: The database name
            table: The table name
        
        Returns:
            True if successful, False otherwise
        """
        # Create a record batch with the database and table
        batch_data = [
            pa.array([database]),
            pa.array([table])
        ]
        
        batch = pa.RecordBatch.from_arrays(
            batch_data,
            names=['database', 'table']
        )
        
        # Convert to IPC message
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        
        # Send action to server
        action = flight.Action("Commit", sink.getvalue().to_pybytes())
        results = list(self.client.do_action(action))
        
        if results and len(results) > 0:
            return results[0].body.to_pybytes().decode('utf-8') == "true"
        return False
    
    def delete_by_filter(self, database: str, table: str, condition: str) -> bool:
        """Delete documents matching the condition.
        
        Args:
            database: The database name
            table: The table name
            condition: Filter condition in JSON format
        
        Returns:
            True if successful, False otherwise
        """
        # Create a record batch with the filter condition
        batch_data = [
            pa.array([database]),
            pa.array([table]),
            pa.array([condition])
        ]
        
        batch = pa.RecordBatch.from_arrays(
            batch_data,
            names=['database', 'table', 'condition']
        )
        
        # Convert to IPC message
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchStreamWriter(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        
        # Send action to server
        action = flight.Action("DeleteByFilter", sink.getvalue().to_pybytes())
        results = list(self.client.do_action(action))
        
        if results and len(results) > 0:
            return results[0].body.to_pybytes().decode('utf-8') == "true"
        return False
    
    def shutdown(self) -> bool:
        """Shutdown the retrieval server.
        
        Returns:
            True if successful, False otherwise
        """
        action = flight.Action("Shutdown", b"")
        results = list(self.client.do_action(action))
        
        if results and len(results) > 0:
            return results[0].body.to_pybytes().decode('utf-8') == "true"
        return False

# Example usage
if __name__ == "__main__":
    client = RetrievalClient()
    
    # Create a table
    schema = "st(field(id,string),field(content,string,analyze),field(raw_content,string),field(vector,array(float)))"
    client.create_table("test_db", "test_table", schema, "/tmp/test_table", 1)
    
    # Add some data
    data = [
        {"_id": "1", "content": "This is a test document",  "raw_content": "This is a test document", "vector": [0.1, 0.2, 0.3]},
        {"_id": "2", "content": "Another test document", "raw_content": "Another test document", "vector": [0.4, 0.5, 0.6]},
        {"_id": "3", "content": "Third test document", "raw_content": "Third test document", "vector": [0.7, 0.8, 0.9]}
    ]
    client.build_from_local("test_db", "test_table", data)
    
    # Commit changes
    client.commit("test_db", "test_table")
    
    # # Search by keyword
    # results = client.search("test_db", "test_table", keyword="test document")
    # print("Keyword search results:", results)
    
    # # Search by vector
    # results = client.search("test_db", "test_table", vector=[0.1, 0.2, 0.3], vector_field="vector")
    # print("Vector search results:", results)
    
    # #Combined search
    results = client.search("test_db", "test_table", 
                           keyword="test", 
                           vector=[0.1, 0.2, 0.3], 
                           vector_field="vector")
    print("Combined search results:", results)