# LocalByzerStorage Python API 指南

LocalByzerStorage 是一个简单易用的本地存储和检索系统，支持向量搜索和文本搜索功能。本文档将介绍如何使用 LocalByzerStorage 进行库表创建、数据写入和数据读取。

## 安装

LocalByzerStorage 是 byzerllm 包的一部分，可以通过 pip 安装：

```bash
pip install byzerllm
```

## 导入必要的库

```python
from byzerllm.apps.byzer_storage.local_simple_api import (
    LocalByzerStorage,
    DataType,
    FieldOption,
    SortOption
)
```

## 创建 LocalByzerStorage 实例

```python
from autocoder.utils.llms import get_single_llm

emb_llm = get_single_llm("emb",product_type="lite")

storage = LocalByzerStorage(
    namespace="byzerai_store",  # 命名空间
    database="my_database",     # 数据库名
    table="my_table",           # 表名
    host="127.0.0.1",           # 主机地址
    port=33333,                 # 端口
    emb_llm=emb_llm             # 嵌入模型，用于向量化内容
)
```

## 创建表结构（Schema）

在使用 LocalByzerStorage 之前，需要先定义表结构。可以使用 `schema_builder()` 方法来构建表结构：

```python
schema_result = (
    storage.schema_builder()
    .add_field("_id", DataType.STRING)                  # 定义主键字段
    .add_field("title", DataType.STRING)                # 普通字符串字段
    .add_field("content", DataType.STRING, [FieldOption.ANALYZE])  # 可全文检索字段
    .add_field("raw_content", DataType.STRING, [FieldOption.NO_INDEX])  # 不建索引字段，属于存储字段，在检索的时候可以拿到原文。
    .add_array_field("vector", DataType.FLOAT)          # 向量字段
    .add_field("timestamp", DataType.DOUBLE, [FieldOption.SORT])  # 可排序字段
    .add_field("is_active", DataType.BOOLEAN)           # 布尔字段
    .add_field("count", DataType.INTEGER)               # 整数字段
    .execute()                                          # 执行创建操作
)
```

### 字段类型

- `DataType.STRING`：字符串类型
- `DataType.INTEGER`：整数类型
- `DataType.DOUBLE`：浮点数类型
- `DataType.BOOLEAN`：布尔类型
- `DataType.FLOAT`：浮点数类型（通常用于向量）

### 字段选项

- `FieldOption.ANALYZE`：表示该字段将会被分析和索引，用于全文搜索
- `FieldOption.NO_INDEX`：表示该字段不会被索引，只用于存储
- `FieldOption.SORT`：表示该字段可以用于排序

## 写入数据

LocalByzerStorage 支持单条和批量写入数据：

### 单条数据写入

```python
item = {
    "_id": "doc1",
    "title": "示例文档",
    "content": "这是一个示例文档，用于演示 LocalByzerStorage 的使用方法。",
    "raw_content": "这是一个示例文档，用于演示 LocalByzerStorage 的使用方法。",
    "vector": "这是一个示例文档，用于演示 LocalByzerStorage 的使用方法。", 
    "timestamp": 1623456789.0,
    "is_active": True,
    "count": 42
}

result = storage.write_builder().add_item(item,
    vector_fields=["vector"],     # 指定哪些字段是向量字段
    search_fields=["content"]     # 指定哪些字段用于全文搜索
).execute()
```

### 批量数据写入

```python
items = [
    {
        "_id": "doc1",
        "title": "示例文档1",
        "content": "示例文档1",
        "vector": "示例文档1",
        "timestamp": 1623456789.0,
        "is_active": True,
        "count": 42
    },
    {
        "_id": "doc2",
        "title": "示例文档2",
        "content": "示例文档2",
        "vector": "示例文档2",
        "timestamp": 1623456789.0,
        "is_active": True,
        "count": 42
    }
]

result = storage.write_builder().add_items(
    items, 
    vector_fields=["vector"],     # 指定哪些字段是向量字段
    search_fields=["content"]     # 指定哪些字段用于全文搜索
).execute()
```

### 提交数据

在写入数据后，需要调用 `commit()` 方法来确保数据被持久化：

```python
storage.commit()
```

## 删除数据

可以通过ID删除单条或多条数据：

```python
# 删除单条数据
storage.delete_by_id("doc1")

# 删除多条数据
storage.delete_by_ids(["doc1", "doc2", "doc3"])
```

## 清空表

如果需要清空整个表的数据：

```python
storage.truncate_table()
```

## 查询数据

LocalByzerStorage 提供了多种查询方式，包括ID查询、条件查询、全文搜索和向量搜索：

### 通过ID查询

```python
# 查询单个文档
doc = storage.get_by_id("doc1")

# 查询多个文档
docs = storage.get_by_ids(["doc1", "doc2", "doc3"])
```

### 条件查询

使用 `query_builder()` 构建复杂的查询条件：

```python
query = storage.query_builder()

# 添加条件过滤
query.and_filter().add_condition("is_active", True).build()

# 设置排序
query.add_sort("timestamp", SortOption.DESC)

# 设置分页
query.set_limit(10)

# 执行查询
results = query.execute()
```

### 全文搜索

```python
query = storage.query_builder()
query.set_search_query("示例文档", fields=["content"])
results = query.execute()
```

### 向量搜索

```python
## 直接使用一个外部提供的向量做查询
query = storage.query_builder()
query.set_vector_query([0.1, 0.2, 0.3, 0.4], fields=["vector"])
results = query.execute()

## 也可以将字符串转化为向量然后做查询
vector = storage.emb("这是一个示例文档，用于演示 LocalByzerStorage 的使用方法。")
query.set_vector_query(vector, fields=["vector"])
results = query.execute()
```

### 组合查询（同时使用全文搜索和向量搜索）

```python
query = storage.query_builder()

# 设置向量搜索
query.set_vector_query([0.1, 0.2, 0.3, 0.4], fields=["vector"])

# 设置全文搜索
query.set_search_query("示例文档", fields=["content"])

# 添加过滤条件
query.and_filter().add_condition("is_active", True).build()

# 设置结果数量限制
query.set_limit(100)

# 执行查询
results = query.execute()
```

## 实际应用示例

以下是一个完整的示例，演示如何使用 LocalByzerStorage 存储和检索文档：

```python
from byzerllm.apps.byzer_storage.local_simple_api import (
    LocalByzerStorage, 
    DataType, 
    FieldOption, 
    SortOption
)

# 创建存储实例
storage = LocalByzerStorage(
    namespace="my_app",
    database="documents",
    table="articles",
    host="127.0.0.1",
    port=33333
)

# 定义表结构
_ = (
    storage.schema_builder()
    .add_field("_id", DataType.STRING)
    .add_field("title", DataType.STRING, [FieldOption.ANALYZE])
    .add_field("content", DataType.STRING, [FieldOption.ANALYZE])
    .add_array_field("vector", DataType.FLOAT)
    .add_field("timestamp", DataType.DOUBLE, [FieldOption.SORT])
    .execute()
)

# 添加数据
items = [
    {
        "_id": "article1",
        "title": "Python 编程入门",
        "content": "Python是一种简单易学的编程语言...",
        "vector": [0.1, 0.2, 0.3, 0.4],
        "timestamp": 1623456789.0
    },
    {
        "_id": "article2",
        "title": "机器学习基础",
        "content": "机器学习是人工智能的一个分支...",
        "vector": [0.5, 0.6, 0.7, 0.8],
        "timestamp": 1623456999.0
    }
]

# 写入数据
storage.write_builder().add_items(
    items, 
    vector_fields=["vector"], 
    search_fields=["title", "content"]
).execute()

# 提交数据
storage.commit()

# 全文搜索
results = storage.query_builder().set_search_query("Python").execute()
print("搜索 'Python' 的结果：")
for result in results:
    print(f"标题: {result['title']}")

# 向量搜索
query_vector = [0.1, 0.2, 0.3, 0.4]
results = storage.query_builder().set_vector_query(query_vector, fields=["vector"]).execute()
print("\n向量搜索结果：")
for result in results:
    print(f"标题: {result['title']}")

# 组合搜索
results = (
    storage.query_builder()
    .set_search_query("机器学习")
    .set_vector_query(query_vector, fields=["vector"])
    .add_sort("timestamp", SortOption.DESC)
    .execute()
)

print("\n组合搜索结果：")
for result in results:
    print(f"标题: {result['title']}, 时间戳: {result['timestamp']}")
```

## 注意事项

1. 使用 `add_array_field` 方法定义向量字段，而不是 `add_field`
2. 确保在写入数据后调用 `commit()` 方法
3. 向量搜索需要指定向量字段，全文搜索需要指定文本字段
4. 向量字段的值应该是浮点数的列表/数组
5. 使用 `FieldOption.ANALYZE` 标记需要全文检索的字段
6. 使用 `FieldOption.SORT` 标记需要排序的字段

LocalByzerStorage 提供了一种简单而强大的方式来实现向量和文本的混合搜索，特别适合构建本地化的语义搜索和RAG（检索增强生成）应用。 