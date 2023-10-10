# BYZER-RETRIEVAL

## Introduction

Byzer-retrieval is a distributed retrieval system which designed as a backend for LLM RAG (Retrieval Augmented Generation).
The system supports both BM25 retrieval algorithm and vector retrieval algorithm, this means there is no need to deploy 
so many systems e.g. the Elasticsearch or Milvus,
and reduce the cost of deployment and maintenance.

The project is implemented based on Lucene + Ray which use Lucene to build the inverted index/vector index and 
use Ray to build the distributed system. Notice that this project requires JDK 21 or higher, because the new features of JDK 21 e.g. vector API
and foreign memory will bring a great performance improvement to the system.

## Prequisites

1. Ray cluster == 2.7.0
2. JDK 21 or higher
3. Python 3.10.11
4. byzerllm (python package) 

## Deploy

Clone this project and build the jar file and dependency:

```
mvn clean package -DskipTests
mvn dependency:copy-dependencies -DoutputDirectory=target/dependency
```

Then copy the jar file and dependency to the ray cluster. Suppose you put
all the jars in `/home/winubuntu/softwares/byzer-retrieval-lib/`.

Also make sure `byzerllm` is installed in your python environment of ray cluster.

That's all.

## Usage (high-level Python API)

We provide the python API to build the index, you can use the following code to build the index.

```python
import ray

code_search_path=["/home/winubuntu/softwares/byzer-retrieval-lib/"]
env_vars = {"JAVA_HOME": "/home/winubuntu/softwares/jdk-21",
            "PATH":"/home/winubuntu/softwares/jdk-21/bin:/home/winubuntu/.cargo/bin:/usr/local/cuda/bin:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.1.1-2.3.2/jdk8/bin:/home/winubuntu/miniconda3/envs/byzerllm-dev/bin:/home/winubuntu/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"}


ray.init(address="auto",namespace="default",
                 job_config=ray.job_config.JobConfig(code_search_path=code_search_path,
                                                      runtime_env={"env_vars": env_vars})
                 )            
```

In the above code, we use the `ray.init` to connect an existing ray cluster, and we set the `code_search_path`
to the path of the jar files which we already put in ray cluster, and set the `runtime_env` to the path of the JDK 21.  
Notice that `JAVA_HOME`, `PATH` are both required.

Then we can start a retrieval cluster:

```python
from byzerllm.records import EnvSettings,ClusterSettings,TableSettings,JVMSettings
from byzerllm.utils.retrieval import ByzerRetrieval
import os
import ray

byzer = ByzerRetrieval()
byzer.launch_gateway()
byzer.start_cluster(
        cluster_settings=ClusterSettings(
            name="cluster1",
            location="/tmp/cluster1",
            numNodes=1
        ),
            env_settings=EnvSettings(
            javaHome=env_vars["JAVA_HOME"],
            path=env_vars["PATH"]
        ), 
            jvm_settings=JVMSettings(
            options=[]
        ))

```

In this step, we will start cluster named `cluster1` with only one node, and the cluster will store the data in `/tmp/cluster1`.
Notice that we still need to set the `JAVA_HOME` and `PATH` in `EnvSettings`.  You can use jvm_options to set the JVM options e.g.
`-Xmx32g` to set the max heap size of Retriveal Node.

Then we can create a table in the cluster:

```python
byzer.create_table("cluster1",TableSettings(
    database="db1",table="table1",
    schema="st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))",
    location="/tmp/cluster1",num_shards=1,
))
```

After that, we can insert some data into the table:

```python
data = [
    {"_id":1, "name":"a", "content":"b c", "vector":[1.0,2.0,3.0]},
    {"_id":2, "name":"d", "content":"b e", "vector":[1.0,2.6,4.0]}
]

import json

data_refs = []

for item in data:
    itemref = ray.put(json.dumps(item,ensure_ascii=False))
    data_refs.append(itemref)

byzer.build("cluster1","db1","table1",data_refs)

byzer.commit("cluster1","db1","table1")
```
In this step, we will insert the data into the table, and build the index. Notice that 

1. the data should be put in the ray cluster object store.
2. we need to commit the index after building the index to make the index persistent.
3. we strongly recommend use JuiceFS to store the index data, because JuiceFS is a distributed file system which is 
   compatible with POSIX, and it is very easy to deploy and use. 


For now, we can search the data.

try to search by keyword:
```python
byzer.search_keyword("cluster1","db1","table1",
                     keyword="c",fields=["content"],limit=10)
## output: [{'name': 'a', '_id': 1, '_score': 0.31506687, 'content': 'b c'}]
```

try to search by vector:
```python
byzer.search_vector("cluster1","db1","table1",
                    vector=[1.0,2.0,3.0],vector_field="vector",limit=10)
## output: [{'name': 'a', '_id': 1, '_score': 1.0, 'content': 'b c'},{'name': 'd', '_id': 2, '_score': 0.9989467, 'content': 'b e'}]                    
```



## Usage (low-level Python API)

### Start Cluster

We provide the python API to build the index, you can use the following code to build the index.

```python
import ray
ray.init(address="auto",namespace="default",
                 job_config=ray.job_config.JobConfig(code_search_path=["/home/winubuntu/softwares/byzer-retrieval-lib/"],
                                                      runtime_env={"env_vars": {"JAVA_HOME": "/home/winubuntu/softwares/jdk-21",
                                                                                "PATH":"/home/winubuntu/softwares/jdk-21/bin:/home/winubuntu/.cargo/bin:/usr/local/cuda/bin:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.1.1-2.3.2/jdk8/bin:/home/winubuntu/miniconda3/envs/byzerllm-dev/bin:/home/winubuntu/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"}})
                 )
```

In the above code, we use the `ray.init` to connect an existing ray cluster, and we set the `code_search_path` 
to the path of the jar files which we already put in ray cluster, and set the `runtime_env` to the path of the JDK 21.  
Notice that `JAVA_HOME`, `PATH` are both required.

Then we can start the retrieval gateway with `RetrievalGatewayLauncher`:

```python
retrieval_gateway_launcher_clzz = ray.cross_language.java_actor_class("tech.mlsql.retrieval.RetrievalGatewayLauncher")
retrieval_gateway_launcher = retrieval_gateway_launcher_clzz.remote()
ray.get(retrieval_gateway_launcher.launch.remote())
retrieval_gateway = ray.get_actor("RetrievalGateway")
retrieval_gateway
```

with the retrieval_gateway's help, we can start our retrieval cluster like the following code:

```python
import json
from byzerllm.records import EnvSettings,ClusterSettings,TableSettings,JVMSettings

env_settings = EnvSettings(
    javaHome="/home/winubuntu/softwares/jdk-21",
    path="/home/winubuntu/softwares/jdk-21/bin:/home/winubuntu/.cargo/bin:/usr/local/cuda/bin:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.1.1-2.3.2/jdk8/bin:/home/winubuntu/miniconda3/envs/byzerllm-dev/bin:/home/winubuntu/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"
)

cluster_setting = ClusterSettings(
    name="cluster1",
    location="/tmp/cluster1",
    numNodes=1
)

table_settings = TableSettings(
    database="db1",table="table1",
    schema="st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))",
    location="/tmp/cluster1",num_shards=1,
)

jvm_settings = JVMSettings(
    options=[]
)

obj_ref1 = retrieval_gateway.buildCluster.remote(
    cluster_setting.json(),
    env_settings.json(),
    jvm_settings.json()
)
ray.get(obj_ref1)
```
In this step, we will start cluster named `cluster1` and create a table named `table1` in this cluster. The table has
only one shard, and the index data will be stored in `/tmp/cluster1/db1/table1`.

We also provide a more convenient way to describe index schema:

```
st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))
```

st: Struct/Table , field: Table field. The field has three parameters: name, type, and analyze. 
The analyze parameter is optional, and it is used to specify the field whether to be analyzed when indexing.

### Build Index

Mock some data, and put the data in the ray cluster object store:

```python
import json
import ray
data = [
    {"_id":1, "name":"a", "content":"b c", "vector":[1.0,2.0,3.0]},
    {"_id":2, "name":"d", "content":"b e", "vector":[1.0,2.6,4.0]}
]

data_refs = []

for item in data:
    itemref = ray.put(json.dumps(item,ensure_ascii=False))
    data_refs.append(itemref)

cluster.createTable.remote(tableSettings.json())
```

For now, we can insert the data into the index:

```python
import byzerllm.utils.object_store_ref_util as ref_utils

data_ids = ref_utils.get_object_ids(data_refs)
locations = ref_utils.get_locations(data_refs)
ray.get(master.buildFromRayObjectStore.remote("db1","table1",data_ids,locations))
```

### Search

Here is an example of vector search:

```python
from byzerllm.records import SearchQuery
import json
search = SearchQuery(keyword=None,fields=[],vector=[1.0,2.0,3.0],vectorField="vector",limit=10)
v = master.search.remote("db1","table1",json.dumps(search,default=lambda o: o.__dict__,ensure_ascii=False))
ray.get(v)

## the output: [{"name":"a","_id":1,"_score":1.0,"content":"b c"},{"name":"d","_id":2,"_score":0.9989467,"content":"b e"}]
```
Here is an example of BM25 search:

```python
from byzerllm.records import SearchQuery
import json
search = SearchQuery(keyword="e",fields=["content"],vector=[],vectorField=None,limit=10)
v = master.search.remote("db1","table1",json.dumps(search,default=lambda o: o.__dict__,ensure_ascii=False))
ray.get(v)

## The output: '[{"name":"d","_id":2,"_score":0.31506687,"content":"b e"}]'
```



