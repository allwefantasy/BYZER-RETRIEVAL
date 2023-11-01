# BYZER-RETRIEVAL

## Introduction

Byzer-retrieval is a distributed retrieval system which designed as a backend for LLM RAG (Retrieval Augmented Generation).
The system supports both BM25 retrieval algorithm and vector retrieval algorithm, you can also use both of them at the same time and 
get a fusion score for each document. 

In contrast to the traditional way, there is no need to deploy so many systems e.g. the Elasticsearch or Milvus, 
and reduce the cost of deployment and maintenance.  You can reuse the cluster which is used for training/serving the LLM model 
because Byzer-retrieval use CPU/Memory(LLM using GPU/GPU Memory) which will make full use of the resources. 

This project is implemented based on Lucene + Ray which use Lucene to build the inverted index/vector index and 
use Ray to build the distributed system. Notice that this project requires JDK 21 or higher, because the new features of JDK 21 e.g. vector API
and foreign memory will bring a great performance improvement to the system. We also introduce the virtual threads in Java
to improve the concurrency performance of cluster.

## Architecture

![](images/byzer-retrieval.png)

The above figure shows the architecture of Byzer-retrieval. The user can use Python/Rest/SQL API to launch a RetrievalGateway,
and then use the RetrievalGateway to create a retrieval cluster. Actually, you can create any number of clusters, and each cluster can contain any number of tables.
Notice that all tables in the same cluster have the same shard number. Each cluster will have a master actor and 
some worker actors, and all these actors are Java actors. The master actor is as the entry point of the cluster.

If you use python API to insert data into the table, the data will be put in the ray cluster object store, and then the 
master actor will route the data to the worker actors. The worker actors will build the index in parallel.

## Requisites

1. Ray cluster == 2.7.0
2. JDK 21 or higher
3. Python 3.10.11
4. pyjava > =0.6.13
4. byzerllm >= 0.1.12 (python package) 
5. byzer-llm >= 0.1.7 (java package for Byzer-SQL API,download address: https://download.byzer.org/byzer-extensions/nightly-build/byzer-llm-3.3_2.12-0.1.7.jar)

The java package `byzer-llm` is used for Byzer-SQL API, download it and put it in `$BYZER_HOME/plugin/` directory.

## Deploy

### Deploy Ray Cluster

Clone this project.

```
conda create -n byzer-retrieval python=3.10.11
conda activate byzer-retrieval
pip install -r requirements.txt

ray start --head  --dashboard-host 0.0.0.0
```

You can download [Byzer-Retrieval Package](https://download.byzer.org/byzer-retrieval/) and extract it to the directory 
which you want to deploy the retrieval system. or build the jar file and dependency from source code.

Here is the command to build the jar file and dependency from source code:

```
## use jdk 21 to compile the project
export JAVA_HOME=/home/winubuntu/softwares/jdk-21
export PATH=$JAVA_HOME/bin/:$PATH
mvn clean package -DskipTests
mvn dependency:copy-dependencies -DoutputDirectory=target/dependency
```

Then copy the jar file and dependency to the ray cluster. Suppose you put
all the jars in `/home/winubuntu/softwares/byzer-retrieval-lib/`.



> If you want to use Byzer-SQL API, you need to download the `byzer-llm` jar file and put it in `$BYZER_HOME/plugin/` directory.

That's all.

## Validate the Environment

Since byzer-retrieval need to configure JDK PATH, you need to setup `JAVA_HOME` and `PATH` before you 
can launch the retrieval gateway in every kind of API(Python/Byzer-SQL/Rest).

You can try to use the following code to validate the environment especially the `PATH` is correct:

```python
import os
os.environ["JAVA_HOME"]="/home/winubuntu/softwares/jdk-21"
os.environ["PATH"]="/home/winubuntu/softwares/jdk-21/bin:/home/winubuntu/.cargo/bin:/usr/local/cuda/bin:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.1.1-2.3.2/jdk8/bin:/home/winubuntu/miniconda3/envs/byzerllm-dev/bin:/home/winubuntu/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"
os.execvp("bash", args=["bash", "-c", "java -version"])
```

If this script fails, The `PATH` is not correct, and you need to check the `PATH` again.
You may miss some key paths e.g. `/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin` in the `PATH`.

## Usage (high-level Python API)

We provide the a high-level python API to build the index, you can use the following code to build the index.

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

or you can search by both keyword and vector:

```python
from byzerllm.records import SearchQuery
byzer.search("cluster1","db1","table1",
                    SearchQuery(keyword="c",fields=["content"],
                                vector=[1.0,2.0,3.0],vectorField="vector",
                                limit=10))

## output: [{'name': 'a', '_id': 1, '_score': 0.016666668, 'content': 'b c'},
## {'name': 'd', '_id': 2, '_score': 0.016393442, 'content': 'b e'}]
```


You can also do follow operations to the table:

1. truncate: delete all data in the table
2. close: close the table and release the resources
3. closeAndDeleteFile: close the table and delete the index files

```python
byzer.truncate("cluster1","db1","table1")
byzer.close("cluster1","db1","table1")
byzer.closeAndDeleteFile("cluster1","db1","table1")
```


## Cluster Recovery

When the Retrieval Cluster  is crash or the Ray Cluster is down, we need to recover our cluster. You can manually
export cluster metadata, and then save it to the storage. Try to use the following code to export the metadata:

```python
cluster = byzer.cluster("cluster1")
cluster1_meta = json.loads(ray.get(cluster.clusterInfo.remote()))
# save s to file
with open("/tmp/cluster_info.json","w") as f:
    json.dump(cluster1_meta,f,ensure_ascii=False)
```

Then you can use the following code to recover the cluster once the cluster is down:

```python
import json
import os
import ray
from byzerllm.records import EnvSettings,ClusterSettings,TableSettings,JVMSettings
from byzerllm.utils.retrieval import ByzerRetrieval

with open("/tmp/cluster_info.json","r") as f:
    s = json.load(f)

byzer = ByzerRetrieval()
byzer.launch_gateway()
byzer.restore_from_cluster_info(s)
```

Notice that if the Ray Cluster is down, you need to connect it Ray cluster first, and then restore the retrieval cluster.

```python
import json
import os
import ray
from byzerllm.records import EnvSettings,ClusterSettings,TableSettings,JVMSettings
from byzerllm.utils.retrieval import ByzerRetrieval

with open("/tmp/cluster_info.json","r") as f:
    s = json.load(f)

code_search_path=["/home/winubuntu/softwares/byzer-retrieval-lib/"]
env_vars = {"JAVA_HOME": s["envSettings"]["javaHome"],
            "PATH":s["envSettings"]["path"]}

ray.init(address="auto",namespace="default",
                 job_config=ray.job_config.JobConfig(code_search_path=code_search_path,
                                                      runtime_env={"env_vars": env_vars})
                 )

byzer = ByzerRetrieval()
byzer.launch_gateway()
byzer.restore_from_cluster_info(s)

```

## Rest API

We also provide a rest api to access the retrieval clusters. You can use the following code to start the rest api:

```python

import ray
from byzerllm.utils.retrieval.rest import deploy_retrieval_rest_server

code_search_path=["/home/winubuntu/softwares/byzer-retrieval-lib/"]
env_vars = {"JAVA_HOME": "/home/winubuntu/softwares/jdk-21",
            "PATH":"/home/winubuntu/softwares/jdk-21/bin:/home/winubuntu/.cargo/bin:/usr/local/cuda/bin:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.1.1-2.3.2/jdk8/bin:/home/winubuntu/miniconda3/envs/byzerllm-dev/bin:/home/winubuntu/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"}

ray.init(address="auto",namespace="default",
                 job_config=ray.job_config.JobConfig(code_search_path=code_search_path,
                                                      runtime_env={"env_vars": env_vars}),ignore_reinit_error=True
                 )

deploy_retrieval_rest_server(host="0.0.0.0",route_prefix="/retrieval")
```

Then you can use endpoint `http://127.0.0.1:8000/retrievel` to access the retrieval cluster.
                              
create a retrieval cluster:

```python
import requests
import json
from byzerllm.utils.retrieval.rest import (ClusterSettingsParam,
                                           EnvSettingsParam,
                                           JVMSettingsParam,
                                           ResourceRequirementParam,
                                           ResourceRequirementSettingsParam)

r = requests.post("http://127.0.0.1:8000/retrieval/cluster/create",json={
    "cluster_settings":ClusterSettingsParam(
            name="cluster1",
            location="/tmp/cluster1",
            numNodes=1
        ).dict(), 
    "env_settings":EnvSettingsParam(
            javaHome=env_vars["JAVA_HOME"],
            path=env_vars["PATH"]
        ).dict(), 
    "jvm_settings":JVMSettingsParam(
            options=[]
        ).dict(), 
    "resource_requirement_settings": ResourceRequirementSettingsParam(
        resourceRequirements=[ResourceRequirementParam(name="CPU",resourceQuantity=1.0)]).dict()
})
r.text
```

or recover the cluster is also supported in the rest api, you can use the following code to recover the cluster:

```python
import requests
import json

with open("/tmp/cluster_info.json","r") as f:
    s = f.read()

r = requests.post("http://127.0.0.1:8000/retrieval/cluster/restore",params={
    "cluster_info":s
})
json.loads(r.text)
```

get cluster info:

```python
import requests
import json

r = requests.get("http://127.0.0.1:8000/retrieval/cluster/get/cluster1")
json.loads(r.text)
```

create a table:

```python
import requests
import json
from byzerllm.utils.retrieval.rest import TableSettingsParam

r = requests.post("http://127.0.0.1:8000/retrieval/table/create/cluster1",json=TableSettingsParam(
    database="db1",table="table1",
    schema="st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))",
    location="/tmp/cluster1",num_shards=1,
).dict())

r.text
```

insert data:

```python
import requests
import json
from byzerllm.utils.retrieval.rest import TableSettingsParam

data = [
    {"_id":3, "name":"a", "content":"b c", "vector":[1.0,2.0,3.0]},
    {"_id":4, "name":"d", "content":"b e", "vector":[1.0,2.6,4.0]}
]



r = requests.post("http://127.0.0.1:8000/retrieval/table/data",json={
    "cluster_name":"cluster1",
    "database":"db1",
    "table":"table1",
    "data":data
})

r.text
```

make the index persistent:

```python
import requests
import json
from byzerllm.utils.retrieval.rest import TableSettingsParam


r = requests.post("http://127.0.0.1:8000/retrieval/table/commit",json={
    "cluster_name":"cluster1",
    "database":"db1",
    "table":"table1"    
})

r.text
```

search:

```python
import requests
import json
from byzerllm.utils.retrieval.rest import SearchQueryParam


r = requests.post("http://127.0.0.1:8000/retrieval/table/search",json={
    "cluster_name":"cluster1", 
    "database":"db1", 
    "table":"table1", 
    "query":SearchQueryParam(keyword="c",fields=["content"],
                                vector=[1.0,2.0,3.0],vectorField="vector",
                                limit=10).dict()
})
json.loads(r.text)
```

More details please refer to `http://127.0.0.1:8000/retrieval/docs`

## Byzer-SQL API

We also integrate the retrieval system into [Byzer-SQL](https://github.com/byzer-org/byzer-lang), 
the first step is setup environment:

```python
!byzerllm setup retrieval;
!byzerllm setup "code_search_path=/home/winubuntu/softwares/byzer-retrieval-lib/";
!byzerllm setup "JAVA_HOME=/home/winubuntu/softwares/jdk-21";
!byzerllm setup "PATH=/home/winubuntu/softwares/jdk-21/bin:/home/winubuntu/.cargo/bin:/usr/local/cuda/bin:/home/winubuntu/softwares/byzer-lang-all-in-one-linux-amd64-3.1.1-2.3.2/jdk8/bin:/home/winubuntu/miniconda3/envs/byzerllm-dev/bin:/home/winubuntu/miniconda3/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin";
```

Then we can create a retrieval cluster:

```sql
run command as Retrieval.``
where action="cluster/create"
and `cluster_settings.name`="cluster1"
and `cluster_settings.location`="/tmp/cluster1"
and `cluster_settings.numNodes`="1";
```

You can create a table in the cluster:

```sql
run command as Retrieval.``
where action="table/create/cluster1"
and `table_settings.database`="db1"
and `table_settings.table`="table2"
and `table_settings.location`="/tmp/cluster1"
and `table_settings.schema`="st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))";
```

After that, we can insert some data into the table:

```sql
!byzerllm setup retrieval/data;

set jsondata = '''
{"_id":3, "name":"a", "content":"b c", "vector": [1.0,2.0,3.0] }
{"_id":4, "name":"d", "content":"b e", "vector": [1.0,2.6,4.0] }
''';

load jsonStr.`jsondata` as newdata;

run command as Retrieval.`` 
where action="table/data"
and clusterName="cluster1"
and database="db1"
and table="table2"
and inputTable="newdata";
```

Try to register the retrieval as a UDF:

```sql
run command as Retrieval.`` where 
action="register"
and udfName="search";
```

You can change the udfName to any name you want, and then you can use the udf to search the data:

```sql
select search(
array(to_json(map(
 "clusterName","cluster1",
 "database","db1",
 "table","table2",
 "query.keyword","c",
 "query.fields","content",
 "query.vector","1.0,2.0,3.0",
 "query.vectorField","vector",
 "query.limit","10"
)))
)
 as c as output;
 
-- output: [ "[{\"name\": \"a\", \"_id\": 3, \"_score\": 0.016666668, \"content\": \"b c\"}, {\"name\": \"d\", \"_id\": 4, \"_score\": 0.016393442, \"content\": \"b e\"}]" ]
```

try to modify the map key-value as you want, and you can get the search result.


## Table Schema Description

We introduce a new schema language to describe the table schema.

```
st(
 field(_id,long),
 field(name,string),
 field(content,string,analyze),
 field(vector,array(float))
)
```
st means Struct, field means Field,the first value in field is columnName,and the second is type,
the third is analyze, and it is optional. If you want to analyze the field when indexing, you can set the third value as
`analyze`.

For now, simple schema supports type like following:

1. st
2. field
3. string
4. float
5. double
6. integer
7. short
8. date
9. binary
10. map
11. array
12. long
13. boolean
14. byte
15. decimal

st also supports nesting:

```sql
st(field(column1,map(string,array(st(field(columnx,string))))))
```


## Todo List

1. [+] Python API
2. [+] Byzer-SQL API
3. [+] Rest API (based on Ray serve)
4. [+] Cluster Recovery(The scenario is that the cluster is down, we need to restore the cluster from the storage.)
5. [+] Resource Management (e.g. CPU, Memory, GPU)
6. [-] Provide point query/delete API (get/delete the doc by _id)
7. [-] Support analyzer configuration when create table

## Versions

- 0.1.1: support multi-requests at the same time
- 0.1.0: first version
 




