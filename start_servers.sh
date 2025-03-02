#!/bin/bash

# Start Arrow Flight Server
java -cp target/byzer-retrieval.jar tech.mlsql.retrieval.RetrievalFlightServer &

# Start Py4J Gateway Server
java -cp target/byzer-retrieval.jar:lib/py4j0.10.9.7.jar tech.mlsql.retrieval.RetrievalPy4JGateway &

echo "Servers started"