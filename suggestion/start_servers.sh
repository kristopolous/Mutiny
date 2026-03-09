#!/bin/bash
echo "Starting Neo4j and Redis servers..."

# Start Neo4j (Community Edition)
if command -v neo4j &> /dev/null; then
    neo4j start
    echo "Neo4j started"
else
    echo "Neo4j CLI not found. Starting via Docker..."
    docker run -d -p 7687:7687 -p 7474:7474 --name neo4j -e NEO4J_AUTH=neo4j/password neo4j:community
fi

# Start Redis
if command -v redis-server &> /dev/null; then
    redis-server --daemonize yes
    echo "Redis started"
else
    echo "redis-server not found. Starting via Docker..."
    docker run -d -p 6379:6379 --name redis redis:alpine
fi

echo "Waiting for services to be ready..."
sleep 5
echo "All services started"