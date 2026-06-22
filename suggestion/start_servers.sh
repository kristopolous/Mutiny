#!/bin/bash
echo "Starting Neo4j and Redis servers..."

start_docker_container() {
    local name="$1"
    local port_maps="$2"
    local image="$3"
    shift 3
    local env_vars=("$@")

    if docker ps -a --format '{{.Names}}' | grep -q "^${name}$"; then
        if docker ps --format '{{.Names}}' | grep -q "^${name}$"; then
            echo "$name already running"
        else
            echo "Starting existing $name container..."
            docker start "$name" > /dev/null
        fi
    else
        echo "Starting $name via Docker..."
        docker run -d $port_maps --name "$name" "${env_vars[@]}" "$image"
    fi
}

# Start Neo4j (Community Edition)
if command -v neo4j &> /dev/null; then
    neo4j start
    echo "Neo4j started"
else
    start_docker_container "neo4j" "-p 7687:7687 -p 7474:7474" "neo4j:community" "-e" "NEO4J_AUTH=neo4j/password"
fi

# Start Redis
if command -v redis-server &> /dev/null; then
    redis-server --daemonize yes
    echo "Redis started"
else
    start_docker_container "redis" "-p 6379:6379" "redis:alpine"
fi

echo "Waiting for services to be ready..."
sleep 5
echo "All services started"