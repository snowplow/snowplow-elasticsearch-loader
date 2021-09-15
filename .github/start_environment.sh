#!/bin/sh

set -e

if [ -z ${GITHUB_WORKSPACE+x} ]; then
    echo "GITHUB_WORKSPACE is unset";
    exit 1
fi

docker run \
    --name elastic-container \
    -p 28875:28875 \
    --env "discovery.type=single-node" \
    --env "bootstrap.memory_lock=true" \
    -v $GITHUB_WORKSPACE/.github/elasticsearch/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml \
    -v $GITHUB_WORKSPACE/.github/elasticsearch/log4j2.properties:/usr/share/elasticsearch/config/log4j2.properties \
    --rm -d \
    docker.elastic.co/elasticsearch/elasticsearch:7.14.1

echo "Waiting for Elasticsearch..."
sleep 30
