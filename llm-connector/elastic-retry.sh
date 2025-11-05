#!/bin/bash

retry_command() {
    local max_attempts=8
    local timeout=5
    local attempt=1
    local exit_code=0

    while [ $attempt -le $max_attempts ]
    do
        "$@"
        exit_code=$?

        if [ $exit_code -eq 0 ]; then
            break
        fi

        echo "Attempt $attempt failed! Retrying in $timeout seconds..."
        sleep $timeout
        attempt=$(( attempt + 1 ))
        timeout=$(( timeout * 2 ))
    done

    if [ $exit_code -ne 0 ]; then
        echo "Command $@ failed after $attempt attempts!"
    fi

    return $exit_code
}
export -f retry_command

retry_command_lin() {
    local max_attempts=256
    local timeout=2
    local attempt=1
    local exit_code=0

    while [ $attempt -le $max_attempts ]
    do
        "$@"
        exit_code=$?

        if [ $exit_code -eq 0 ]; then
            break
        fi

        echo "Attempt $attempt failed! Retrying in $timeout seconds..."
        sleep $timeout
        attempt=$(( attempt + 1 ))
    done

    if [ $exit_code -ne 0 ]; then
        echo "Command $@ failed after $attempt attempts!"
    fi

    return $exit_code
}
export -f retry_command_lin

check_es_health() {
    ENV_FILE_PARENT_DIR=/home/kubernetes-vm
    ENV_FILE=$ENV_FILE_PARENT_DIR/env
    export $(cat $ENV_FILE | xargs)

    output=$(curl -s -X POST "$ELASTICSEARCH_URL_LOCAL/test/_doc" \
    -H 'Content-Type: application/json' \
    --header "Authorization: Basic $ELASTICSEARCH_AUTH_BASE64" -d'
    {
      "message": "Hello World"
    }')
    echo $output
    RESULT=$(echo $output | jq -r '.result')
    if [[ $RESULT = created ]]; then
        echo "check_es_health: doc created"
    else
        echo "Waiting for Elasticsearch: $output"
        return 1
    fi

    output=$(curl -s -X GET "$ELASTICSEARCH_URL_LOCAL/test/_search" \
    -H 'Content-Type: application/json' \
    --header "Authorization: Basic $ELASTICSEARCH_AUTH_BASE64")
    RESULT=$(echo $output | jq -r '._shards.successful')
    if [[ $RESULT = 1 ]]; then
        echo "check_es_health: doc searched"
    else
        echo "Waiting for Elasticsearch: $output"
        return 1
    fi

    output=$(curl -s -X DELETE "$ELASTICSEARCH_URL_LOCAL/test" \
    -H 'Content-Type: application/json' \
    --header "Authorization: Basic $ELASTICSEARCH_AUTH_BASE64")
    RESULT=$(echo $output | jq -r '.acknowledged')
    if [[ $RESULT = true ]]; then
        echo "check_es_health: index deleted"
    else
        echo "Waiting for Elasticsearch: $output"
        return 1
    fi
}
export -f check_es_health
