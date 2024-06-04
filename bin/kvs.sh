#!/bin/bash

port=50061
mode=$1
if [ "$mode" == "" ]; then
    echo "Usage: ./kvs.sh [set|get|delete]"
    exit
fi


cd $(dirname "$0")/../

case "$mode" in
    "set")
        key=$2
        value=$3
        if [ "$key" == "" ] || [ "$value" == "" ]; then
            echo "Usage: ./kvs.sh set [key] [value]"
            exit
        fi
        key=$(echo -n $key | base64)
        value=$(echo -n $value | base64)
        grpcurl -plaintext -import-path ./grpc -proto espikey.proto \
          -d '{"key": "'$key'", "value": "'$value'"}' \
          "[::]:${port}" espikey.KVService/Set
        ;;
    "get")
        key=$2
        if [ "$key" == "" ]; then
            echo "Usage: ./kvs.sh get [key]"
            exit
        fi
        key=$(echo -n $key | base64)
        result=$(grpcurl -plaintext -import-path ./grpc -proto espikey.proto \
          -d '{"key": "'$key'"}' \
          "[::]:${port}" espikey.KVService/Get)
        echo -n "status: "
        echo $result | jq -r '.status'
        echo $result | jq -r '.value' | base64 -d
        echo
        ;;
    *)
        echo "Usage: ./kvs.sh [set|get|delete]"
        ;;
esac

