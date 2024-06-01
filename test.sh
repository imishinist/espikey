#!/bin/bash

set -e

port="50061"

key=$(echo -n "message" | base64)
result=$(grpcurl \
  -plaintext \
  -import-path ./grpc/ \
  -proto espikey.proto -d "{\"key\":\"${key}\"}" \
  "[::]:${port}" \
  espikey.KVService/Get | jq -r '.status')
[ "$result" = "STATUS_NOT_FOUND" ]
echo "get empty key: OK"

value=$(echo -n "Hey" | base64)
result=$(grpcurl \
  -plaintext \
  -import-path ./grpc/ \
  -proto espikey.proto -d "{\"key\":\"${key}\", \"value\":\"${value}\"}" \
  "[::]:${port}" \
  espikey.KVService/Set | jq -r '.status')
[ "$result" = "STATUS_OK" ]
echo "set 'message' 'value': OK"

key=$(echo -n "message" | base64)
result=$(grpcurl \
  -plaintext \
  -import-path ./grpc/ \
  -proto espikey.proto -d "{\"key\":\"${key}\"}" \
  "[::]:${port}" \
  espikey.KVService/Get | jq -r '.value' | base64 -d)
[ "$result" = "Hey" ]
echo "get 'message': OK"

echo OK
