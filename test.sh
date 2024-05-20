#!/bin/bash

set -e

key=$(echo -n "message" | base64)
value=$(grpcurl \
  -plaintext \
  -import-path ./grpc/ \
  -proto espikey.proto -d "{\"key\":\"${key}\"}" \
  '[::]:50051' \
  espikey.KVService/Get | jq -r '.value' | base64 -d)

[ "$value" == "Hello, world!" ]

value=$(echo -n "Hey" | base64)
value=$(grpcurl \
  -plaintext \
  -import-path ./grpc/ \
  -proto espikey.proto -d "{\"key\":\"${key}\", \"value\":\"${value}\"}" \
  '[::]:50051' \
  espikey.KVService/Set | jq -r '.status')
[ "$value" == "1" ]

echo OK
