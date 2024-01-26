#!/bin/sh
ab -p payloads/create.json \
    -T application/json \
    -c 1 \
    -n 100000 \
    http://localhost:9000/graphql
