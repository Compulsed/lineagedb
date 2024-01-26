#!/bin/sh
~/go/bin/bombardier http://localhost:9000/graphql \
    --method=POST \
    --body-file=./payloads/create.json \
    --header="Content-Type: application/json" \
    --duration 10s