#!/bin/bash

# Perform the migrations using sea
# /usr/local/bin/sea-orm-cli migrate up

if [[ -f .env ]]; then
    export $(cat .env | sed 's/#.*//g' | xargs)
fi

if [[ -z "$PINAX_API_KEY" && -z "$STREAMINGFAST_API_KEY" ]]; then
    echo "Error: Either PINAX_API_KEY or STREAMINGFAST_API_KEY must be provided"
    exit 1
elif [[ -n "$PINAX_API_KEY" ]]; then
    export SUBSTREAMS_API_TOKEN=$(curl https://auth.pinax.network/v1/auth/issue -s --data-binary '{"api_key":"'$PINAX_API_KEY'"}' | jq -r .token) 
    echo $SUBSTREAMS_API_TOKEN set on SUBSTREAMS_API_TOKEN
elif [[ -n "$STREAMINGFAST_API_KEY" ]]; then
    export SUBSTREAMS_API_TOKEN=$(curl https://auth.streamingfast.io/v1/auth/issue -s --data-binary '{"api_key":"'$STREAMINGFAST_API_KEY'"}' | jq -r .token)
    echo $SUBSTREAMS_API_TOKEN set on SUBSTREAMS_API_TOKEN
fi

# Run your main application
./geo-substream-sink "$@"