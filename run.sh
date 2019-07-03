#!/bin/bash

# check if kafka url is assigned
if [[ -z "${KAFKA_URL}" ]]; then
    echo "Environment variable \$KAFKA_URL is required, but has no value.";
    exit 1;
fi

# check if kafka topic is assigned
if [[ -z "${KAFKA_TOPIC}" ]]; then
    echo "Environment variable \$KAFKA_TOPIC is required, but has no value.";
    exit 1;
fi

python -u ./main.py -u $KAFKA_URL -t $KAFKA_TOPIC