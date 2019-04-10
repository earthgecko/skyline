#!/bin/bash
echo "Cleanup for redis"
REDIS_PASSWORD="XXXXXXXXX"

METRICS=( $(redis-cli -a $REDIS_PASSWORD --scan --pattern metrics*) )
for METRIC in "${METRICS[@]}";
do
    "Deleting metric $METRIC from redis"
    redis-cli -a $REDIS_PASSWORD del $METRIC
done
echo "Loading data"
python2.7 load_data.py
