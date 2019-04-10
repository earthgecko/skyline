#!/bin/bash
echo "Cleanup for redis"
redis-cli -a 9N4J3axNpffgYKr8tBqm$ del metrics.system.loadavg_1min
redis-cli -a 9N4J3axNpffgYKr8tBqm$ del metrics.system.loadavg_5min
redis-cli -a 9N4J3axNpffgYKr8tBqm$ del metrics.system.loadavg_15min
echo "Loading data"
python2.7 load_data.py
