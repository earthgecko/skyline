.. role:: skyblue
.. role:: red

grafana_promql_anomaly_detection
================================

A loose Python implementation of the Grafana PromQL Anomaly Detection method.

https://github.com/grafana/promql-anomaly-detection/tree/cd5a307ac7e44beb7e42299fe05cd71dd5647237

https://grafana.com/blog/2024/10/03/how-to-use-prometheus-to-efficiently-detect-anomalies-at-scale/

This implementation does not take into account the alert only if breached
x times which in a running implementation on Grafana would be provided by
the rules.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.grafana_promql_anomaly_detection

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/grafana_promql_anomaly_detection.py

