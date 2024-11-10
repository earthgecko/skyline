.. role:: skyblue
.. role:: red

LAD
===

Large Deviations Anomaly Detection

Univariate implementation of

Large Deviations Anomaly Detection (LAD) for collection of multivariate time series data: Applications to COVID-19 data (Sreelekha Guggilam, Varun Chandola, Abani K. Patra)

Journal of Computational Science 72 (2023) 102101

https://www.sciencedirect.com/science/article/pii/S1877750323001618

https://pdf.sciencedirectassets.com/280179/1-s2.0-S1877750323X00076/1-s2.0-S1877750323001618/main.pdf

https://doi.org/10.1016/j.jocs.2023.102101

At 95 percentile may be one the noisiest algorithm yet.

If threshold is set at 95, will detect step changes, etc.

If threshold is set at 99 will only detect most severe spike/dip/point anomalies.

Fast but noisy.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.lad

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/lad.py

