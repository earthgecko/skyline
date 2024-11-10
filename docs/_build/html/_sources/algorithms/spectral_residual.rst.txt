.. role:: skyblue
.. role:: red

spectral_residual
=================

Outlier detector for time-series data using the spectral residual algorithm.
Based on the alibi-detect implementation of

Time-Series Anomaly Detection Service at Microsoft (Ren et al., 2019) https://arxiv.org/abs/1906.03821

For Mirage this algorithm is FAST

For Analyzer this algorithm is SLOW

Although this algorithm is fast, it is not fast enough to be run in Analyzer,
even if only deployed against a subset of metrics.  In testing spectral_residual
took between 0.134828 and 0.698201 seconds to run per metrics, which is much too
long for Analyzer.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.spectral_residual

See the custom_algorithms source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/spectral_residual.py

See the custom_algorithm_sources - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithm_sources/spectral_residual/spectral_residual.py
