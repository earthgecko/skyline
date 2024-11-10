.. role:: skyblue
.. role:: red

DBSCAN
======

Outlier detector based on DBSCAN.

Fairly unreliable as it is very sensitive to input parameters which make it
difficult to automatically determine suitable parameters.  Automatically
determined parameters can sometimes be very effective, but often they do not
have the desired results.  Seeing as there is a single epsilon value for all
clusters the algorithm fails when varying density clusters are present in the
data.

Therefore if DBSCAN identifies more than 33% of the data points in a time series
as outliers, this algorithm will return an inconclusive results.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.dbscan

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/dbscan.py

