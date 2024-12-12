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

Example analysis output
------------------------

The below graphs show the results of dbscan run with the default
algorithm_parameters against seasonal, seasonal unstable, stable and unstable
time series.

.. figure:: ../images/custom_algorithms/dbscan/dbscan.seasonal.A.png
    :alt: dbscan seasonal time series A
    
    *dbscan.seasonal.A - runtime: 0.381 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.seasonal.B.png
    :alt: dbscan seasonal time series B
    
    *dbscan.seasonal.B - runtime: 0.23 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.seasonal.C.png
    :alt: dbscan seasonal time series C
    
    *dbscan.seasonal.C - runtime: 0.186 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.seasonal.D.png
    :alt: dbscan seasonal time series D
    
    *dbscan.seasonal.D - runtime: 0.145 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.seasonal_unstable.A.png
    :alt: dbscan seasonal_unstable time series A
    
    *dbscan.seasonal_unstable.A - runtime: 0.214 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.seasonal_unstable.B.png
    :alt: dbscan seasonal_unstable time series B
    
    *dbscan.seasonal_unstable.B - runtime: 2.904 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.stable.A.png
    :alt: dbscan stable time series A
    
    *dbscan.stable.A - runtime: 0.271 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.stable.B.png
    :alt: dbscan stable time series B
    
    *dbscan.stable.B - runtime: 0.3 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.stable.E.png
    :alt: dbscan stable time series E
    
    *dbscan.stable.E - runtime: 0.142 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.stable.F.png
    :alt: dbscan stable time series F
    
    *dbscan.stable.F - runtime: 0.174 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.unstable.A.png
    :alt: dbscan unstable time series A
    
    *dbscan.unstable.A - runtime: 0.788 seconds*

.. figure:: ../images/custom_algorithms/dbscan/dbscan.unstable.B.png
    :alt: dbscan unstable time series B
    
    *dbscan.unstable.B - runtime: 0.99 seconds*
