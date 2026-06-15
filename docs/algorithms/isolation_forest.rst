.. role:: skyblue
.. role:: red

isolation_forest
================

Outlier detector based on Isolation Forest.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.isolation_forest

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/isolation_forest.py

Example analysis output
------------------------

The below graphs show the results of isolation_forest run with the default
algorithm_parameters against seasonal, seasonal unstable, stable and unstable
time series.

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.seasonal.A.png
    :alt: isolation_forest seasonal time series A
    
    *isolation_forest.seasonal.A - runtime: 0.335 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.seasonal.B.png
    :alt: isolation_forest seasonal time series B
    
    *isolation_forest.seasonal.B - runtime: 0.385 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.seasonal.C.png
    :alt: isolation_forest seasonal time series C
    
    *isolation_forest.seasonal.C - runtime: 0.357 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.seasonal.D.png
    :alt: isolation_forest seasonal time series D
    
    *isolation_forest.seasonal.D - runtime: 0.403 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.seasonal_unstable.A.png
    :alt: isolation_forest seasonal_unstable time series A
    
    *isolation_forest.seasonal_unstable.A - runtime: 0.399 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.seasonal_unstable.B.png
    :alt: isolation_forest seasonal_unstable time series B
    
    *isolation_forest.seasonal_unstable.B - runtime: 0.368 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.stable.A.png
    :alt: isolation_forest stable time series A
    
    *isolation_forest.stable.A - runtime: 0.356 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.stable.B.png
    :alt: isolation_forest stable time series B
    
    *isolation_forest.stable.B - runtime: 0.434 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.stable.E.png
    :alt: isolation_forest stable time series E
    
    *isolation_forest.stable.E - runtime: 0.571 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.stable.F.png
    :alt: isolation_forest stable time series F
    
    *isolation_forest.stable.F - runtime: 0.528 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.unstable.A.png
    :alt: isolation_forest unstable time series A
    
    *isolation_forest.unstable.A - runtime: 0.878 seconds*

.. figure:: ../images/custom_algorithms/isolation_forest/isolation_forest.unstable.B.png
    :alt: isolation_forest unstable time series B
    
    *isolation_forest.unstable.B - runtime: 0.424 seconds*
