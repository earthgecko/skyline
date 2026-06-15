.. role:: skyblue
.. role:: red

Local Outlier Factor
====================

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.lof

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/lof.py

Example analysis output
------------------------

The below graphs show the results of lof run with the default
algorithm_parameters against seasonal, seasonal unstable, stable and unstable
time series.

.. figure:: ../images/custom_algorithms/lof/lof.seasonal.A.png
    :alt: lof seasonal time series A
    
    *lof.seasonal.A - runtime: 0.082 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.seasonal.B.png
    :alt: lof seasonal time series B
    
    *lof.seasonal.B - runtime: 0.104 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.seasonal.C.png
    :alt: lof seasonal time series C
    
    *lof.seasonal.C - runtime: 0.017 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.seasonal.D.png
    :alt: lof seasonal time series D
    
    *lof.seasonal.D - runtime: 0.024 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.seasonal_unstable.A.png
    :alt: lof seasonal_unstable time series A
    
    *lof.seasonal_unstable.A - runtime: 0.098 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.seasonal_unstable.B.png
    :alt: lof seasonal_unstable time series B
    
    *lof.seasonal_unstable.B - runtime: 0.037 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.stable.A.png
    :alt: lof stable time series A
    
    *lof.stable.A - runtime: 0.018 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.stable.B.png
    :alt: lof stable time series B
    
    *lof.stable.B - runtime: 0.018 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.stable.E.png
    :alt: lof stable time series E
    
    *lof.stable.E - runtime: 0.075 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.stable.F.png
    :alt: lof stable time series F
    
    *lof.stable.F - runtime: 0.032 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.unstable.A.png
    :alt: lof unstable time series A
    
    *lof.unstable.A - runtime: 0.092 seconds*

.. figure:: ../images/custom_algorithms/lof/lof.unstable.B.png
    :alt: lof unstable time series B
    
    *lof.unstable.B - runtime: 0.102 seconds*