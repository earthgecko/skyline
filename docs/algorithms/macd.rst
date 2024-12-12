.. role:: skyblue
.. role:: red

macd
====

Outlier detection for time series data using Moving Average
Convergence/Divergence

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.macd

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/macd.py

Example analysis output
------------------------

The below graphs show the results of macd run with the default
algorithm_parameters against seasonal, seasonal unstable, stable and unstable
time series.

.. note:: This is a changepoint detection algorithm only.

.. figure:: ../images/custom_algorithms/macd/macd.seasonal.A.png
    :alt: macd seasonal time series A
    
    *macd.seasonal.A - runtime: 0.009 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.seasonal.B.png
    :alt: macd seasonal time series B
    
    *macd.seasonal.B - runtime: 0.013 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.seasonal.C.png
    :alt: macd seasonal time series C
    
    *macd.seasonal.C - runtime: 0.01 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.seasonal.D.png
    :alt: macd seasonal time series D
    
    *macd.seasonal.D - runtime: 0.206 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.seasonal_unstable.A.png
    :alt: macd seasonal_unstable time series A
    
    *macd.seasonal_unstable.A - runtime: 0.105 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.seasonal_unstable.B.png
    :alt: macd seasonal_unstable time series B
    
    *macd.seasonal_unstable.B - runtime: 0.293 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.stable.A.png
    :alt: macd stable time series A
    
    *macd.stable.A - runtime: 0.012 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.stable.B.png
    :alt: macd stable time series B
    
    *macd.stable.B - runtime: 0.087 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.stable.E.png
    :alt: macd stable time series E
    
    *macd.stable.E - runtime: 0.077 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.stable.F.png
    :alt: macd stable time series F
    
    *macd.stable.F - runtime: 0.181 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.unstable.A.png
    :alt: macd unstable time series A
    
    *macd.unstable.A - runtime: 0.103 seconds*

.. figure:: ../images/custom_algorithms/macd/macd.unstable.B.png
    :alt: macd unstable time series B
    
    *macd.unstable.B - runtime: 0.185 seconds*