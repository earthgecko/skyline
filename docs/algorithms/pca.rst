.. role:: skyblue
.. role:: red

PCA
===

An implementation of PCA based on using the mean and variance features of the
time series based on `@andrewm4894 <https://andrewm4894.com/2021/10/11/time-series-anomaly-detection-using-pca/>`_
`interpretation <https://github.com/andrewm4894/colabs/blob/master/time_series_anomaly_detection_with_pca.ipynb>`_.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.pca

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/pca.py

Example analysis output
------------------------

The below graphs show the results of pca run with the default
algorithm_parameters against seasonal, seasonal unstable, stable and unstable
time series.

.. figure:: ../images/custom_algorithms/pca/pca.seasonal.A.png
    :alt: pca seasonal time series A
    
    *pca.seasonal.A - runtime: 0.119 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.seasonal.B.png
    :alt: pca seasonal time series B
    
    *pca.seasonal.B - runtime: 0.295 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.seasonal.C.png
    :alt: pca seasonal time series C
    
    *pca.seasonal.C - runtime: 0.112 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.seasonal.D.png
    :alt: pca seasonal time series D
    
    *pca.seasonal.D - runtime: 0.064 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.seasonal_unstable.A.png
    :alt: pca seasonal_unstable time series A
    
    *pca.seasonal_unstable.A - runtime: 0.267 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.seasonal_unstable.B.png
    :alt: pca seasonal_unstable time series B
    
    *pca.seasonal_unstable.B - runtime: 0.298 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.stable.A.png
    :alt: pca stable time series A
    
    *pca.stable.A - runtime: 0.112 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.stable.B.png
    :alt: pca stable time series B
    
    *pca.stable.B - runtime: 0.384 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.stable.E.png
    :alt: pca stable time series E
    
    *pca.stable.E - runtime: 0.117 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.stable.F.png
    :alt: pca stable time series F
    
    *pca.stable.F - runtime: 0.068 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.unstable.A.png
    :alt: pca unstable time series A
    
    *pca.unstable.A - runtime: 0.919 seconds*

.. figure:: ../images/custom_algorithms/pca/pca.unstable.B.png
    :alt: pca unstable time series B
    
    *pca.unstable.B - runtime: 0.303 seconds*