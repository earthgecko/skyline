.. role:: skyblue
.. role:: red

MSTL
====

A basic implementation of statsforecast MSTL.

https://github.com/Nixtla/statsforecast

https://nixtlaverse.nixtla.io/statsforecast/docs/models/multipleseasonaltrend.html

This algorithm has a vary variable run time depending on the data.  Not really
suitable for use as real time custom_algorithm to be used with Mirage due to the
vary variable run time.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.mstl

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/mstl.py

Example analysis output
------------------------

The below graphs show the results of mstl run with the default
algorithm_parameters against seasonal, seasonal unstable, stable and unstable
time series.

.. figure:: ../images/custom_algorithms/mstl/mstl.seasonal.A.png
    :alt: mstl seasonal time series A
    
    *mstl.seasonal.A - runtime: 3.276 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.seasonal.B.png
    :alt: mstl seasonal time series B
    
    *mstl.seasonal.B - runtime: 4.403 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.seasonal.C.png
    :alt: mstl seasonal time series C
    
    *mstl.seasonal.C - runtime: 3.036 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.seasonal.D.png
    :alt: mstl seasonal time series D
    
    *mstl.seasonal.D - runtime: 8.104 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.seasonal_unstable.A.png
    :alt: mstl seasonal_unstable time series A
    
    *mstl.seasonal_unstable.A - runtime: 4.364 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.seasonal_unstable.B.png
    :alt: mstl seasonal_unstable time series B
    
    *mstl.seasonal_unstable.B - runtime: 6.889 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.stable.A.png
    :alt: mstl stable time series A
    
    *mstl.stable.A - runtime: 10.858 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.stable.B.png
    :alt: mstl stable time series B
    
    *mstl.stable.B - runtime: 15.602 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.stable.E.png
    :alt: mstl stable time series E
    
    *mstl.stable.E - runtime: 22.005 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.stable.F.png
    :alt: mstl stable time series F
    
    *mstl.stable.F - runtime: 14.815 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.unstable.A.png
    :alt: mstl unstable time series A
    
    *mstl.unstable.A - runtime: 3.563 seconds*

.. figure:: ../images/custom_algorithms/mstl/mstl.unstable.B.png
    :alt: mstl unstable time series B
    
    *mstl.unstable.B - runtime: 20.613 seconds*