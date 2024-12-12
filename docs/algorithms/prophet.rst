.. role:: skyblue
.. role:: red

prophet
=======

Facebook prophet.  Slow not suitable for Analyzer or Mirage takes around 30
seconds to analysis a data set of 1008 data points.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.specskyline_prophet

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/skyline_prophet.py

Example analysis output
------------------------

The below graphs show the results of prophet run with the default
algorithm_parameters against seasonal, seasonal unstable, stable and unstable
time series.

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.seasonal.A.png
    :alt: prophet seasonal time series A
    
    *prophet.seasonal.A - runtime: 1.89 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.seasonal.B.png
    :alt: prophet seasonal time series B
    
    *prophet.seasonal.B - runtime: 0.598 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.seasonal.C.png
    :alt: prophet seasonal time series C
    
    *prophet.seasonal.C - runtime: 0.569 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.seasonal.D.png
    :alt: prophet seasonal time series D
    
    *prophet.seasonal.D - runtime: 0.627 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.seasonal_unstable.A.png
    :alt: prophet seasonal_unstable time series A
    
    *prophet.seasonal_unstable.A - runtime: 5.198 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.seasonal_unstable.B.png
    :alt: prophet seasonal_unstable time series B
    
    *prophet.seasonal_unstable.B - runtime: 2.1 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.stable.A.png
    :alt: prophet stable time series A
    
    *prophet.stable.A - runtime: 0.551 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.stable.B.png
    :alt: prophet stable time series B
    
    *prophet.stable.B - runtime: 1.129 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.stable.E.png
    :alt: prophet stable time series E
    
    *prophet.stable.E - runtime: 0.693 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.stable.F.png
    :alt: prophet stable time series F
    
    *prophet.stable.F - runtime: 0.518 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.unstable.A.png
    :alt: prophet unstable time series A
    
    *prophet.unstable.A - runtime: 5.917 seconds*

.. figure:: ../images/custom_algorithms/prophet/skyline_prophet.unstable.B.png
    :alt: prophet unstable time series B
    
    *prophet.unstable.B - runtime: 2.709 seconds*