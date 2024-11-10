.. role:: skyblue
.. role:: red

one_class_svm
=============

Outlier detector for time series data using One Class SVM base on the moving
mean and variance, unless the variance is low in which case the standard
deviation will be used in place of variance.  The algorithm parameters to
be concerned with are ``'window'`` which defines the length of sliding
window to use, ``nu`` which defines the percentage that can be considered as
outliers e.g. 0.1 would be 10%.  Do note that if the variance is low each
spike or trough will probably be identified as an outlier.

See the docstrings - https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.one_class_svm

See the custom_algorithm source - https://github.com/earthgecko/skyline/blob/master/skyline/custom_algorithms/one_class_svm.py

