"""
minmax_scale.py
"""
import numpy as np


# @added 20241115 - Feature #5548: functions.numpy.minmax_scale
# A common pattern to handle MinMax scaling using numpy to replace all MinMax
# scaling definitions where appropriate.
def minmax_scale(X):
    """
    Given a numpy array, apply MinMax scaling or return array of 0s if max == min.

    :param X: The numpy array to apply Min-Max scale.
    :type X: np.array
    :return: X_minmax_scaled
    :rtype: np.array
    
    """
    # Default to an empty np.array

# TO DO PROPERLY

    X_minmax_scaled = np.array([])
    try:
        np_max = np.amax(X)
        np_min = np.amin(X)
        if np_max == np_min:
            X_minmax_scaled = np.zeros_like(X)
        else:
            X_minmax_scaled = (X - X.min()) / (X.max() - X.min())
    except:
        pass
    return X_minmax_scaled
