"""A set of utility functions to support outlier detection.
"""
# Author: Yinchen WU <yinchen@uchicago.edu>


from __future__ import division
from __future__ import print_function

import numpy as np
import numbers

from sklearn.preprocessing import StandardScaler
from sklearn.utils import check_array
from sklearn.utils import column_or_1d

from statsmodels.tsa.stattools import acf
from scipy.signal import argrelextrema

MAX_INT = np.iinfo(np.int32).max
MIN_INT = -1 * MAX_INT


def check_parameter(param, low=MIN_INT, high=MAX_INT, param_name='',
                    include_left=False, include_right=False):
    """Check if an input is within the defined range.
    Parameters
    ----------
    param : int, float
        The input parameter to check.
    low : int, float
        The lower bound of the range.
    high : int, float
        The higher bound of the range.
    param_name : str, optional (default='')
        The name of the parameter.
    include_left : bool, optional (default=False)
        Whether includes the lower bound (lower bound <=).
    include_right : bool, optional (default=False)
        Whether includes the higher bound (<= higher bound).
    Returns
    -------
    within_range : bool or raise errors
        Whether the parameter is within the range of (low, high)
    """

    # param, low and high should all be numerical
    if not isinstance(param, (numbers.Integral, np.integer, np.float64)):
        raise TypeError('{param_name} is set to {param} Not numerical'.format(
            param=param, param_name=param_name))

    if not isinstance(low, (numbers.Integral, np.integer, np.float64)):
        raise TypeError('low is set to {low}. Not numerical'.format(low=low))

    if not isinstance(high, (numbers.Integral, np.integer, np.float64)):
        raise TypeError('high is set to {high}. Not numerical'.format(
            high=high))

    # at least one of the bounds should be specified
    if low is MIN_INT and high is MAX_INT:
        raise ValueError('Neither low nor high bounds is undefined')

    # if wrong bound values are used
    if low > high:
        raise ValueError(
            'Lower bound > Higher bound')

    # value check under different bound conditions
    if (include_left and include_right) and (param < low or param > high):
        raise ValueError(
            '{param_name} is set to {param}. '
            'Not in the range of [{low}, {high}].'.format(
                param=param, low=low, high=high, param_name=param_name))

    elif (include_left and not include_right) and (
            param < low or param >= high):
        raise ValueError(
            '{param_name} is set to {param}. '
            'Not in the range of [{low}, {high}).'.format(
                param=param, low=low, high=high, param_name=param_name))

    elif (not include_left and include_right) and (
            param <= low or param > high):
        raise ValueError(
            '{param_name} is set to {param}. '
            'Not in the range of ({low}, {high}].'.format(
                param=param, low=low, high=high, param_name=param_name))

    elif (not include_left and not include_right) and (
            param <= low or param >= high):
        raise ValueError(
            '{param_name} is set to {param}. '
            'Not in the range of ({low}, {high}).'.format(
                param=param, low=low, high=high, param_name=param_name))
    else:
        return True


def invert_order(scores, method='multiplication'):
    """ Invert the order of a list of values. The smallest value becomes
    the largest in the inverted list. This is useful while combining
    multiple detectors since their score order could be different.
    Parameters
    ----------
    scores : list, array or numpy array with shape (n_samples,)
        The list of values to be inverted
    method : str, optional (default='multiplication')
        Methods used for order inversion. Valid methods are:
        - 'multiplication': multiply by -1
        - 'subtraction': max(scores) - scores
    Returns
    -------
    inverted_scores : numpy array of shape (n_samples,)
        The inverted list
    Examples
    --------
    >>> scores1 = [0.1, 0.3, 0.5, 0.7, 0.2, 0.1]
    >>> invert_order(scores1)
    array([-0.1, -0.3, -0.5, -0.7, -0.2, -0.1])
    >>> invert_order(scores1, method='subtraction')
    array([0.6, 0.4, 0.2, 0. , 0.5, 0.6])
    """

    scores = column_or_1d(scores)

    if method == 'multiplication':
        return scores.ravel() * -1

    if method == 'subtraction':
        return (scores.max() - scores).ravel()


def standardizer(X, X_t=None, keep_scalar=False):
    """Conduct Z-normalization on data to turn input samples become zero-mean
    and unit variance.
    Parameters
    ----------
    X : numpy array of shape (n_samples, n_features)
        The training samples
    X_t : numpy array of shape (n_samples_new, n_features), optional (default=None)
        The data to be converted
    keep_scalar : bool, optional (default=False)
        The flag to indicate whether to return the scalar
    Returns
    -------
    X_norm : numpy array of shape (n_samples, n_features)
        X after the Z-score normalization
    X_t_norm : numpy array of shape (n_samples, n_features)
        X_t after the Z-score normalization
    scalar : sklearn scalar object
        The scalar used in conversion
    """
    X = check_array(X)
    scaler = StandardScaler().fit(X)

    if X_t is None:
        if keep_scalar:
            return scaler.transform(X), scaler
        else:
            return scaler.transform(X)
    else:
        X_t = check_array(X_t)
        if X.shape[1] != X_t.shape[1]:
            raise ValueError(
                "The number of input data feature should be consistent"
                "X has {0} features and X_t has {1} features.".format(
                    X.shape[1], X_t.shape[1]))
        if keep_scalar:
            return scaler.transform(X), scaler.transform(X_t), scaler
        else:
            return scaler.transform(X), scaler.transform(X_t)


def find_length(data):
    """
    determine sliding window (period) based on autocorrelation.
        
    Parameters
    ----------
    data : numpy array of shape (n_samples, )
        The time series on which we find the optimal subsequence length.
    
    Returns
    -------
    length : int
        argmax on the autocorrelation curve. Cannot be smaller than 3 and bigger than 300.
        In case of extreme small (below 3) or big (above 300) argmax, we set a default subseuqence length and return 100. 
    """
    if len(data.shape)>1:
        return 0
    data = data[:min(20000, len(data))]
    
    base = 3
    auto_corr = acf(data, nlags=400, fft=True)[base:]
    
    
    local_max = argrelextrema(auto_corr, np.greater)[0]
    try:
        max_local_max = np.argmax([auto_corr[lcm] for lcm in local_max])
        if local_max[max_local_max]<3 or local_max[max_local_max]>300:
            return 100
        return local_max[max_local_max]+base
    except:
        return 100
    
