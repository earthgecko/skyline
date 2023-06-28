"""
normalized_variance.py
"""
import numpy as np

# @added 20230418 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
def normalized_variance(timeseries):
    """
    Determine the variance of a normalized timeseries.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list [(1649235260, 123),...,(1649321660, 78)]
    :type current_skyline_app: str
    :type timeseries: list
    :return: normalized_var
    :rtype: float
    """
    normalized_var = np.nan
    try:
        np_values = np.array([item[1] for item in timeseries])
        np_max = np.amax(np_values)
        np_min = np.amin(np_values)
        norm_np_values = (np_values - np_min) / (np_max - np_min)
        normalized_var = np.var(norm_np_values)
        if not np.isnan(normalized_var):
            normalized_var = round(normalized_var, 4)
    except Exception as err:
        normalized_var = {'error': err}
    return normalized_var


