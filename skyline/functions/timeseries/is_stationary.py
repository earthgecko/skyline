"""
is_stationary.py
"""
import warnings
from statsmodels.tsa.stattools import kpss

warnings.filterwarnings('ignore')


def kpss_test(timeseries_values):
    """
    If the p-value is < significance level, then the series is non-stationary
    """
    s = False
    # Test The data is stationary around a constant
    kpsstest = kpss(timeseries_values, regression='c', nlags='auto')
    if kpsstest[1] >= 0.05:
        s = True
    if not s:
        # The data is stationary around a trend.
        kpsstest = kpss(timeseries_values, regression='ct', nlags='auto')
        if kpsstest[1] >= 0.05:
            s = True
    return s


# @added 20220521 - Ideas #4480: Stationarity
def is_stationary(timeseries):
    """
    This function is used to determine whether timeseries is stationary using
    the Kwiatkowski-Phillips-Schmidt-Shin test (KPSS) because it is faster
    than the Augmented Dickey-Fuller test (ADF)
    """
    stationary = False
    values = [value for ts, value in timeseries if str(value) not in ['nan', 'NaN', 'None']]
    if len(set(values)) == 1:
        return stationary
    try:
        stationary = kpss_test(values)
    except:
        pass
    return stationary
