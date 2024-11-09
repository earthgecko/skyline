"""
deduplicate_timeseries.py
"""

# @added 20241107 - Feature #5536: deduplicate_timeseries
def deduplicate_timeseries(timeseries, skipna=False):
    """
    Deduplicate time series items that are the same using a set to track unique
    (timestamp, value) pairs and deduplicate items that are the same.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list [(1649235260, 123),...,(1649321660, 78)]
    :type current_skyline_app: str
    :type timeseries: list
    :return: deduplicated_timeseries
    :rtype: list

    """
    deduplicated_timeseries = list(timeseries)
    try:
        seen = set()
        deduplicated_timeseries = []
        for ts, v in timeseries:
            if skipna and str(v) in ['nan', 'NaN', 'NAN', 'None', 'np.nan', 'none', 'null']:
                continue
            if (ts, v) not in seen:
                seen.add((ts, v))
                deduplicated_timeseries.append([ts, v])
    except Exception as err:
        deduplicated_timeseries = list(timeseries)
    return deduplicated_timeseries


