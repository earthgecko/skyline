"""
strictly_increasing_monotonicity.py
"""
import numpy as np


# @added 20220419 - Feature #4528: metrics_manager - derivative_metric_check
#                   Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
#                   Feature #2034: analyse_derivatives
# Added strictly_increasing_monotonicity as a shared function available to
# Mirage and not just Analyzer
def strictly_increasing_monotonicity(timeseries):
    """
    This function is used to determine whether timeseries is strictly increasing
    monotonically, it will only return True if the values are strictly
    increasing, an incrementing count.  It also handles resets, often a counter
    will reset to zero (or close to zero if aggregated data) if a service
    restarts so the function splits to timeseries into increasing subsequences
    and checks each subsequnce for monotonicity.
    """

    is_strictly_increasing_monotonically = False

    # @added 20200529 - Feature #3480: batch_processing
    #                   Bug #2050: analyse_derivatives - change in monotonicity
    # Only apply to time series that have sufficient data to make this
    # determination
    if len(timeseries) < 90:
        return is_strictly_increasing_monotonically

    values = []
    for item in timeseries:
        # This only identifies and handles positive, strictly increasing
        # monotonic timeseries
        if item[1] < 0.0:
            return is_strictly_increasing_monotonically
        # Skip nans and None
        if not isinstance(item[1], float):
            continue
        values.append(item[1])
    values_array = np.asarray(values)
    diffs = list(np.diff(values_array))
    total_decreases = len([x for x in diffs if x < 0])
    total_increases = len([x for x in diffs if x > 0])

    # These are fairly arbitary values to handle metrics that do not change very
    # much.
    # These could be a derivative metric but they vary so little they do not
    # require being analysed as a derivative
    if total_decreases == total_increases:
        return is_strictly_increasing_monotonically
    if total_decreases > total_increases:
        return is_strictly_increasing_monotonically
    if total_decreases < total_increases:
        if ((total_decreases / total_increases) * 100) > 25:
            return is_strictly_increasing_monotonically

    if total_decreases == 0 and total_increases >= 10:
        is_strictly_increasing_monotonically = True
        return is_strictly_increasing_monotonically

    # Exclude timeseries that are all the same value, these are not increasing
    if len(set(values)) == 1:
        return is_strictly_increasing_monotonically

    # Exclude timeseries that sum to 0, these are not increasing
    ts_sum = sum(values[1:])
    if ts_sum == 0:
        return is_strictly_increasing_monotonically

    # Break the timeseries up into subsequences of increasing values to handle
    # resets of counters
    subsequences = []
    subsequence = []
    drops = {}
    last_value = None
    for ts, value in timeseries:
        # Skip nan and None
        if not isinstance(value, float):
            continue
        if not last_value:
            # Just use the first value as the last_value
            last_value = value
        if value < last_value:
            if subsequence:
                subsequences.append(subsequence)
            # Start a new subsequence
            subsequence = [[ts, value]]
            drops[int(ts)] = {'from': last_value, 'to': value, 'diff': (value - last_value)}
        else:
            subsequence.append([ts, value])
        last_value = value
    if subsequence:
        subsequences.append(subsequence)
    number_of_subsequences = len(subsequences)

    # That is 10 resets a day at 28 days
    if number_of_subsequences > 280:
        return is_strictly_increasing_monotonically

    # Best effort to determine if the drops represent a counter reset
    max_value = max(values)
    min_value = min(values)
    full_range = max_value - min_value
    percent_of_range = 1
    if full_range > 1000:
        percent_of_range = 2
    if full_range > 10000:
        percent_of_range = 3
    min_reset_value = (full_range / 100) * percent_of_range
    counter_resets = []
    for ts in list(drops.keys()):
        value = drops[ts]['to']
        if value <= min_reset_value:
            counter_resets.append(True)
        else:
            counter_resets.append(False)
    if counter_resets.count(True) == len(counter_resets):
        is_strictly_increasing_monotonically = True
        return is_strictly_increasing_monotonically

    # Check the monotonicity of each subsequence, which is alwasy going to be
    # True given the subsequence function above...
    ensemble = []
    for subsequence in subsequences:
        values = [value for ts, value in subsequence]
        # If the subsequence is static set it to True
        if len(set(values)) == 1:
            ensemble.append(True)
            continue
        # If the subsequence is 0 set it to True
        ts_sum = sum(values[1:])
        if ts_sum == 0:
            ensemble.append(True)
            continue
        diff_ts = np.asarray(values)
        ensemble.append(np.all(np.diff(diff_ts) >= 0))
    if ensemble.count(True) == number_of_subsequences:
        is_strictly_increasing_monotonically = True

    return is_strictly_increasing_monotonically
