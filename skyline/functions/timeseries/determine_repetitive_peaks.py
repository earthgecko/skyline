"""
determine_repetitive_peaks.py
"""
import numpy as np
from scipy.signal import find_peaks


# @added 20241208 - Feature #5567: determine_repetitive_peaks
def determine_repetitive_peaks(
        timeseries, sigma=2, min_peak_sigma=3, min_period_steps=3,
        min_repetitive_peaks_for_period=5):
    """
    Determine the repetitive peaks in a time series and return a dict of periods
    with a list of peak indices per period.  The period is not returned in
    seconds but in the number of steps that represent the period.

    Generally time series can have repetitive peaks in them that represent some
    operation/s that occur only a basis or daily basis.  These peaks will often
    trigger algorithms.  This function identifies these types of patterns in
    terms of peaks not troughs.  Troughs are not considered as they are much
    less prevalent as repetitive patterns in server and application metrics.
    The majority of these patterns display significant peaks in the same time
    period every day.  Peak indices identified be this function can be
    considered as normal behaviour.

    :param timeseries: the time series
    :param sigma: the sigma value on which to calculate a mean based threshold.
        Only peaks with a value above this threshold will be considered.  This
        value defaults to 2 as the types of patterns been searched will always
        be above 2-sigma.
    :param min_peak_sigma:  this is the minimum sigma  
    :type current_skyline_app: str
    :type timeseries: list
    :return: normalized_var
    :rtype: float
    """
    repetitive_peaks = {}
    repetitive_peaks = {'peak_periods': {}, 'repetitive_peak_indices': []}

    values = np.array([x[1] for x in timeseries])
    mean_value = np.nanmean(values)
    std_dev = np.nanstd(values)
    threshold = mean_value + sigma * std_dev
#    print('mean:', mean_value, 'std_dev:', std_dev, 'threshold:', threshold)
    peaks, _ = find_peaks(values)
    # Coerce to int
    peaks = [int(i) for i in peaks]

    candidate_peaks = {}
    for i, item in enumerate(timeseries):
        if item[1] > threshold:
            candidate_peaks[i] = item
    candidate_peak_indices = list(candidate_peaks.keys())

    peak_periods = {}
    for p in peaks:
        p_value = timeseries[p][1]
        p_zscore = int((p_value - mean_value) / std_dev)
        if p_zscore < 1:
            continue
        if p_value < threshold:
            continue
        # Check other peaks
        for op in peaks:
            if p == op:
                continue
            if op < p:
                continue
            op_value = timeseries[op][1]
            if op_value < threshold:
                continue
            period = int(op - p)
            # Do not consider any periods with less than 3 steps
            if period < 3:
                continue
            op_zscore = int((op_value - mean_value) / std_dev)
            if op_zscore < 1:
                continue
            op_zscores = list(range((op_zscore - 2), (op_zscore + 2)))
            # Similiar zscores rounded to the int indicate similiar value range
            if p_zscore not in op_zscores:
                continue
            if period not in peak_periods.keys():
                peak_periods[period] = []
            if [p, op] not in peak_periods[period]:
                peak_periods[period].append([p, op])
    periods = list(peak_periods.keys())
    peak_periods_all_indices = []
    for period, period_indices in peak_periods.items():
        for item in period_indices:
            for index in item:
                peak_periods_all_indices.append(index)
    last_peak_period_indices = []
    if peak_periods_all_indices:
        peak_periods_all_indices = sorted(list(set(peak_periods_all_indices)))
        last_peak_period_indices = peak_periods_all_indices[-2:]
    last_timeseries_index = len(timeseries) - 1
    last_timeseries_indices = [last_timeseries_index - 2, last_timeseries_index - 1, last_timeseries_index]
    padded_periods = {}
    for period in periods:
        padded_periods[period] = list(range((period - 3), (period + 3)))
    # Added the final peaks if they have not been added because the addition is
    # based on period comparison with other peaks IN FRONT of the peak.  With
    # the peaks identified at the end of the time series, they must be checked
    # against the previous peaks.
    for index in last_timeseries_indices:
        if index in peaks and index not in peak_periods_all_indices:
            for pi in peaks:
                eval_period = (index - pi)
                for period, period_list in padded_periods.items():
                    if eval_period in period_list:
                        peak_periods[period].append([pi, index])
    for period in periods:
        all_peak_period_indices = []
        if len(peak_periods[period]) < 0:
            for item in peak_periods[period]:
                for pid in item:
                    all_peak_period_indices.append(pid)
        all_peak_period_indices = sorted(list(set(all_peak_period_indices)))
        for pi in peaks:
            if pi in all_peak_period_indices:
                continue
            padded_pi = list(range((pi - 3), (pi + 3)))
            add_index = []
            use_index = None
            for pid in padded_pi:
                if pid in all_peak_period_indices:
                    add_index.append(pid)
                    if not use_index:
                        use_index = pid
            if add_index:
                peak_periods[period].append([pi, (use_index + period)])
    for period in periods:
        if len(peak_periods[period]) < 5:
            del peak_periods[period]
    repetitive_peak_indices = []
    for period in peak_periods.keys():
        period_indices = peak_periods[period]
        for item in period_indices:
            p = item[0]
            if p not in repetitive_peak_indices:
                repetitive_peak_indices.append(p)
            op = item[1]
            if op not in repetitive_peak_indices:
                repetitive_peak_indices.append(op)
    repetitive_peaks['repetitive_peak_indices'] = sorted(repetitive_peak_indices)
    repetitive_peaks['peak_periods'] = peak_periods
    repetitive_peaks['peaks'] = peaks

    return repetitive_peaks


