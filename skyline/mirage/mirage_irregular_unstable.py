"""
mirage_irregular_unstable.py
"""
import logging
import traceback

import numpy as np

import settings

from functions.timeseries.normalized_variance import normalized_variance
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from skyline_functions import get_graphite_metric

skyline_app = 'mirage'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

def mirage_irregular_unstable(base_name, timeseries):
    """
    Determine if a timeseries is irregular and unstable and if it is, determine
    if it is also irregular and unstable at 30 days, if it is return the
    timeseries_30d

    :param base_name: the metric base_name.
    :param timeseries: the time series as a list e.g. ``[[1667608854, 1269121024.0],
        [1667609454, 1269174272.0], [1667610054, 1269174272.0]]``
    :type base_name: str
    :type timeseries: list
    :return: timeseries_30d
    :rtype: tuple(boolean, float)

    """
    logger.info('mirage_irregular_unstable :: checking %s' % base_name)

    results = {
        'irregular_unstable': False,
        'resolution_7d': None,
        'skipped': None,
        'normalised_var_7d': None,
        'normalised_var_30d': None,
        'timeseries_30d': [],
    }
    timeseries_30d = []
    low_variance_7d = 0.009

    # On 20230505 for Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
    # After analysing and assessing the variance values for 251 tN SNAB results and
    # 7 fN results and 1 unsure result, it was determined that at 30 days the
    # normalised_var should be < 0.0065.  Of the 251 tN result only 7 of those were
    # >= 0.0065 (0.0065, 0.0065, 0.0066, 0.0066, 0.0081, 0.0096, 0.0104).  These
    # were reassessed and found that they could be deemed either way.  Further the
    # unsure metric was 0.0065, therefore for the sake of perhaps 2.5% fPs we ensure
    # less fNs or unsures (with current knowledge)
    low_variance_30d = 0.0065

    try:
        timestamps = [int(item[0]) for item in timeseries]
        np_timestamps = np.array(timestamps)
        ts_diffs = np.diff(np_timestamps)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        results['resolution_7d'] = resolution
        if resolution > 900:
            # Not suited to low resolution data
            results['skipped'] = 'Not suited to low resolution > 900'
            return results
        duration = timestamps[-1] - timestamps[0]
        results['duration'] = duration
        if duration < 446400:
            # Not suitable for less than 5.25 days worth of data
            results['skipped'] = 'Not suitable for less than 5.25 days worth of data'
            return results

        normalized_var = None
        try:
            normalized_var = normalized_variance(timeseries)
        except Exception as err:
            results['normalized_var execution error'] = err
            return results
        if not isinstance(normalized_var, float):
            results['normalized_var error'] = normalized_var['error']
            return results
        results['normalised_var_7d'] = normalized_var
        if normalized_var > low_variance_7d:
            results['skipped'] = 'normalized_var > %s' % str(low_variance_7d)
            return results

        until_timestamp = timestamps[-1]
        from_timestamp = until_timestamp - (86400 * 30)
        try:
            if '_tenant_id=' not in base_name:
                timeseries_30d = get_graphite_metric(skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
            else:
                timeseries_30d = get_victoriametrics_metric(skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
        except Exception as err:
            logger.error('error :: no timeseries_30d fetched for %s - %s' % (base_name, err))
            results['skipped'] = 'no timeseries_30d fetched - %s' % str(err)
            return results
        if len(timeseries_30d) == 0:
            results['skipped'] = 'no timeseries_30d returned'
            return results

        if len(timeseries_30d) <= len(timeseries):
            results['skipped'] = 'len(timeseries_30d) <= len(timeseries)'
            return results

        normalized_var_30d = None
        try:
            normalized_var_30d = normalized_variance(timeseries_30d)
        except Exception as err:
            results['normalized_var_30d execution error'] = err
            return results
        if not isinstance(normalized_var_30d, float):
            results['normalized_var_30d error'] = normalized_var_30d['error']
            return results
        results['normalised_var_30d'] = normalized_var_30d
        if normalized_var_30d >= low_variance_30d:
            results['skipped'] = 'normalized_var_30d >= %s (low_variance_30d)' % str(low_variance_30d)
            return results

        results['irregular_unstable'] = True
        results['timeseries_30d'] = timeseries_30d
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: mirage_irregular_unstable failed - %s' % err)
        results['error'] = err

    return results