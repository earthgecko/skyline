"""
bq_df_to_metrics.py
"""
import logging
from datetime import datetime
from dateutil import parser

import pandas as pd
import numpy as np


# @added 20240516 - Feature #5352: vista - bigquery
def bq_df_to_metrics(
        current_skyline_app, bq_account_settings, df, backfill=False,
        zero_fill=False, return_nan_metrics=False):
    """
    Returns a list of metric dicts
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    metrics = []
    nan_metrics = []

    metric_prefix = bq_account_settings['metric_prefix']
    primary_dimension = bq_account_settings['primary_dimension']
    metric_dimensions = bq_account_settings['metric_dimensions']
    date_field = bq_account_settings['date_field']
    metric_format = bq_account_settings['metric_format']

    for index, row in df.iterrows():
        try:
            primary_dimension_value = str(row[primary_dimension])
        except Exception as err:
            current_logger.error('error :: bq_df_to_metrics :: failed to determine the primary_dimension_value from row: %s, err: %s' % (
                str(row), err))
            break
        try:
            date_string = str(row[date_field])
            # Seeing as the date format may be unknown use
            # dateutil.parser which can automatically parse date strings in a variety of formats and
            # handles many common date formats without needing explicit format strings.
            # Even though the date fields in the pandas_gbq seem to return as
            # type datetime.date, you never know...
            dt = parser.parse(date_string)
            # Convert the datetime object to a Unix timestamp
            unix_timestamp = int(dt.timestamp())
        except Exception as err:
            current_logger.error('error :: bq_df_to_metrics :: failed to determine the unix_timestamp from row: %s, err: %s' % (
                str(row), err))
            break
        for metric_dimension in metric_dimensions:
            try:
                metric = '%s.%s.%s' % (metric_prefix, primary_dimension_value, str(metric_dimension))
                value = float(row[metric_dimension])
                nan_metric = False
                if np.isnan(value) and zero_fill:
                    value = 0.0
                if np.isnan(value):
                    nan_metric = True
                    value = None
                metric_data = {'metric': metric, 'timestamp': unix_timestamp, 'value': value}
                if backfill:
                    metric_data['fill'] = True
                if nan_metric:
                    nan_metrics.append(metric_data)
                # Flux will only accept floats so if it is a nan do not
                # submit it
                if metric_format == 'graphite' and nan_metric:
                    continue
                metrics.append(metric_data)
            except Exception as err:
                current_logger.error('error :: bq_df_to_metrics :: failed to determine a metric for %s, err: %s' % (
                    str(metric_dimension), err))
                continue

    if return_nan_metrics and nan_metrics:
        for metric_data in nan_metrics:
            metrics.append(metric_data)
    if not metrics:
        current_logger.error('error :: bq_df_to_metrics :: failed to compose any metrics')
    else:
        current_logger.info('bq_df_to_metrics :: generated %s metrics' % str(len(metrics)))

    return metrics
