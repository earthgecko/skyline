"""
skyline_prophet.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import pandas as pd
from prophet import Prophet

logger = logging.getLogger('cmdstanpy')
logger.addHandler(logging.NullHandler())
logger.propagate = False
logger.setLevel(logging.CRITICAL)

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221127 - Feature #4742: custom_algorithms - skyline_prophet
def skyline_prophet(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Outlier detector for time-series data using the spectral residual algorithm.
    Based on the alibi-detect implementation of "Time-Series Anomaly Detection
    Service at Microsoft" (Ren et al., 2019) https://arxiv.org/abs/1906.03821

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1667608854, 1269121024.0],
        [1667609454, 1269174272.0], [1667610054, 1269174272.0]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the anomalous_daily_peak
        custom algorithm no specific algorithm_parameters are required apart
        from an empty dict, example:
        ``algorithm_parameters={'return_instance_score': True}``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, instance_scores
    :rtype: tuple(boolean, float, instance_scores)

    """

    def fit_predict_model(
        dataframe, interval_width=0.99, changepoint_range=0.8,
            daily_seasonality=False, yearly_seasonality=False,
            weekly_seasonality=False, seasonality_mode='multiplicative'):
        m = Prophet(daily_seasonality=daily_seasonality,
                    yearly_seasonality=yearly_seasonality,
                    weekly_seasonality=weekly_seasonality,
                    seasonality_mode=seasonality_mode,
                    interval_width=interval_width,
                    changepoint_range=changepoint_range)
        m = m.fit(dataframe)
        forecast = m.predict(dataframe)
        forecast['fact'] = dataframe['y'].reset_index(drop=True)
        return forecast

    def detect_anomalies(forecast):
        forecasted = forecast[['ds', 'trend', 'yhat', 'yhat_lower', 'yhat_upper', 'fact']].copy()
        # forecast['fact'] = df['y']
        forecasted['anomaly'] = 0
        forecasted.loc[forecasted['fact'] > forecasted['yhat_upper'], 'anomaly'] = 1
        forecasted.loc[forecasted['fact'] < forecasted['yhat_lower'], 'anomaly'] = -1
        # anomaly importances
        forecasted['importance'] = 0
        forecasted.loc[forecasted['anomaly'] == 1, 'importance'] = (forecasted['fact'] - forecasted['yhat_upper']) / forecast['fact']
        forecasted.loc[forecasted['anomaly'] == -1, 'importance'] = (forecasted['yhat_lower'] - forecasted['fact']) / forecast['fact']
        return forecasted

    # You MUST define the algorithm_name
    algorithm_name = 'skyline_prophet'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    results = {'anomalies': [], 'prophet_scores': []}

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    return_results = False
    try:
        return_results = algorithm_parameters['return_results']
    except:
        return_results = False

    if not return_results:
        try:
            return_results = algorithm_parameters['return_anomalies']
        except:
            return_results = False

    # Use the algorithm_parameters to determine the sample_period
    debug_logging = None
    try:
        debug_logging = algorithm_parameters['debug_logging']
    except:
        debug_logging = False
    if debug_logging:
        try:
            current_logger = get_log(current_skyline_app)
            current_logger.debug('debug :: %s :: debug_logging enabled with algorithm_parameters - %s' % (
                algorithm_name, str(algorithm_parameters)))
        except:
            # This except pattern MUST be used in ALL custom algortihms to
            # facilitate the traceback from any errors.  The algorithm we want to
            # run super fast and without spamming the log with lots of errors.
            # But we do not want the function returning and not reporting
            # anything to the log, so the pythonic except is used to "sample" any
            # algorithm errors to a tmp file and report once per run rather than
            # spewing tons of errors into the log e.g. analyzer.log
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            # Return None and None as the algorithm could not determine True or False
            if return_results:
                return (None, None, results)
            return (None, None)

    # Use the algorithm_parameters to determine variables
    debug_print = None
    try:
        debug_print = algorithm_parameters['debug_print']
    except:
        debug_print = False
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1
    interval_width = 0.99
    try:
        interval_width = float(algorithm_parameters['interval_width'])
    except:
        interval_width = 0.99
    changepoint_range = 0.8
    try:
        changepoint_range = float(algorithm_parameters['changepoint_range'])
    except:
        changepoint_range = 0.8
    daily_seasonality = False
    try:
        daily_seasonality = algorithm_parameters['daily_seasonality']
    except:
        daily_seasonality = False
    yearly_seasonality = False
    try:
        yearly_seasonality = algorithm_parameters['yearly_seasonality']
    except:
        yearly_seasonality = False
    weekly_seasonality = False
    try:
        weekly_seasonality = algorithm_parameters['weekly_seasonality']
    except:
        weekly_seasonality = False
    seasonality_mode = 'multiplicative'
    try:
        seasonality_mode = algorithm_parameters['seasonality_mode']
    except:
        seasonality_mode = 'multiplicative'

    prophet_anomalies = []

    try:
        prophet_df = pd.DataFrame(timeseries, columns=['ds', 'y'])
        prophet_df['ds'] = pd.to_datetime(prophet_df['ds'], unit='s')

        pred = fit_predict_model(prophet_df, interval_width=interval_width,
                                 changepoint_range=changepoint_range,
                                 daily_seasonality=daily_seasonality,
                                 yearly_seasonality=yearly_seasonality,
                                 weekly_seasonality=weekly_seasonality,
                                 seasonality_mode=seasonality_mode)
        pred = detect_anomalies(pred)
        a_df = pred.loc[(pred['anomaly'] > 0) & (pred['importance'] > 0)]

        prophet_anomalies_df = a_df[['ds', 'fact']].copy()
        dates = prophet_anomalies_df['ds'].tolist()
        prophet_anomaly_timestamps, prophet_anomalies = [], []
        for d in dates:
            prophet_anomaly_timestamps.append(int(d.strftime('%s')))
        for item in timeseries:
            if int(item[0]) in prophet_anomaly_timestamps:
                prophet_anomalies.append(1)
            else:
                prophet_anomalies.append(0)

        anomaly_sum = sum(prophet_anomalies[-anomaly_window:])
        anomalies = {}
        for index, item in enumerate(timeseries):
            if prophet_anomalies[index] == 1:
                ts = int(item[0])
                anomalies[ts] = {'value': item[1], 'index': index, 'score': 1}
        if anomaly_sum > 0:
            anomalous = True
            anomalyScore = 1.0
        else:
            anomalous = False
            anomalyScore = 0.0
        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'anomalyScore_list': prophet_anomalies,
            'scores': prophet_anomalies,
        }

    except StopIteration:
        if debug_print:
            print('warning - StopIteration called on prophet')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on prophet')

        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_results:
            return (None, None, results)
        return (None, None)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on prophet - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, results)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
