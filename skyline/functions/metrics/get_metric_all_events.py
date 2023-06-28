"""
get_metric_all_events.py
"""
import logging
from time import strftime, gmtime, mktime
from ast import literal_eval
import copy
import datetime
from timeit import default_timer as timer

from skyline_functions import get_redis_conn_decoded, get_graphite_metric
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.database.queries.get_matches import get_matches
from functions.database.queries.query_anomalies import get_anomalies_for_period
from functions.timeseries.determine_data_frequency import determine_data_frequency


# @added 20230126 - Feature #4530: namespace.analysed_events
def get_metric_all_events(
        current_skyline_app, base_name, from_timestamp, until_timestamp):
    """
    Return a dict with the metric timeseries timestamps, values, analyzer_all events,
    analyzer events, mirage events, anomalies and matches.

    :param current_skyline_app: the app calling the function
    :param base_name: the base_name of the metric
    :param from_timestamp: get analsed events from
    :param until_timestamp: get analsed events until
    :type current_skyline_app: str
    :type base_name: str
    :type from_timestamp: int
    :type until_timestamp: int
    :return: all_events_dict
    :rtype: dict

    """

    function_str = 'functions.metrics.get_metric_all_events'
    all_events_dict = {}

    start_start = timer()

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: %s :: determining all events for %s from: %s, until: %s' % (
        current_skyline_app, function_str, base_name, from_timestamp,
        until_timestamp))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))

    graphite = True
    query_by_metric_id_str = False
    use_base_name = str(base_name)
    tsdb_base_name = str(base_name)
    metric_id = 0
    if base_name.startswith('labelled_metrics.'):
        graphite = False
        query_by_metric_id_str = True
        current_logger.info('%s :: looking up base_name for %s' % (function_str, base_name))
        try:
            tsdb_base_name = get_base_name_from_labelled_metrics_name(current_skyline_app, base_name)
            current_logger.info('%s :: base_name: %s' % (function_str, tsdb_base_name))
        except Exception as err:
            current_logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                function_str, base_name, err))
        try:
            metric_id_str = base_name.replace('labelled_metrics.', '', 1)
            metric_id = int(float(metric_id_str))
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine metric id for %s - %s' % (
                function_str, base_name, err))
    if 'tenant_id="' in base_name:
        graphite = False
        query_by_metric_id_str = True
        current_logger.info('%s :: looking up metric id for %s' % (function_str, base_name))
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except Exception as err:
            current_logger.error('error :: %s :: get_metric_id_from_base_name failed for %s - %s' % (
                function_str, base_name, err))
        current_logger.info('%s :: looked up metric id as %s' % (function_str, str(metric_id)))
        if metric_id:
            use_base_name = 'labelled_metrics.%s' % str(metric_id)

    timeseries = []
    if graphite:
        try:
            timeseries = get_graphite_metric(current_skyline_app, use_base_name, from_timestamp, until_timestamp, 'list', 'object')
        except Exception as err:
            current_logger.error('error :: %s :: get_graphite_metric failed - %s' % (
                function_str, err))
    else:
        try:
            timeseries = get_victoriametrics_metric(current_skyline_app, tsdb_base_name, from_timestamp, until_timestamp, 'list', 'object')
        except Exception as err:
            current_logger.error('error :: %s :: get_graphite_metric failed - %s' % (
                function_str, err))

    # @added 20230127 - Feature #4830: webapp - panorama_plot_anomalies - all_events
    # Determine the resolution because VicotriaMetrics metrics are currently at
    # step=600 and the illuminance key data needs to be aligned from 60 to 600
    try:
        resolution = determine_data_frequency(current_skyline_app, timeseries, False)
    except Exception as err:
        current_logger.error('error :: determine_data_frequency failed for %s - %s' % (
            use_base_name, err))
        resolution = 60
        if not graphite:
            resolution = 600

    all_events_dict_timestamps = []
    all_events_dict = {}
    for ts, value in timeseries:
        aligned_ts = int(int(ts) // resolution * resolution)
        all_events_dict[aligned_ts] = {}
        all_events_dict[aligned_ts]['value'] = value
        all_events_dict_timestamps.append(aligned_ts)
    if not metric_id:
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, use_base_name)
        except Exception as err:
            current_logger.error('error :: %s :: get_metric_id_from_base_name failed for %s - %s' % (
                function_str, use_base_name, err))

    matches = {}
    current_logger.info('%s :: %s :: getting matches for %s with metric_id: %s, from: %s, until: %s' % (
        current_skyline_app, function_str, base_name, str(metric_id), from_timestamp,
        until_timestamp))
    try:
        matches = get_matches(current_skyline_app, metric_id, from_timestamp, until_timestamp)
    except Exception as err:
        current_logger.error('error :: %s :: get_matches failed for metric_id %s - %s' % (
            function_str, str(metric_id), err))
    current_logger.info('%s :: %s :: got %s matches for %s' % (
        current_skyline_app, function_str, str(len(matches)), base_name))

    matched_dict = {}
    for match_id in list(matches.keys()):
        ts = int(matches[match_id]['metric_timestamp'] // resolution * resolution)
        matched_dict[ts] = 1

    current_logger.info('%s :: %s :: getting anomalies for %s from: %s, until: %s' % (
        current_skyline_app, function_str, base_name, from_timestamp,
        until_timestamp))
    anomalies = {}
    try:
        anomalies = get_anomalies_for_period(current_skyline_app, [metric_id], from_timestamp, until_timestamp)
    except Exception as err:
        current_logger.error('error :: %s :: get_anomalies_for_period failed for metric_id %s - %s' % (
            function_str, str(metric_id), err))
    current_logger.info('%s :: %s :: got %s anomalies for %s' % (
        current_skyline_app, function_str, str(len(anomalies)), base_name))

    anomalies_dict = {}
    for anomaly_id in list(anomalies.keys()):
        try:
            ts = int(anomalies[anomaly_id]['anomaly_timestamp'] // resolution * resolution)
            anomalies_dict[ts] = 1
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine anomaly - %s' % (
                function_str, err))

    current_logger.info('%s :: %s :: determining illuminance keys to get' % (
        current_skyline_app, function_str))

    date_keys_to_get = {}
    try:
        from_date_str = str(strftime('%Y-%m-%d', gmtime(from_timestamp)))
        until_date_str = str(strftime('%Y-%m-%d', gmtime(until_timestamp)))
        from_date_day_start_timestamp = int(mktime(datetime.datetime.strptime(from_date_str, '%Y-%m-%d').timetuple()))
        current_key_date_str = str(from_date_str)
        current_date_timestamp = from_date_day_start_timestamp
        while current_key_date_str != until_date_str:
            date_keys_to_get[current_key_date_str] = current_date_timestamp
            current_date_timestamp = current_date_timestamp + 86400
            current_key_date_str = str(strftime('%Y-%m-%d', gmtime(current_date_timestamp)))
        date_keys_to_get[current_key_date_str] = current_date_timestamp
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine date_keys for illuminance data - %s' % (
            function_str, err))
    current_logger.info('%s :: %s :: using illuminance keys from %s' % (
        current_skyline_app, function_str, str(date_keys_to_get)))

    analyzer_events = {}
    mirage_events = {}

    # @added 20230126 - Feature #4830: webapp - panorama_plot_anomalies - all_events
    analyzer_all_events = {}

    for date_str in list(date_keys_to_get.keys()):
        try:
            current_logger.info('%s :: %s :: getting illuminance.%s keys for %s' % (
                current_skyline_app, function_str, date_str, base_name))
            key = 'analyzer.illuminance.%s' % date_str
            c_analyzer_events = {}
            try:
                c_analyzer_events = redis_conn_decoded.hgetall(key)
            except Exception as err:
                current_logger.error('error :: %s :: hget failed on %s - %s' % (
                    function_str, str(key), err))
            for ts in list(c_analyzer_events.keys()):
                analyzer_events[ts] = copy.deepcopy(c_analyzer_events[ts])

            # @added 20230126 - Feature #4830: webapp - panorama_plot_anomalies - all_events
            key = 'analyzer.illuminance.all.%s' % date_str
            c_analyzer_all_events = {}
            try:
                c_analyzer_all_events = redis_conn_decoded.hgetall(key)
            except Exception as err:
                current_logger.error('error :: %s :: hget failed on %s - %s' % (
                    function_str, str(key), err))

            # @added 20230324 - Feature #4530: namespace.analysed_events
            # Speed up, reduce the number of analyzer_all_events
            aligned_c_analyzer_all_events_timestamps = []
            get_event_timestamps = []
            for ts in list(c_analyzer_all_events.keys()):
                aligned_ts = int((int(float(ts)) // resolution * resolution))
                if aligned_ts in all_events_dict_timestamps:
                    get_event_timestamps.append(ts)
            # get_event_timestamps = list(set(all_events_dict_timestamps) & set([int((int(float(ts)) // resolution * resolution)) for ts in list(c_analyzer_all_events.keys())]))
            current_logger.info('%s :: %s :: retrieving %s keys from c_analyzer_all_events' % (
                current_skyline_app, function_str, str(len(get_event_timestamps))))
            for ts in get_event_timestamps:
                try:
                    analyzer_all_events[ts] = copy.deepcopy(c_analyzer_all_events[ts])
                except Exception as err:
                    current_logger.warning('warning :: %s :: failed to deepcopy %s from c_analyzer_all_events - %s' % (
                        function_str, str(ts), err))

            # @modified 20230324 - Feature #4530: namespace.analysed_events
            # Deprecate slower old method
            # for ts in list(c_analyzer_all_events.keys()):
            #     analyzer_all_events[ts] = copy.deepcopy(c_analyzer_all_events[ts])

            key = 'mirage.illuminance.%s' % date_str
            c_mirage_events = {}
            try:
                c_mirage_events = redis_conn_decoded.hgetall(key)
            except Exception as err:
                current_logger.error('error :: %s :: hget failed on %s - %s' % (
                    function_str, str(key), err))
            for ts in list(c_mirage_events.keys()):
                mirage_events[ts] = copy.deepcopy(c_mirage_events[ts])
        except Exception as err:
            current_logger.error('error :: %s :: failed to get illuminance data - %s' % (
                function_str, err))

    current_logger.info('%s :: %s :: creating all_events_dict for %s' % (
        current_skyline_app, function_str, base_name))

    # @added 20230128 - Feature #4830: webapp - panorama_plot_anomalies - all_events
    # Determine the resolution because VicotriaMetrics metrics are currently at
    # step=600 and the illuminance key data needs to be aligned from 60 to 600
    # all_events_dict_timestamps = list(all_events_dict.keys())
    analyzer_events_timestamp_strs = list(analyzer_events.keys())
    start = timer()
    analyzer_illuminance_events_added = 0
    for analyzer_events_timestamp_str in analyzer_events_timestamp_strs:
        aligned_analyzer_events_timestamp = int((int(float(analyzer_events_timestamp_str)) // resolution * resolution))
        if aligned_analyzer_events_timestamp in all_events_dict_timestamps:
            try:
                if all_events_dict[ts]['analyzer']['triggered_algorithms_count']:
                    continue
            except:
                triggered_algorithms_count = 0
            triggered_algorithms_count = 0
            events_str = None
            events = None
            try:
                events_str = analyzer_events[str(analyzer_events_timestamp_str)]
            except KeyError:
                events_str = None
            if events_str:
                try:
                    events = literal_eval(events_str)
                except:
                    events = None
            if events:
                for i_metric in list(events.keys()):
                    add_event = False
                    if query_by_metric_id_str:
                        if i_metric == str(metric_id):
                            add_event = True
                    else:
                        if i_metric == use_base_name:
                            add_event = True
                    if add_event:
                        triggered_algorithms_count = events[i_metric]['triggered_algorithms_count']
                        analyzer_illuminance_events_added += 1
            all_events_dict[aligned_analyzer_events_timestamp]['analyzer'] = {'triggered_algorithms_count': triggered_algorithms_count}
    current_logger.info('%s :: %s :: processed analyzer.illuminance and added %s events in %s seconds' % (
        current_skyline_app, function_str, str(analyzer_illuminance_events_added),
        (timer() - start)))

    analyzer_all_events_timestamp_strs = list(analyzer_all_events.keys())
    current_logger.info('%s :: %s :: len(analyzer_all_events_timestamp_strs): %s' % (
        current_skyline_app, function_str, str(len(analyzer_all_events_timestamp_strs))))

    # @added 20230324 - Feature #4530: namespace.analysed_events
    # Speed up.  Check if the metric_id is in the events_str first before
    # literal_eval.  This MASSIVELY sped up to process from 43.11437844391912 seconds
    # to 4.664127588039264 (10 time faster)!
    first_str = "{'%s': {" % str(metric_id)
    second_str = "}, '%s': {" % str(metric_id)

    start = timer()
    analyzer_all_illuminance_events_added = 0
    for analyzer_all_events_timestamp_str in analyzer_all_events_timestamp_strs:
        try:
            aligned_analyzer_all_events_timestamp = int((int(float(analyzer_all_events_timestamp_str)) // resolution * resolution))
        except Exception as err:
            current_logger.error('error :: %s :: %s :: failed to determine aligned_analyzer_all_events_timestamp - %s' % (
                current_skyline_app, function_str, err))
            aligned_analyzer_all_events_timestamp = 0

        if aligned_analyzer_all_events_timestamp in all_events_dict_timestamps:
            try:
                try:
                    if all_events_dict[ts]['analyzer_all']['triggered_algorithms_count']:
                        continue
                except:
                    triggered_algorithms_count = 0
                triggered_algorithms_count = 0
                events_str = None
                events = None
                try:
                    events_str = analyzer_all_events[str(analyzer_all_events_timestamp_str)]
                except KeyError:
                    events_str = None

                # @added 20230324 - Feature #4530: namespace.analysed_events
                # Speed up.  Check if the metric_id is in the events_str first before
                # literal_eval.  This MASSIVELY sped up to process from 104.71571656106971
                # to 43.11437844391912 seconds and then managed to get it to
                # to 4.664127588039264 (10 times faster then the first
                # improvement made!) OVERALL 26 times faster!
                get_data = False
                if first_str in events_str or second_str in events_str:
                    get_data = True
                if not get_data:
                    events_str = None

                if events_str:
                    try:
                        events = literal_eval(events_str)
                    except:
                        events = None

                if events:
                    for i_metric_id in list(events.keys()):
                        if int(i_metric_id) == metric_id:
                            triggered_algorithms_count = len(events[i_metric_id]['a'])
                            analyzer_all_illuminance_events_added += 1
                all_events_dict[aligned_analyzer_all_events_timestamp]['analyzer_all'] = {'triggered_algorithms_count': triggered_algorithms_count}
            except Exception as err:
                current_logger.error('error :: %s :: %s :: building all_events_dict - %s' % (
                    current_skyline_app, function_str, err))

    current_logger.info('%s :: %s :: processed analyzer.illuminance.all and added %s events in %s seconds' % (
        current_skyline_app, function_str, str(analyzer_all_illuminance_events_added),
        (timer() - start)))

    mirage_events_timestamp_strs = list(mirage_events.keys())
    start = timer()
    mirage_illuminance_events_added = 0
    for mirage_events_timestamp_str in mirage_events_timestamp_strs:
        aligned_mirage_events_timestamp = int((int(float(mirage_events_timestamp_str)) // resolution * resolution))
        if aligned_mirage_events_timestamp in all_events_dict_timestamps:
            try:
                if all_events_dict[ts]['mirage']['triggered_algorithms_count']:
                    continue
            except:
                triggered_algorithms_count = 0
            triggered_algorithms_count = 0
            events_str = None
            events = None
            try:
                events_str = mirage_events[str(mirage_events_timestamp_str)]
            except KeyError:
                events_str = None
            if events_str:
                try:
                    events = literal_eval(events_str)
                except:
                    events = None
            if events:
                for i_metric in list(events.keys()):
                    add_event = False
                    if query_by_metric_id_str:
                        if i_metric == str(metric_id):
                            add_event = True
                    else:
                        if i_metric == use_base_name:
                            add_event = True
                    if add_event:
                        triggered_algorithms_count = events[i_metric]['triggered_algorithms_count']
                        mirage_illuminance_events_added += 1
                        
                        current_logger.debug('debug :: %s :: %s :: mirage.illuminance event %s, aligned_mirage_events_timestamp: %s, event: %s' % (
                            current_skyline_app, function_str, str(i_metric),
                            str(aligned_mirage_events_timestamp), str(events[i_metric])))

            all_events_dict[aligned_mirage_events_timestamp]['mirage'] = {'triggered_algorithms_count': triggered_algorithms_count}
    current_logger.info('%s :: %s :: processed mirage.illuminance and added %s events in %s seconds' % (
        current_skyline_app, function_str, str(mirage_illuminance_events_added),
        (timer() - start)))
    start = timer()
    for index, ts in enumerate(list(all_events_dict.keys())):
        all_events_dict[ts]['index'] = index
        try:
            triggered_count = all_events_dict[ts]['analyzer']['triggered_algorithms_count']
        except:
            all_events_dict[ts]['analyzer'] = {'triggered_algorithms_count': 0}
        try:
            triggered_count = all_events_dict[ts]['analyzer_all']['triggered_algorithms_count']
        except:
            all_events_dict[ts]['analyzer_all'] = {'triggered_algorithms_count': 0}
        try:
            triggered_count = all_events_dict[ts]['mirage']['triggered_algorithms_count']
        except:
            all_events_dict[ts]['mirage'] = {'triggered_algorithms_count': 0}
        try:
            all_events_dict[ts]['match'] = matched_dict[ts]
        except KeyError:
            all_events_dict[ts]['match'] = 0
        try:
            all_events_dict[ts]['anomaly'] = anomalies_dict[ts]
        except KeyError:
            all_events_dict[ts]['anomaly'] = 0
    current_logger.info('%s :: %s :: filled all events in %s seconds' % (
        current_skyline_app, function_str, (timer() - start)))

    current_logger.info('%s :: %s :: created all_events_dict of length %s in %s seconds for %s' % (
        current_skyline_app, function_str, str(len(all_events_dict)),
        str(timer() - start_start), base_name))

    return all_events_dict
