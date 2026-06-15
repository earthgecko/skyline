import copy
import logging
import traceback

import settings
import skyline_version

from sqlalchemy.sql import select
# @added 20250117 - Feature #5588: snab.process_algorithm
from sqlalchemy import or_

from database import get_engine, snab_table_meta

# @added 20230802 - Feature #5038: snab_results_algorithms
#                   Feature #5008: webapp - snab report page
from functions.database.queries.get_snab_algorithms_results import get_snab_algorithms_results

# @added 20230915 - Feature #5070: snab - anomaly_detection_metrics
from functions.snab.anomaly_detection_metrics import anomaly_detection_metrics

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def get_snab_engine():

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: get_snab_report :: failed to get MySQL engine for snab table'
        logger.error('%s' % fail_msg)
        # return None, fail_msg, trace
        raise  # to webapp to return in the UI


def snab_engine_disposal(engine):
    if engine:
        try:
            engine.dispose()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_snab_report :: calling engine.dispose()')
    return


# @added 20230718 - Feature #5008: webapp - snab report page
def get_snab_report(filter_on):
    """
    Get the relevant data from the snab table.

    :param filter_on: the filter_on dictionary
    :param algorithms: the algorithms dictionary
    :param algorithm_groups: the algorithm_groups dictionary
    :type filter_on: dict
    :type algorithms: dict
    :type algorithm_groups: dict
    :return: report_data
    :rtype: dict

    """

    # @added 20230915 - Feature #5008: webapp - snab report page
    # Add heatmap to snab report page
    def all_channels2(func):
        def wrapper(channel1, channel2, *args, **kwargs):
            try:
                return func(channel1, channel2, *args, **kwargs)
            except TypeError:
                return tuple(func(c1, c2, *args, **kwargs) for c1,c2 in zip(channel1, channel2))
        return wrapper

    @all_channels2
    def lerp(color1, color2, frac):
        return color1 * (1 - frac) + color2 * frac

    def hexcolor(rgb):
        return f'#{int(rgb[0]):0>2X}{int(rgb[1]):0>2X}{int(rgb[2]):0>2X}'

    report_data = {}
    logger.info('get_snab_report :: filter_on: %s' % (str(filter_on)))

    algorithm = None
    algorithm_id = 0
    if filter_on['algorithm']:
        algorithm = filter_on['algorithm']
        if algorithm != 'all':
            algorithm_id = filter_on['algorithms'][algorithm]
    report_data['algorithm'] = algorithm

    algorithms_by_name = filter_on['algorithms']
    algorithms_by_id = {}
    for algo in list(algorithms_by_name.keys()):
        algo_id = algorithms_by_name[algo]
        algorithms_by_id[algo_id] = algo
    algorithm_groups_by_name = filter_on['algorithm_groups']
    algorithm_groups_by_id = {}
    for algorithm_group_name in list(algorithm_groups_by_name.keys()):
        algorithm_group_id = algorithm_groups_by_name[algorithm_group_name]
        algorithm_groups_by_id[algorithm_group_id] = algorithm_group_name

    # @added 20230722 - Feature #5008: webapp - snab report page
    # algorithm_group_ids = []
    # algorithm_ids = []
    # if 'all_algorithms_by_id' in list(filter_on.keys()):
    #     algorithm_groups_by_id = filter_on['all_algorithms_by_id']
    # if 'all_algorithm_groups_by_id' in list(filter_on.keys()):
    #     algorithm_groups_by_id = filter_on['all_algorithm_groups_by_id']
    # if algorithm_groups_by_id:
    #     algorithm_group_ids = list(algorithm_groups_by_id.keys())
    # algorithm_ids = []

    from_timestamp = filter_on['from_timestamp']
    until_timestamp = filter_on['until_timestamp']

    # @added 20230919 - Feature #5008: webapp - snab report page
    # Added the ability to exclude algorithms
    exclude_algorithms = filter_on['exclude_algorithms']

    # @added 20250115 - Feature #5588: snab.process_algorithm
    # Disable algorithms_report and consensuses_report by default
    algorithm_only = True
    if 'algorithm_only' in filter_on:
        algorithm_only = filter_on['algorithm_only']
    algorithms_report = False
    if 'algorithms_report' in filter_on:
        algorithms_report = filter_on['algorithms_report']
    consensuses_report = False
    if 'consensuses_report' in filter_on:
        consensuses_report = filter_on['consensuses_report']
    compare_algorithm_group = None
    if 'compare_algorithm_group' in filter_on:
        compare_algorithm_group = filter_on['compare_algorithm_group']

    if algorithm == 'all':
        algorithm_only = False
        algorithms_report = False
        consensuses_report = False
        compare_algorithm_group = False

    logger.info('get_snab_report :: getting report on algorithm: %s, algorithm_id: %s, with filter_on: %s' % (
        algorithm, str(algorithm_id), str(filter_on)))

    logger.info('get_snab_report :: getting MySQL engine')
    try:
        engine, fail_msg, trace = get_snab_engine()
        logger.info(fail_msg)
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: get_snab_report :: could not get a MySQL engine to get snab table - %s' % str(err))
        raise  # to webapp to return in the UI

    try:
        snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('get_snab_report :: snab_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_report :: failed to get snab_table meta')
        if engine:
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI

    # For performance reasons first get all the anomaly ids that the algorithm
    # has processed
    anomaly_ids = []
    algorithm_results = {}
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([snab_table]).\
        stmt = select(snab_table).\
            where(snab_table.c.algorithm_id == algorithm_id)
        if from_timestamp:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([snab_table]).\
            stmt = select(snab_table).\
            where(snab_table.c.algorithm_id == algorithm_id).\
                where(snab_table.c.snab_timestamp >= int(from_timestamp))
        if until_timestamp:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([snab_table]).\
            stmt = select(snab_table).\
            where(snab_table.c.algorithm_id == algorithm_id).\
                where(snab_table.c.snab_timestamp <= int(until_timestamp))
        if from_timestamp and until_timestamp:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([snab_table]).\
            stmt = select(snab_table).\
            where(snab_table.c.algorithm_id == algorithm_id).\
                where(snab_table.c.snab_timestamp >= int(from_timestamp)).\
                where(snab_table.c.snab_timestamp <= int(until_timestamp))
        if algorithm == 'all':
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([snab_table]).\
            stmt = select(snab_table).\
                where(
                    or_(
                        snab_table.c.tP.isnot(None),
                        snab_table.c.fP.isnot(None),
                        snab_table.c.tN.isnot(None),
                        snab_table.c.fN.isnot(None)
                    )
                )
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]

        for row in results:
            snab_id = row['id']
            algorithm_results[snab_id] = dict(row)
            anomaly_ids.append(row['anomaly_id'])
        #connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_report :: could not determine algorithm_results - %s' % str(err)
        logger.error(fail_msg)
        if engine:
            # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #try:
            #    connection.close()
            #except:
            #    pass
            snab_engine_disposal(engine)
        raise
    logger.info('get_snab_report :: determined %s SNAB results for %s' % (
        str(len(algorithm_results)), algorithm))

    anomaly_ids_set = set(anomaly_ids)
    anomaly_ids = list(set(anomaly_ids))
    snab_records = {}
    algorithm_groups = []

    all_sql_results = {}

    if algorithm == 'all':
        all_sql_results = algorithm_results
    
    if not all_sql_results:
        try:
            #connection = engine.connect()
            # @modified 20230922 - 
            # Improve query performance because the WHERE IN query is inefficient
            if anomaly_ids:
                # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
                #                      Task #5628: Build v5.0.0 and test
                #stmt = select([snab_table], snab_table.c.anomaly_id.in_(anomaly_ids))
                stmt = select(snab_table).\
                        where(snab_table.c.anomaly_id.in_(anomaly_ids))
            else:
                # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
                #                      Task #5628: Build v5.0.0 and test
                #stmt = select([snab_table]).\
                stmt = select(snab_table).\
                    where(snab_table.c.anomaly_id >= min(anomaly_ids)).\
                    where(snab_table.c.anomaly_id <= max(anomaly_ids))
            logger.info('get_snab_report :: querying snab table for %s anomaly_ids' % (
                str(len(anomaly_ids))))
            # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #results = connection.execute(stmt)
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]
            logger.info('get_snab_report :: queried snab table for anomaly_ids')
            for row in results:
                # @added 20230922 - 
                # Improve query performance because the WHERE IN query is inefficient
                all_sql_results[row['id']] = dict(row)
                continue
                snab_id = row['id']
                algorithm_group_id = row['algorithm_group_id']
                algorithm_groups.append(algorithm_group_id)
                snab_records[snab_id] = dict(row)
            #connection.close()
            logger.info('get_snab_report :: created all_sql_results dict with %s items' % str(len(all_sql_results)))
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_snab_report :: could not determine snab_records - %s' % str(err)
            logger.error(fail_msg)
            if engine:
                # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
                #                      Task #5628: Build v5.0.0 and test
                #try:
                #    connection.close()
                #except:
                #    pass
                snab_engine_disposal(engine)
            raise

    # @added 20230922 - 
    # Improve query performance because the WHERE IN query is inefficient
    for key, value in all_sql_results.items():
        # membership of the list is slower than membership of a set
        # if all_sql_results[key]['anomaly_id'] in anomaly_ids:
        if value['anomaly_id'] in anomaly_ids_set:
            algorithm_group_id = value['algorithm_group_id']
            algorithm_groups.append(algorithm_group_id)
            snab_records[key] = value

    logger.info('get_snab_report :: determined %s SNAB results to report on' % (
        str(len(snab_records))))
    algorithm_groups = list(set(algorithm_groups))
    logger.info('get_snab_report :: reporting on algorithm_groups: %s' % (
        str(algorithm_groups)))

    # @added 20230802 - Feature #5038: snab_results_algorithms
    #                   Feature #5008: webapp - snab report page
    snab_checked_snab_ids = []
    snab_checked_snab_ids_results = {}

    # @added 20230915 - Feature #5070: snab - anomaly_detection_metrics
    ad_metric_keys = [
        'f1_score', 'precision', 'recall', 'accuracy', 'fP_rate', 'fN_rate',
        # @added 20230916 - Feature #5070: snab - anomaly_detection_metrics
        #                   Feature #5008: webapp - snab report page
        # Added auc_roc
        'tP_rate', 'tN_rate', 'auc_roc',
    ]

    compare_algorithm_group_name = None
    if compare_algorithm_group:
        try:
            compare_algorithm_group_name = algorithm_groups_by_id[compare_algorithm_group]
        except:
            compare_algorithm_group_name = None
        if not compare_algorithm_group_name:
            logger.error('error :: get_snab_report :: failed to determine compare_algorithm_group_name for compare_algorithm_group with algorithm_group_id: %s' % (
                str(compare_algorithm_group)))

    report_data['use_algorithm_headers'] = algorithm

    report_data['results'] = {}
    report_data['algorithms'] = []
    snab_ids = list(snab_records.keys())
    for algorithm_group_id in algorithm_groups:
        try:
            try:
                algorithm_group_name = algorithm_groups_by_id[algorithm_group_id]
            except:
                continue

            # @added 20230919 - Feature #5008: webapp - snab report page
            # Added the ability to exclude algorithms
            if algorithm_group_name in exclude_algorithms:
                continue

            for snab_id in snab_ids:
                if snab_records[snab_id]['algorithm_group_id'] != algorithm_group_id:
                    continue
                if snab_records[snab_id]['algorithm_id']:
                    i_algorithm_id = int(snab_records[snab_id]['algorithm_id'])
                    i_algorithm = algorithms_by_id[i_algorithm_id]
                else:
                    i_algorithm = algorithm_group_name
                    i_algorithm_id = None
                break

            if algorithm_only:
                if i_algorithm_id != algorithm_id:
                    continue
            if algorithm != 'all':
                if not algorithms_report:
                    if i_algorithm_id != algorithm_id:
                        if compare_algorithm_group:
                            if compare_algorithm_group != algorithm_group_id:
                                continue
                        else:
                            continue
                if not consensuses_report:
                    if i_algorithm_id != algorithm_id:
                        if compare_algorithm_group:
                            if compare_algorithm_group != algorithm_group_id:
                                continue
                        else:
                            continue

            # @added 20230919 - Feature #5008: webapp - snab report page
            # Added the ability to exclude algorithms
            if i_algorithm in exclude_algorithms:
                continue

            report_data['algorithms'].append(i_algorithm)
            report_data['results'][i_algorithm] = {}
            report_data['results'][i_algorithm]['algorithm_id'] = i_algorithm_id

            report_data['results'][i_algorithm]['algorithm_group_name'] = algorithm_group_name
            report_data['results'][i_algorithm]['algorithm_group_id'] = algorithm_group_id
            total = 0
            total_evaluated = 0
            percent_evaluated = 0
            total_fP = 0
            percent_fP = 0
            total_tP = 0
            percent_tP = 0
            total_fN = 0
            percent_fN = 0
            total_tN = 0
            percent_tN = 0
            total_unsure = 0
            percent_unsure = 0
            runtimes = []
            for snab_id in snab_ids:
                if snab_records[snab_id]['algorithm_group_id'] != algorithm_group_id:
                    continue

                # @added 20230802 - Feature #5038: snab_results_algorithms
                #                   Feature #5008: webapp - snab report page
                snab_checked_snab_ids.append(int(snab_id))
                try:
                    runtimes.append(float(snab_records[snab_id]['runtime']))
                except:
                    pass
                total += 1
                if snab_records[snab_id]['fP']:
                    total_fP += 1
                    total_evaluated += 1
                    snab_checked_snab_ids_results[snab_id] = 'fP'
                    continue
                if snab_records[snab_id]['tP']:
                    total_tP += 1
                    total_evaluated += 1
                    snab_checked_snab_ids_results[snab_id] = 'tP'
                    continue
                if snab_records[snab_id]['fN']:
                    total_fN += 1
                    total_evaluated += 1
                    snab_checked_snab_ids_results[snab_id] = 'fN'
                    continue
                if snab_records[snab_id]['tN']:
                    total_tN += 1
                    total_evaluated += 1
                    snab_checked_snab_ids_results[snab_id] = 'tN'
                    continue
                if snab_records[snab_id]['unsure']:
                    total_unsure += 1
                    total_evaluated += 1
                    snab_checked_snab_ids_results[snab_id] = 'unsure'
                    continue
            report_data['results'][i_algorithm]['analysed'] = total
            report_data['results'][i_algorithm]['evaluated'] = total_evaluated
            if total and total_evaluated:
                percent_evaluated = round(((total_evaluated / total) * 100), 2)
            report_data['results'][i_algorithm]['evaluated %'] = percent_evaluated
            report_data['results'][i_algorithm]['tP'] = total_tP
            if total_tP and total_evaluated:
                percent_tP = round(((total_tP / total_evaluated) * 100), 2)
            report_data['results'][i_algorithm]['tP %'] = percent_tP
            report_data['results'][i_algorithm]['fP'] = total_fP
            if total_fP and total_evaluated:
                percent_fP = round(((total_fP / total_evaluated) * 100), 2)
            report_data['results'][i_algorithm]['fP %'] = percent_fP
            report_data['results'][i_algorithm]['tN'] = total_tN
            if total_tN and total_evaluated:
                percent_tN = round(((total_tN / total_evaluated) * 100), 2)
            report_data['results'][i_algorithm]['tN %'] = percent_tN
            report_data['results'][i_algorithm]['fN'] = total_fN
            percent_fN = 0
            if total_fN and total_evaluated:
                percent_fN = round(((total_fN / total_evaluated) * 100), 2)
            report_data['results'][i_algorithm]['fN %'] = percent_fN
            report_data['results'][i_algorithm]['unsure'] = total_unsure
            if total_unsure and total_evaluated:
                percent_unsure = round(((total_unsure / total_evaluated) * 100), 2)
            report_data['results'][i_algorithm]['unsure %'] = percent_unsure
            try:
                incorrect = (((total_fP + total_fN) / (total_evaluated - total_unsure)) * 100)
            except:
                incorrect = 0
            report_data['results'][i_algorithm]['incorrect % ((fP+fN)/(evaluated-unsure)*100)'] = round(incorrect, 2)
            try:
                accuracy = (((total_tP + total_tN) / (total_evaluated - total_unsure)) * 100)
            except:
                accuracy = 0
            report_data['results'][i_algorithm]['accuracy % ((tP+tN)/(evaluated-unsure)*100)'] = round(accuracy, 2)
            try:
                accuracy_fn_penalty = (((total_tP + total_tN) / (total_evaluated - total_unsure)) * 100) - percent_fN
            except:
                accuracy_fn_penalty = 0
            report_data['results'][i_algorithm]['fN penalty accuracy % ((tP+tN)/(evaluated-unsure)*100)-fN%'] = round(accuracy_fn_penalty, 2)

            # @added 20230915 - Feature #5070: snab - anomaly_detection_metrics
            ad_metrics = {
                'algorithm': i_algorithm, 'evaluated': total_evaluated, 'tP': total_tP, 'fP': total_fP,
                'tN': total_tN, 'fN': total_fN,
                'f1_score': None, 'precision': None, 'recall': None,
                'accuracy': None,  'fP_rate': None, 'fN_rate': None,
                # @added 20230916 - Feature #5070: snab - anomaly_detection_metrics
                #                   Feature #5008: webapp - snab report page
                # Added auc_roc
                'tP_rate': None, 'tN_rate': None, 'auc_roc': None,
            }
            #if i_algorithm == report_data['algorithm']:
            if i_algorithm == report_data['algorithm'] or algorithm == 'all':
                try:
                    ad_metrics = anomaly_detection_metrics(skyline_app, algorithm=i_algorithm, tP=total_tP, fP=total_fP, 
                                                        tN=total_tN, fN=total_fN,
                                                        evaluated=(total_evaluated - total_unsure))
                    if algorithm == 'all':
                        report_data['results'][i_algorithm]['anomaly_detection_metrics'] = ad_metrics

                except Exception as err:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    fail_msg = 'error :: get_snab_report :: anomaly_detection_metrics failed on algorithm: %s - %s' % (
                        i_algorithm, str(err))
                    logger.error(fail_msg)
                    if engine:
                        #try:
                        #    connection.close()
                        #except:
                        #    pass
                        snab_engine_disposal(engine)
                    raise
            if ad_metrics:
                for ad_key in ad_metric_keys:
                    report_data['results'][i_algorithm][ad_key] = ad_metrics[ad_key]

            # Original SNAB was not recording runtime so only calculate if
            # present 
            sum_runtimes = None
            try:
                sum_runtimes = sum(runtimes)
            except:
                sum_runtimes = None

            if sum_runtimes:
                avg_runtime = round((sum_runtimes / len(runtimes)), 2)
            else:
                avg_runtime = 'not recorded'
            report_data['results'][i_algorithm]['avg_runtime'] = avg_runtime
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_snab_report :: could not determine report_data - %s' % str(err)
            logger.error(fail_msg)
            if engine:
                #try:
                #    connection.close()
                #except:
                #    pass
                snab_engine_disposal(engine)
            raise

    if len(report_data['algorithms']) > 1:
        report_data['algorithms'] = list(set(report_data['algorithms']))
    if algorithm == 'all':
        report_data['use_algorithm_headers'] = list(report_data['results'].keys())[0]
        logger.info('get_snab_report :: sorting results by fN penalty, current order: %s' % (
            str(list(report_data['results'].keys()))))
        algos_fN_penalty = []
        for algo, algo_results in report_data['results'].items():
            algos_fN_penalty.append([algo, algo_results['fN penalty accuracy % ((tP+tN)/(evaluated-unsure)*100)-fN%']])
        sorted_results = {}
        sorted_algos_fN_penalty = sorted(algos_fN_penalty, key=lambda x: x[1], reverse=True)
        for algo, fN_penalty in sorted_algos_fN_penalty:
            sorted_results[algo] = copy.deepcopy(report_data['results'][algo])
            if 'anomaly_detection_metrics' in sorted_results[algo]:
                del sorted_results[algo]['anomaly_detection_metrics']
        report_data['results'] = sorted_results
        logger.info('get_snab_report :: sorted results by fN penalty to: %s' % (
            str(list(report_data['results'].keys()))))
        report_data['algorithms'] = list(sorted_results.keys())

    # @added 20230802 - Feature #5038: snab_results_algorithms
    #                   Feature #5008: webapp - snab report page
    # Add a summary of snab_algorithms_results reporting the number of tP, fP,
    # tN, fN per algorithm and consensus group
    snab_checked_snab_ids = list(set(snab_checked_snab_ids))
    algorithms_results = {}
    consensuses = {}
    algorithm_results = {}
    algorithm_group_id_results = {}
    logger.info('get_snab_report :: %s snab results to determine algorithm_results on' % str(len(snab_checked_snab_ids)))
    logger.info('get_snab_report :: %s snab_checked_snab_ids_results' % str(len(snab_checked_snab_ids_results)))

    remove_algo_keys = []

    #if snab_checked_snab_ids:
    if snab_checked_snab_ids and algorithm != 'all':
        try:
            algorithms_results = get_snab_algorithms_results(skyline_app, snab_id=0, snab_ids=snab_checked_snab_ids)
            logger.info('get_snab_report :: get_snab_algorithms_results got %s algorithms_results' % str(len(algorithms_results)))
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_snab_report :: get_snab_algorithms_results failed - %s' % str(err)
            logger.error(fail_msg)
            if engine:
                #try:
                #    connection.close()
                #except:
                #    pass
                snab_engine_disposal(engine)
            raise

        if algorithms_results:
            for snab_id in list(algorithms_results.keys()):
                algorithm_group_id = algorithms_results[snab_id]['algorithm_group_id']
                algorithm = algorithms_results[snab_id]['algorithm']
                snab_algorithm = str(algorithm)

                # @added 20230919 - Feature #5008: webapp - snab report page
                # Added the ability to exclude algorithms
                if snab_algorithm in exclude_algorithms:
                    continue

                snab_evaluated_result = False
                try:
                    snab_evaluated_result = snab_checked_snab_ids_results[snab_id]
                except:
                    snab_evaluated_result = False
                consensus_achieved = algorithms_results[snab_id]['consensus_achieved']
                consensus_achieved_str = None
                if consensus_achieved:
                    consensus_achieved_str = str(algorithms_results[snab_id]['consensus_achieved'])

                    # @added 20230919 - Feature #5008: webapp - snab report page
                    # Added the ability to exclude algorithms
                    skip = False
                    for ex_algorithm in exclude_algorithms:
                        if ex_algorithm in consensus_achieved:
                            skip = True
                            break
                    if skip:
                        continue

                    if consensus_achieved_str not in consensuses:
                        consensuses[consensus_achieved_str] = {
                            # 'algorithm_group_id': algorithm_group_id,
                            # 'algorithm': snab_algorithm,
                            'evaluated': 0,
                            'tP': 0, 'tP %': 0,
                            'fP': 0, 'fP %': 0,
                            'tN': 0, 'tN %': 0,
                            'fN': 0, 'fN %': 0,
                            'unsure': 0, 'unsure %': 0,
                        }
                if snab_evaluated_result and consensus_achieved_str:
                    consensuses[consensus_achieved_str]['evaluated'] += 1
                    consensuses[consensus_achieved_str][snab_evaluated_result] += 1

                for algorithms_results_id in algorithms_results[snab_id]['algorithms_results']:
                    algorithm_group_id = algorithms_results[snab_id]['algorithms_results'][algorithms_results_id]['algorithm_group_id']
                    algorithm = algorithms_results[snab_id]['algorithms_results'][algorithms_results_id]['algorithm']

                    # @added 20230919 - Feature #5008: webapp - snab report page
                    # Added the ability to exclude algorithms
                    if algorithm in exclude_algorithms:
                        continue

                    if algorithm_group_id not in algorithm_group_id_results:
                        algorithm_group_id_results[algorithm_group_id] = {
                            # 'algorithm_group_id': algorithm_group_id,
                            'algorithm': algorithm,
                            'evaluated': 0,
                            'tP': 0, 'tP %': 0,
                            'fP': 0, 'fP %': 0,
                            'tN': 0, 'tN %': 0,
                            'fN': 0, 'fN %': 0,
                            'unsure': 0, 'unsure %': 0,
                        }
                    if algorithm not in algorithm_results:
                        algorithm_results[algorithm] = {
                            # 'algorithm_group_id': algorithm_group_id,
                            # 'algorithm': algorithm,
                            'evaluated': 0,
                            'tP': 0, 'tP %': 0,
                            'fP': 0, 'fP %': 0,
                            'tN': 0, 'tN %': 0,
                            'fN': 0, 'fN %': 0,
                            'unsure': 0, 'unsure %': 0,
                            'runtimes': [],
                        }
                    if snab_evaluated_result:
                        algorithm_results[algorithm]['evaluated'] += 1
                    anomalyScore = algorithms_results[snab_id]['algorithms_results'][algorithms_results_id]['anomalyScore']
                    runtime = algorithms_results[snab_id]['algorithms_results'][algorithms_results_id]['runtime']

                    if algorithm in ['single_value_anomaly']:
                        if snab_evaluated_result:
                            use_evaluated_result = 'tN'
                            in_consensus_achieved_str = False
                            if consensus_achieved_str:
                                if algorithm in consensus_achieved_str:
                                    in_consensus_achieved_str = True
                                    if anomalyScore and snab_evaluated_result == 'tP':
                                        use_evaluated_result = snab_evaluated_result
                                    if anomalyScore and snab_evaluated_result == 'fN':
                                        use_evaluated_result = snab_evaluated_result
                            algorithm_results[algorithm][use_evaluated_result] += 1
                            algorithm_results[algorithm]['runtimes'].append(runtime)
                            continue

                    if anomalyScore and snab_evaluated_result == 'fP':
                        algorithm_results[algorithm][snab_evaluated_result] += 1
                    if anomalyScore and snab_evaluated_result == 'tP':
                        algorithm_results[algorithm][snab_evaluated_result] += 1
                    if anomalyScore and snab_evaluated_result == 'fN':
                        # algorithm_results[algorithm][snab_evaluated_result] += 1
                        algorithm_results[algorithm]['tP'] += 1
                    if anomalyScore and snab_evaluated_result == 'tN':
                        # algorithm_results[algorithm][snab_evaluated_result] += 1
                        algorithm_results[algorithm]['fP'] += 1
                    if not anomalyScore and snab_evaluated_result == 'fP':
                        # algorithm_results[algorithm][snab_evaluated_result] += 1
                        algorithm_results[algorithm]['tN'] += 1
                    if not anomalyScore and snab_evaluated_result == 'tP':
                        # algorithm_results[algorithm][snab_evaluated_result] += 1
                        algorithm_results[algorithm]['fN'] += 1
                    if not anomalyScore and snab_evaluated_result == 'fN':
                        algorithm_results[algorithm][snab_evaluated_result] += 1
                    if not anomalyScore and snab_evaluated_result == 'tN':
                        algorithm_results[algorithm][snab_evaluated_result] += 1
                    if snab_evaluated_result == 'unsure':
                        algorithm_results[algorithm][snab_evaluated_result] += 1
                    algorithm_results[algorithm]['runtimes'].append(runtime)

            consensuses_keys = []
            for consensus in list(consensuses.keys()):
                evaluated = consensuses[consensus]['evaluated']

                # if evaluated == 0:
                    # del consensuses[consensus]
                    # continue

                tP = consensuses[consensus]['tP']
                fP = consensuses[consensus]['fP']
                tN = consensuses[consensus]['tN']
                fN = consensuses[consensus]['fN']
                unsure = consensuses[consensus]['unsure']
                if tP:
                    consensuses[consensus]['tP %'] = round(((tP / evaluated) * 100), 2)
                if fP:
                    consensuses[consensus]['fP %'] = round(((fP / evaluated) * 100), 2)
                if tN:
                    consensuses[consensus]['tN %'] = round(((tN / evaluated) * 100), 2)
                percent_fN = 0
                if fN:
                    percent_fN = round(((fN / evaluated) * 100), 2)
                    consensuses[consensus]['fN %'] = percent_fN
                if unsure:
                    consensuses[consensus]['unsure %'] = round(((unsure / evaluated) * 100), 2)
                try:
                    incorrect = (((fP + fN) / (evaluated - unsure)) * 100)
                except:
                    incorrect = 0
                consensuses[consensus]['incorrect % ((fP+fN)/(evaluated-unsure)*100)'] = round(incorrect, 2)
                try:
                    accuracy = (((tP + tN) / (evaluated - unsure)) * 100)
                except:
                    accuracy = 0
                consensuses[consensus]['accuracy % ((tP+tN)/(evaluated-unsure)*100)'] = round(accuracy, 2)
                try:
                    accuracy_fn_penalty = (((tP + tN) / (evaluated - unsure)) * 100) - percent_fN
                except:
                    accuracy_fn_penalty = 0
                consensuses[consensus]['fN penalty accuracy % ((tP+tN)/(evaluated-unsure)*100)-fN%'] = round(accuracy_fn_penalty, 2)

                # @added 20230915 - Feature #5070: snab - anomaly_detection_metrics
                ad_metrics = {
                    'consensus': consensus, 'evaluated': evaluated, 'tP': tP, 'fP': fP, 'tN': tN,
                    'fN': fN,
                    'f1_score': None, 'precision': None, 'recall': None,
                    'accuracy': None, 'fP_rate': None, 'fN_rate': None,
                    # @added 20230916 - Feature #5070: snab - anomaly_detection_metrics
                    #                   Feature #5008: webapp - snab report page
                    # Added auc_roc
                    'tP_rate': None, 'tN_rate': None, 'auc_roc': None,
                }
                try:
                    ad_metrics = anomaly_detection_metrics(skyline_app, algorithm=consensus, tP=tP, fP=fP, 
                                                        tN=tN, fN=fN,
                                                        evaluated=(evaluated - unsure))
                except Exception as err:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    fail_msg = 'error :: get_snab_report :: anomaly_detection_metrics failed on consensus: %s - %s' % (
                        consensus, str(err))
                    logger.error(fail_msg)
                    if engine:
                        #try:
                        #    connection.close()
                        #except:
                        #    pass
                        snab_engine_disposal(engine)
                    raise
                if ad_metrics:
                    for ad_key in ad_metric_keys:
                        consensuses[consensus][ad_key] = ad_metrics[ad_key]

                if not consensuses_keys:
                    consensuses_keys = list(consensuses[consensus].keys())

            algorithm_results_keys = []
            for algorithm in list(algorithm_results.keys()):
                evaluated = algorithm_results[algorithm]['evaluated']

                if evaluated == 0:
                    remove_algo_keys.append(algorithm)
                    # continue

                tP = algorithm_results[algorithm]['tP']
                fP = algorithm_results[algorithm]['fP']
                tN = algorithm_results[algorithm]['tN']
                fN = algorithm_results[algorithm]['fN']
                unsure = algorithm_results[algorithm]['unsure']
                if tP:
                    algorithm_results[algorithm]['tP %'] = round(((tP / evaluated) * 100), 2)
                if fP:
                    algorithm_results[algorithm]['fP %'] = round(((fP / evaluated) * 100), 2)
                if tN:
                    algorithm_results[algorithm]['tN %'] = round(((tN / evaluated) * 100), 2)
                percent_fN = 0
                if fN:
                    percent_fN = round(((fN / evaluated) * 100), 2)
                    algorithm_results[algorithm]['fN %'] = percent_fN
                if unsure:
                    algorithm_results[algorithm]['unsure %'] = round(((unsure / evaluated) * 100), 2)
                try:
                    incorrect = (((fP + fN) / (evaluated - unsure)) * 100)
                except:
                    incorrect = 0
                algorithm_results[algorithm]['incorrect % ((fP+fN)/(evaluated-unsure)*100)'] = round(incorrect, 2)
                try:
                    accuracy = (((tP + tN) / (evaluated - unsure)) * 100)
                except:
                    accuracy = 0
                algorithm_results[algorithm]['accuracy % ((tP+tN)/(evaluated-unsure)*100)'] = round(accuracy, 2)
                try:
                    accuracy_fn_penalty = (((tP + tN) / (evaluated - unsure)) * 100) - percent_fN
                except:
                    accuracy_fn_penalty = 0
                algorithm_results[algorithm]['fN penalty accuracy % ((tP+tN)/(evaluated-unsure)*100)-fN%'] = round(accuracy_fn_penalty, 2)

                # @added 20230915 - Feature #5070: snab - anomaly_detection_metrics
                ad_metrics = {
                    'algorithm': algorithm, 'evaluated': evaluated, 'tP': tP, 'fP': fP, 'tN': tN,
                    'fN': fN,
                    'f1_score': None, 'precision': None, 'recall': None,
                    'accuracy': None, 'fP_rate': None, 'fN_rate': None,
                    # @added 20230916 - Feature #5070: snab - anomaly_detection_metrics
                    #                   Feature #5008: webapp - snab report page
                    # Added auc_roc
                    'tP_rate': None, 'tN_rate': None, 'auc_roc': None,
                }
                try:
                    ad_metrics = anomaly_detection_metrics(skyline_app, algorithm=algorithm, tP=tP, fP=fP, 
                                                        tN=tN, fN=fN,
                                                        evaluated=(evaluated - unsure))
                except Exception as err:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    fail_msg = 'error :: get_snab_report :: anomaly_detection_metrics failed on algorithm: %s - %s' % (
                        algorithm, str(err))
                    logger.error(fail_msg)
                    if engine:
                        #try:
                        #    connection.close()
                        #except:
                        #    pass
                        snab_engine_disposal(engine)
                    raise
                # @modified 20230920
                # Now evaluated == 0 are being removed only add if the consensus
                # has not been removed
                # if ad_metrics:
                #if ad_metrics and consensus in list(consensuses.keys()):
                #    consensuses[consensus]['anomaly_detection_metrics'] = ad_metrics
                if ad_metrics and consensus_achieved_str in list(consensuses.keys()):
                    consensuses[consensus_achieved_str]['anomaly_detection_metrics'] = ad_metrics
                    # algorithm_results[algorithm]
                if ad_metrics:
                    for ad_key in ad_metric_keys:
                        algorithm_results[algorithm][ad_key] = ad_metrics[ad_key]

                runtimes = list(algorithm_results[algorithm]['runtimes'])
                del algorithm_results[algorithm]['runtimes']
                algorithm_results[algorithm]['avg_runtime'] = round((sum(runtimes) / len(runtimes)), 4)
                if not algorithm_results_keys:
                    algorithm_results_keys = list(algorithm_results[algorithm].keys())

            report_data['consensuses'] = {}
            report_data['consensuses']['keys'] = consensuses_keys
            report_data['consensuses']['results'] = consensuses
            report_data['algorithm_results'] = {}
            report_data['algorithm_results']['keys'] = algorithm_results_keys
            report_data['algorithm_results']['results'] = algorithm_results

    remove_keys = False
    if remove_keys:
        remove_algo_keys = list(set(remove_algo_keys))
        for algo in list(set(remove_algo_keys)):
            try:
                del report_data['algorithm_results']['results'][algo]
            except:
                pass
        if 'algorithm_results' in report_data:
            if 'results' in report_data['algorithm_results']:
                if len(report_data['algorithm_results']['results']) == 0:
                    del report_data['algorithm_results']
        if 'consensuses' in report_data:
            if 'results' in report_data['consensuses']:
                if len(report_data['consensuses']['results']) == 0:
                    del report_data['consensuses']

    logger.info('get_snab_report :: report_data: %s' % str(report_data))

    # @added 20230915 - Feature #5008: webapp - snab report page
    # Add heatmap to snab report page
    coloured_key_strs = ['fP %', 'fN %', 'incorrect', 'accuracy'] + ad_metric_keys
    inverted_key_strs = ['fP %', 'fN %', 'incorrect', 'fP_rate', 'fN_rate']
    for algo in list(report_data['results'].keys()):
        for key in list(report_data['results'][algo].keys()):
            # if '%' in key and 'heatmap' not in key:
            if ('%' in key and 'heatmap' not in key) or (key in ad_metric_keys and 'heatmap' not in key):
                h_key = 'heatmap_html_color_code_%s' % key
                value = report_data['results'][algo][key]
                if value is not None:
                    original_value = float(value)
                else:
                    original_value = value
                coloured_key = False
                for key_str in coloured_key_strs:
                    if key_str in key:
                        coloured_key = True
                        break
                if not coloured_key:
                    continue
                if key in ad_metric_keys:
                    if not isinstance(value, float):
                        continue
                inverted_key_colour = False
                for key_str in inverted_key_strs:
                    if key_str in key:
                        inverted_key_colour = True
                        break
                if inverted_key_colour:
                    if key in ad_metric_keys:
                        value = 1 - value
                    else:
                        value = 100 - value
                if value < 25:
                    colour = hexcolor(lerp((0xff, 0x00, 0x00), (0xff, 0x80, 0x00), value / 25))
                elif value < 50:
                    colour = hexcolor(lerp((0xFF, 0x80, 0x00), (0xFF, 0xFF, 0x00), (value - 25) / 25))
                elif value < 75:
                    colour = hexcolor(lerp((0xFF, 0xFF, 0x00), (0x53, 0xd9, 0x26), (value - 50) / 25))
                else:
                    colour = hexcolor(lerp((0x53, 0xd9, 0x26), (0x00, 0xff, 0x00), (value - 75) / 25))
                if key in ad_metric_keys:
                    if value < 0.25:
                        colour = hexcolor(lerp((0xff, 0x00, 0x00), (0xff, 0x80, 0x00), (value * 100) / 25))
                    elif value < 0.50:
                        colour = hexcolor(lerp((0xFF, 0x80, 0x00), (0xFF, 0xFF, 0x00), ((value * 100) - 25) / 25))
                    elif value < 0.75:
                        colour = hexcolor(lerp((0xFF, 0xFF, 0x00), (0x53, 0xd9, 0x26), ((value * 100) - 50) / 25))
                    else:
                        colour = hexcolor(lerp((0x53, 0xd9, 0x26), (0x00, 0xff, 0x00), ((value * 100) - 75) / 25))
                    if key == 'auc_roc':
                        if original_value == 1:
                            colour = '#00FF00'
                if key == 'fN %':
                    if value > 98:
                        colour = '#00FF00'
                    elif value > 94:
                        colour = '#FFFF00'
                    elif value > 90:
                        colour = '#ffa500'
                    else:
                        colour = '#FF0000'
                report_data['results'][algo][h_key] = colour
    if 'algorithm_results' in list(report_data.keys()):
        for algo in list(report_data['algorithm_results']['results'].keys()):
            for key in list(report_data['algorithm_results']['results'][algo].keys()):
                # if '%' in key and 'heatmap' not in key:
                if ('%' in key and 'heatmap' not in key) or (key in ad_metric_keys and 'heatmap' not in key):
                    h_key = 'heatmap_html_color_code_%s' % key
                    value = report_data['algorithm_results']['results'][algo][key]
                    coloured_key = False
                    for key_str in coloured_key_strs:
                        if key_str in key:
                            coloured_key = True
                            break
                    if not coloured_key:
                        continue
                    if key in ad_metric_keys:
                        if not isinstance(value, float):
                            continue
                    inverted_key_colour = False
                    for key_str in inverted_key_strs:
                        if key_str in key:
                            inverted_key_colour = True
                            break
                    if inverted_key_colour:
                        if key in ad_metric_keys:
                            value = 1 - value
                        else:
                            value = 100 - value
                    if value < 25:
                        colour = hexcolor(lerp((0xff, 0x00, 0x00), (0xff, 0x80, 0x00), value / 25))
                    elif value < 50:
                        colour = hexcolor(lerp((0xFF, 0x80, 0x00), (0xFF, 0xFF, 0x00), (value - 25) / 25))
                    elif value < 75:
                        colour = hexcolor(lerp((0xFF, 0xFF, 0x00), (0x53, 0xd9, 0x26), (value - 50) / 25))
                    else:
                        colour = hexcolor(lerp((0x53, 0xd9, 0x26), (0x00, 0xff, 0x00), (value - 75) / 25))
                    if key in ad_metric_keys:
                        if value < 0.25:
                            colour = hexcolor(lerp((0xff, 0x00, 0x00), (0xff, 0x80, 0x00), (value * 100) / 25))
                        elif value < 0.50:
                            colour = hexcolor(lerp((0xFF, 0x80, 0x00), (0xFF, 0xFF, 0x00), ((value * 100) - 25) / 25))
                        elif value < 0.75:
                            colour = hexcolor(lerp((0xFF, 0xFF, 0x00), (0x53, 0xd9, 0x26), ((value * 100) - 50) / 25))
                        else:
                            colour = hexcolor(lerp((0x53, 0xd9, 0x26), (0x00, 0xff, 0x00), ((value * 100) - 75) / 25))
                    if key == 'fN %':
                        if value > 98:
                            colour = '#00FF00'
                        elif value > 94:
                            colour = '#FFFF00'
                        elif value > 90:
                            colour = '#ffa500'
                        else:
                            colour = '#FF0000'
                    report_data['algorithm_results']['results'][algo][h_key] = colour                
    if 'consensuses' in list(report_data.keys()):
        for consensus in list(report_data['consensuses']['results'].keys()):
            for key in list(report_data['consensuses']['results'][consensus].keys()):
                # if '%' in key and 'heatmap' not in key:
                if ('%' in key and 'heatmap' not in key) or (key in ad_metric_keys and 'heatmap' not in key):
                    h_key = 'heatmap_html_color_code_%s' % key
                    value = report_data['consensuses']['results'][consensus][key]
                    coloured_key = False
                    for key_str in coloured_key_strs:
                        if key_str in key:
                            coloured_key = True
                            break
                    if not coloured_key:
                        continue
                    if key in ad_metric_keys:
                        if not isinstance(value, float):
                            continue
                    inverted_key_colour = False
                    for key_str in inverted_key_strs:
                        if key_str in key:
                            inverted_key_colour = True
                            break
                    if inverted_key_colour:
                        if key in ad_metric_keys:
                            value = 1 - value
                        else:
                            value = 100 - value
                    if value < 25:
                        colour = hexcolor(lerp((0xff, 0x00, 0x00), (0xff, 0x80, 0x00), value / 25))
                    elif value < 50:
                        colour = hexcolor(lerp((0xFF, 0x80, 0x00), (0xFF, 0xFF, 0x00), (value - 25) / 25))
                    elif value < 75:
                        colour = hexcolor(lerp((0xFF, 0xFF, 0x00), (0x53, 0xd9, 0x26), (value - 50) / 25))
                    else:
                        colour = hexcolor(lerp((0x53, 0xd9, 0x26), (0x00, 0xff, 0x00), (value - 75) / 25))
                    if key in ad_metric_keys:
                        if value < 0.25:
                            colour = hexcolor(lerp((0xff, 0x00, 0x00), (0xff, 0x80, 0x00), (value * 100) / 25))
                        elif value < 0.50:
                            colour = hexcolor(lerp((0xFF, 0x80, 0x00), (0xFF, 0xFF, 0x00), ((value * 100) - 25) / 25))
                        elif value < 0.75:
                            colour = hexcolor(lerp((0xFF, 0xFF, 0x00), (0x53, 0xd9, 0x26), ((value * 100) - 50) / 25))
                        else:
                            colour = hexcolor(lerp((0x53, 0xd9, 0x26), (0x00, 0xff, 0x00), ((value * 100) - 75) / 25))
                    if key == 'fN %':
                        if value > 98:
                            colour = '#00FF00'
                        elif value > 94:
                            colour = '#FFFF00'
                        elif value > 90:
                            colour = '#ffa500'
                        else:
                            colour = '#FF0000'
                    report_data['consensuses']['results'][consensus][h_key] = colour

    #if connection:
    #    try:
    #        connection.close()
    #    except:
    #        pass

    if engine:
        snab_engine_disposal(engine)

    logger.info('get_snab_report :: final report_data: %s' % str(report_data))

    return report_data
