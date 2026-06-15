"""
snab_results_algorithms.py
"""
import logging
import traceback

from functions.database.queries.get_algorithms import get_algorithms
from functions.database.queries.get_snab_result import get_snab_result
from functions.database.queries.insert_new_algorithm import insert_new_algorithm
from functions.database.queries.insert_snab_results_algorithms import insert_snab_results_algorithms

skyline_app = 'snab'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20230729 - Feature #5038: snab_results_algorithms
#                   Feature #4988: Allow snab to return and save results
# Save the results for each algorithm in the snab_algorithm_results table
def snab_results_algorithms(results):

    function_str = 'snab_results_algorithms'
    inserted_results = {}

    if 'snab_id' not in results:
        logger.info('%s :: no snab_id in results, nothing to add')
        return inserted_results
    if 'anomaly_id' not in results:
        logger.info('%s :: no anomaly_id in results, nothing to add')
        return inserted_results
    
    snab_id = results['snab_id']
    anomaly_id = results['anomaly_id']

    # @added 20250112 - Feature #5588: snab.process_algorithm
    snab_process_algorithm = False
    if 'snab_process_algorithm' in results:
        snab_process_algorithm = results['snab_process_algorithm']

    snab_result = {}
    try:
        snab_result = get_snab_result(skyline_app, anomaly_id)
    except Exception as err:
        logger.error('error :: %s :: get_snab_result failed for anomaly_id %s - %s' % (
            function_str, str(anomaly_id), str(err)))
    if snab_result:
        logger.info('%s :: got snab_result for anomaly id: %s' % (
            function_str, str(anomaly_id)))

    for i_snab_id in list(snab_result.keys()):
        if i_snab_id != snab_id:
            logger.info('%s :: removing %s snab_id from snab_result: %s' % (
                function_str, str(i_snab_id), str(snab_result[i_snab_id])))
            del snab_result[i_snab_id]
    if not snab_result:
        logger.info('%s :: no snab_result, nothing to process' % function_str)
        return inserted_results
    else:
        logger.info('%s :: adding results for snab_id %s - %s' % (
            function_str, str(snab_id), str(snab_result)))

    algorithms_with_results = []
    if 'algorithms' in results:
        algorithms_with_results = list(results['algorithms'].keys())
        logger.info('%s :: adding results to snab_algorithm_results for %s algorithms' % (
            function_str, str(len(algorithms_with_results))))

    if not algorithms_with_results:
        logger.info('%s :: nothing to add to snab_algorithm_results from the results' % function_str)
        return inserted_results

    # Although Redis data could be surfaced for these and the DB only queried
    # if not found in Redis data, a DB query for each snab check is not
    # excessive
    known_algorithms = {}
    try:
        known_algorithms = get_algorithms(skyline_app)
        # known_algorithms dict example
        # {'histogram_bins': 1, 'first_hour_average': 2, ..., 'irregular_unstable': 253}
    except Exception as err:
        logger.error('error :: %s :: get_algorithms failed - %s' % (
            function_str, str(err)))

    unknown_algorithms = []
    known_algos = list(known_algorithms.keys())
    for algo in algorithms_with_results:
        unknown_algo = False
        use_algo = str(algo)
        if algo.startswith('skyline_'):
            use_algo = algo.lstrip('skyline_')
        #if algo not in list(known_algorithms.keys()):
        #    unknown_algorithms.append(algo)
        if algo not in known_algos or use_algo not in known_algos:
            unknown_algorithms.append(use_algo)

    inserted_new_algorithms = {}
    if unknown_algorithms:
        for algo in unknown_algorithms:
            new_algorithm_id = None
            new_algorithm_group_id = None
            try:
                # @modified 20250112 - Feature #5588: snab.process_algorithm
                #new_algorithm_id = insert_new_algorithm(skyline_app, algo)
                new_algorithm_id, new_algorithm_group_id = insert_new_algorithm(skyline_app, algo)
            except Exception as err:
                logger.error('error :: %s :: insert_new_algorithm failed - %s' % (
                    function_str, str(err)))
            if new_algorithm_id:
                known_algorithms[algo] = int(new_algorithm_id)
                inserted_new_algorithms[algo] = int(new_algorithm_id)
    if inserted_new_algorithms:
        logger.info('%s :: newly inserted algorithms - %s' % (
            function_str, str(inserted_new_algorithms)))

    algorithm_group_id = None
    try:
        algorithm_group_id = snab_result[snab_id]['algorithm_group_id']
    except:
        logger.info('%s :: algorithm_group_id unknown cannot add to snab_algorithm_results' % function_str)
        return inserted_results

    consensus_reached = []
    try:
        consensus_reached = results['consensus_reached']
    except Exception as err:
        logger.info('warning :: %s :: failed to determine consensus_reached from the results, err: %s' % (
            function_str, str(err)))
        consensus_reached = []

    consensus_achieved_list = []
    for algo in consensus_reached:
        algo_id = None

        # @added 20251108 - Feature #5655: custom_algorithm - skyline_tsb_uad_pca
        #                   Feature #5656: custom_algorithm - skyline_tsb_uad_ocsvm
        if algo.startswith('skyline_tsb_uad'):
            algo = algo.replace('skyline_tsb_uad', 'tsb_uad')

        try:
            algo_id = known_algorithms[algo]
        except:
            logger.error('error :: %s :: cannot update snab_results_algorithms for %s as no algorithm_id is known' % (
                function_str, str(algo)))
            continue
        consensus_achieved_list.append(algo_id)
    consensus_achieved = None
    if consensus_achieved_list:
        consensus_achieved_list = sorted(consensus_achieved_list)
        for i in consensus_achieved_list:
            if consensus_achieved:
                consensus_achieved = '%s,%s' % (consensus_achieved, str(i))
            else:
                consensus_achieved = str(i)

    for algo in algorithms_with_results:
        algo_id = None

        # @added 20251108 - Feature #5655: custom_algorithm - skyline_tsb_uad_pca
        #                   Feature #5656: custom_algorithm - skyline_tsb_uad_ocsvm
        try_algo = str(algo)
        if algo.startswith('skyline_tsb_uad'):
            try_algo = algo.replace('skyline_tsb_uad', 'tsb_uad')

        try:
            # algo_id = known_algorithms[algo]
            algo_id = known_algorithms[try_algo]
        except:
            logger.error('error :: %s :: cannot update snab_results_algorithms for %s as no algorithm_id is known' % (
                function_str, str(algo)))
            continue
        anomalyScore = 0
        try:
            anomalous = results['algorithms'][algo]['anomalous']
            if anomalous:
                anomalyScore = 1
        except:
            logger.info('warning :: %s :: cannot update snab_results_algorithms for %s as anomalous is not known' % (
                function_str, str(algo)))
            continue
        runtime = 0.0
        try:
            runtime = results['algorithms'][algo]['analysis_runtime']
        except:
            runtime = 0.0
        # snab_id,algorithm_group_id,algorithm_id,anomalyScore,runtime,consensus_achieved
        data = {
            'snab_id': snab_id,
            'algorithm_group_id': algorithm_group_id,
            'algorithm_id': algo_id,
            'anomalyScore': anomalyScore,
            'runtime': runtime,
            'consensus_achieved': consensus_achieved,
        }
        new_row = {}
        try:
            new_row = insert_snab_results_algorithms(skyline_app, data)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: insert_snab_results_algorithms failed with data: %s - %s' % (
                function_str, str(data), err))
            continue
        if new_row:
            if snab_id in list(inserted_results.keys()):
                inserted_results[snab_id][algo_id] = dict(new_row)
            else:
                inserted_results[snab_id] = {}
                inserted_results[snab_id][algo_id] = dict(new_row)

    logger.info('%s :: added %s results to snab_algorithm_results' % (
        function_str, str(len(inserted_results))))

    return inserted_results
