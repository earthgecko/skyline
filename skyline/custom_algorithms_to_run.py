import logging
import traceback
from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20200603 - Feature #3566: custom_algorithms
def get_custom_algorithms_to_run(current_skyline_app, base_name, custom_algorithms, debug):
    """
    Return a dictionary of custom algoritms to run on a metric determined from
    the :mod:`settings.CUSTOM_ALGORITHMS` dictionary.
    """
    if debug:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    custom_algorithms_to_run = {}
    for custom_algorithm in list(custom_algorithms.keys()):
        use_with_current_skyline_app = False
        try:
            use_with = custom_algorithms[custom_algorithm]['use_with']
            if current_skyline_app in use_with:
                use_with_current_skyline_app = True
        except:
            use_with_current_skyline_app = None
        if not use_with_current_skyline_app:
            continue
        namespaces = []
        try:
            namespaces = custom_algorithms[custom_algorithm]['namespaces']
        except:
            namespaces = []
        algorithm_source = None
        consensus = None
        algorithms_allowed_in_consensus = []
        run_custom_algorithm = False
        max_execution_time = None
        # @added 20201119 - Feature #3566: custom_algorithms
        #                   Task #3744: POC matrixprofile
        # Added missing run_3sigma_algorithms
        run_3sigma_algorithms = True

        if namespaces:
            for namespace in namespaces:
                if not run_custom_algorithm:
                    try:
                        run_custom_algorithm, run_custom_algorithm_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, [namespace])
                    except:
                        pass
                    if run_custom_algorithm:
                        if debug:
                            current_logger.debug('debug :: get_custom_algorithms_to_run :: %s - namespace - %s, run_custom_algorithm_matched_by %s' % (
                                base_name, str(namespace), str(run_custom_algorithm_matched_by)))
                        break
        if run_custom_algorithm:
            try:
                algorithm_source = custom_algorithms[custom_algorithm]['algorithm_source']
            except:
                algorithm_source = None
            try:
                algorithm_parameters = custom_algorithms[custom_algorithm]['algorithm_parameters']
            except:
                algorithm_parameters = {}
            try:
                max_execution_time = custom_algorithms[custom_algorithm]['max_execution_time']
            except:
                max_execution_time = 0.05
            try:
                consensus = int(custom_algorithms[custom_algorithm]['consensus'])
            except:
                consensus = 0
            try:
                algorithms_allowed_in_consensus = custom_algorithms[custom_algorithm]['algorithms_allowed_in_consensus']
            except:
                algorithms_allowed_in_consensus = []
            try:
                debug_logging = custom_algorithms[custom_algorithm]['debug_logging']
            except:
                debug_logging = False
            # @added 20201119 - Feature #3566: custom_algorithms
            #                   Task #3744: POC matrixprofile
            # Added missing run_3sigma_algorithms and use_with parameters
            try:
                run_3sigma_algorithms = custom_algorithms[custom_algorithm]['run_3sigma_algorithms']
            except:
                run_3sigma_algorithms = True

            # @added 20201125 - Feature #3848: custom_algorithms - run_before_3sigma parameter
            try:
                run_before_3sigma = custom_algorithms[custom_algorithm]['run_before_3sigma']
            except:
                run_before_3sigma = True
            try:
                run_only_if_consensus = custom_algorithms[custom_algorithm]['run_only_if_consensus']
            except:
                run_only_if_consensus = False

            try:
                use_with = custom_algorithms[custom_algorithm]['use_with']
            except:
                use_with = []
            use_with_app = False
            for app in use_with:
                if app == current_skyline_app:
                    use_with_app = True

            if debug:
                current_logger.debug('debug :: get_custom_algorithms_to_run :: %s - custom_algorithm - %s, max_execution_time - %s' % (
                    base_name, str(custom_algorithm), str(max_execution_time)))

        # @modified 20201119 - Feature #3566: custom_algorithms
        #                      Task #3744: POC matrixprofile
        # Added missing run_3sigma_algorithms and use_with parameters
        # if run_custom_algorithm and algorithm_source:
        if run_custom_algorithm and algorithm_source and use_with_app:
            try:
                custom_algorithms_to_run[custom_algorithm] = {
                    'namespaces': namespaces,
                    'algorithm_source': algorithm_source,
                    'algorithm_parameters': algorithm_parameters,
                    'max_execution_time': max_execution_time,
                    'consensus': consensus,
                    'algorithms_allowed_in_consensus': algorithms_allowed_in_consensus,
                    'debug_logging': debug_logging,
                    # @added 20201119 - Feature #3566: custom_algorithms
                    #                   Task #3744: POC matrixprofile
                    # Added missing run_3sigma_algorithms
                    'run_3sigma_algorithms': run_3sigma_algorithms,
                    # @added 20201125 - Feature #3848: custom_algorithms - run_before_3sigma parameter
                    'run_before_3sigma': run_before_3sigma,
                    'run_only_if_consensus': run_only_if_consensus,
                    'use_with': use_with,
                }
                if debug:
                    current_logger.debug('debug :: get_custom_algorithms_to_run :: %s - custom_algorithms_to_run - %s' % (
                        base_name, str(custom_algorithms_to_run)))
            except:
                if debug:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: get_custom_algorithms_to_run :: failed to create dict for %s' % (
                        base_name))
                else:
                    pass
    return custom_algorithms_to_run
