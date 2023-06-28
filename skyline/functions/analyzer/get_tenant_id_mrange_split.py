"""
get_tenant_id_mrange_split.py
"""
import logging
from time import time

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @modified 20230404 - Feature #4890: analyzer_labelled_metrics - use mrange
def get_tenant_id_mrange_split(self, number_of_processes, tenant_ids_with_count):
    """

    Create the filters_dict with a list of tenant_ids for each process to pass
    to Redis timeseries mrange.

    :param self: the self object
    :param number_of_processes: the number of processes that will be used
    :param tenant_ids_with_count: the number of metrics per tenant_id
    :type self: object
    :type number_of_processes: int
    :type tenant_ids_with_count: dict
    :return: filters_dict
    :rtype: dict

    """

    logger.info('analyzer_labelled_metrics :: get_tenant_id_mrange_split :: determining filters_list')

    # data = {
    #     'a': 2179, 'b': 2452, 'c': 321, 'd': 1232, 'e': 23, 'f': 2, 'g': 5679,
    #     'h': 12179, 'i': 452, 'j': 44321, 'k': 54, 'l': 15008, 'm': 1, 'n': 987,
    # }

    # number_of_processes = 2

    # Calculate total count and target count for each list
    total_count = sum(tenant_ids_with_count.values())
    target_count = total_count // number_of_processes

    # Sort keys by descending count
    #sorted_keys = sorted(tenant_ids_with_count.keys(), key=tenant_ids_with_count.get, reverse=True)
    # Sort keys by ascending count not descending count which can result in one
    # list being assigned no keys
    sorted_keys = sorted(tenant_ids_with_count.keys(), key=tenant_ids_with_count.get)

    filters_dict = {}
    filters_dict_sums = {}
    for i in range(number_of_processes):
        filters_dict[i] = []
        filters_dict_sums[i] = []

    # Iterate over sorted keys and distribute them into the lists
    current_count = 0
    current_list = 0
    for key in sorted_keys:
        count = tenant_ids_with_count[key]
        # print(current_count, (current_count + count), key, count, current_list)
        if current_count + count <= target_count * (int(current_list) + 1):
            # print('current_count + count <= target_count * (current_list + 1)', (current_count + count), current_list)
            # print(result, current_list)
            filters_dict[current_list].append(key)
            current_count += count
            filters_dict_sums[current_list].append(count)
            # print(result, current_list)
        else:
            current_list += 1
            if current_list >= number_of_processes:
                filters_dict[number_of_processes-1].append(key)
                filters_dict_sums[number_of_processes-1].append(count)
            else:
                filters_dict[current_list].append(key)
                filters_dict_sums[current_list].append(count)
            current_count = count
            # print(result, current_list)
        # print(result_sums, current_list)

    logger.info('analyzer_labelled_metrics :: get_tenant_id_mrange_split :: filters_dict_sums: %s' % str(filters_dict_sums))
    filters_dict_sums_sum = {k:sum(v) for k,v in filters_dict_sums.items()}
    logger.info('analyzer_labelled_metrics :: get_tenant_id_mrange_split :: filters_dict_sums_sum: %s' % str(filters_dict_sums_sum))
    logger.info('analyzer_labelled_metrics :: get_tenant_id_mrange_split :: filters_dict: %s' % str(filters_dict))
    return filters_dict
