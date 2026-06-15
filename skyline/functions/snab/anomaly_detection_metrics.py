"""
anomaly_detection_metrics.py
"""

import logging

# @added 20230915 - Feature #5070: snab - anomaly_detection_metrics
#                   Feature #5008: webapp - snab report page
# Add recall, precision and F1 to snab report
# @julia.bohutska - https://towardsdatascience.com/anomaly-detection-how-to-tell-good-performance-from-bad-b57116d71a10
# recall = tP/(tP + fN)
# precision = tP /(tP + fP)
# f1_score = 2 * (recall * precision) / (precision + recall)
# fP_rate = fP / (tN + fP)
# fN_rate = fN / (tP + fN)
# Unfortunately Julia does not describe the accuracy formula :)
# https://medium.com/@katser/a-review-of-anomaly-detection-metrics-with-a-lot-of-related-information-736d88774712
# Accuracy: this is the ratio of correctly classified data points (or change points)
# # to total data points (change points). Refer to this paper for more details.
# https://link.springer.com/article/10.1007/s10115-016-0987-z
# A survey of methods for time series change point detection
# Published: 08 September 2016
# Samaneh Aminikhanghahi & Diane J. Cook1 
# accuracy = tP / (tP + fN)
# tP_rate = tP / (tP + fN)
# tN_rate = tN / (fP + tN)
# auc_roc = (((tP_rate * fP_rate) / 2) + (tP_rate * (1 - fP_rate))) + (((1 - tP_rate) * (1 - fP_rate)) / 2)

# tsad does a NAB metric - https://github.com/waico/tsad/blob/main/tsad/utils/evaluating/evaluating.py
def anomaly_detection_metrics(current_skyline_app, algorithm=None, tP=None, fP=None, tN=None, fN=None, evaluated=None):
    """
    Calculate recall, precision, f1_score, fP_rate, fN_rate and accuracy

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param algorithm: the algorithm metrics are being calculated for
    :param tP: the number of True Positives
    :param fP: the number of False Positives
    :param tN: the number of True Negatives
    :param fN: the number of False Positives
    :param evaluated: the number of evaluations 
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type algorithm: str
    :type tP: int
    :type fP: int
    :type tN: int
    :type fN: int
    :type evaluated: int
    :type log: boolean
    :return: results
    :rtype:  dict
    """
    function_str = 'functions.snab.anomaly_detection_metrics'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    recall = None
    precision = None
    fP_rate = None
    fN_rate = None
    accuracy = None
    results = {
        'algorithm': str(algorithm), 'evaluated': evaluated, 'tP': tP, 'fP': fP,
        'tN': tN, 'fN': fN,
        'f1_score': None, 'precision': precision, 'recall': recall,
        'accuracy': None, 'fP_rate': None, 'fN_rate': None,
    }

    # @added 20230916 - Feature #5070: snab - anomaly_detection_metrics
    #                   Feature #5008: webapp - snab report page
    # Added auc_roc along with tP_rate and tN_rate
    tP_rate = None
    tN_rate = None
    auc_roc = None
    results['tP_rate'] = tP_rate
    results['tN_rate'] = tN_rate
    results['auc_roc'] = auc_roc

    current_logger.info('%s :: calculating anomaly_detection_metrics for: %s' % (
        function_str, str(results)))
    if isinstance(tP, int):
        if tP == 0:
            recall = 0.0
            results['recall'] = round(recall, 4)
            current_logger.info('%s :: recall: %s for: %s' % (
                function_str, str(recall), str(algorithm)))
        if isinstance(fN, int) and tP != 0:
            recall = tP / (tP + fN)
            results['recall'] = round(recall, 4)
            current_logger.info('%s :: recall: %s for: %s' % (
                function_str, str(recall), str(algorithm)))
    if isinstance(tP, int):
        if tP == 0:
            precision = 0.0
            results['precision'] = round(precision, 4)
            current_logger.info('%s :: precision: %s for: %s' % (
                function_str, str(precision), str(algorithm)))
        if isinstance(fP, int) and tP != 0:
            precision = tP / (tP + fP)
            results['precision'] = round(precision, 4)
            current_logger.info('%s :: precision: %s for: %s' % (
                function_str, str(precision), str(algorithm)))
    if isinstance(precision, float) and isinstance(recall, float):
        if precision == 0 and recall == 0:
            f1_score = 0.0
        else:
            f1_score = 2 * (recall * precision) / (precision + recall)
        results['f1_score'] = round(f1_score, 4)
        current_logger.info('%s :: f1_score: %s for: %s' % (
            function_str, str(f1_score), str(algorithm)))
    if isinstance(fP, int):
        # @modified 20230916 - Feature #5070: snab - anomaly_detection_metrics
        #                      Feature #5008: webapp - snab report page
        # Corrected incorrect interpretation of fP_rate and fN_rate based on
        # https://machinelearningmastery.com/roc-curves-and-precision-recall-curves-for-imbalanced-classification/
        # if isinstance(tP, int) and fP != 0:
            # fP_rate = fP / (fP + tP)
        if isinstance(tN, int) and fP != 0:
            fP_rate = fP / (tN + fP)
            results['fP_rate'] = round(fP_rate, 4)
        if isinstance(fP, int):
            if fP == 0:
                fP_rate = 0.0
                results['fP_rate'] = round(fP_rate, 4)
        current_logger.info('%s :: fP_rate: %s for: %s' % (
            function_str, str(fP_rate), str(algorithm)))
    if isinstance(fN, int):
        if isinstance(tP, int):
            try:
                fN_rate = round((fN / (tP + fN)), 4)
            except Exception as err:
                current_logger.info('%s :: cannot calculate fN_rate for: %s, %s / (%s + %s)' % (
                    function_str, str(algorithm), str(fN), str(fN), str(tP)))
                fN_rate = None
            results['fN_rate'] = fN_rate
        if isinstance(fN, int):
            if fN == 0:
                fN_rate = 0.0
                results['fN_rate'] = round(fP_rate, 4)
        current_logger.info('%s :: fN_rate: %s for: %s' % (
            function_str, str(fN_rate), str(algorithm)))
    if isinstance(tP, int):
        if isinstance(fN, int):
            if (tP + fN) != 0 and tP != 0:
                # accuracy = round((tP / (tP + fN)), 4)
                accuracy = round(((tP + tN) / evaluated), 4)
            else:
                current_logger.info('%s :: cannot calculate accuracy for: %s, %s / (%s + %s)' % (
                    function_str, str(algorithm), str(tP), str(tP), str(fN)))
                accuracy = 0.0
            results['accuracy'] = accuracy
            current_logger.info('%s :: accuracy: %s for: %s' % (
                function_str, str(accuracy), str(algorithm)))

    # @added 20230916 - Feature #5070: snab - anomaly_detection_metrics
    #                   Feature #5008: webapp - snab report page
    # Added auc_roc
    # Is It Worth It? Comparing Six Deep and Classical Methods for Unsupervised Anomaly Detection in Time Series
    # https://www.mdpi.com/2076-3417/13/3/1778/pdf
    # Citation: Rewicki, F.; Denzler, J.;Niebling, J.
    # # Is It Worth It? Comparing Six Deep and Classical Methods for Unsupervised Anomaly Detection in Time Series.
    # # Appl. Sci. 2023, 13, 1778.
    # https://doi.org/10.3390/app13031778
    # AUC ROC
    # The AUC ROC is a measure of the ability of a binary classifier to separate two classes
    # and can be seen as a single-number summary of a ROC plot [ 48 ]. In a ROC plot, the
    # true-positive rate is plotted against the false-positive rate at increasing threshold levels for
    # thresholding the output of the classifier. The higher the AUC ROC, the more easily the
    # classifier can separate the two classes. A perfect classifier achieves a score of one by ranking
    # all examples of the positive class higher than all examples of the negative class. Therefore,
    # we use the AUC ROC as a measure of the quality of the produced anomaly scores: a high
    # score indicates good separability between normal and anomalous points or subsequences.
    # We are aware that the AUC ROC is not a suitable measure for unbalanced problems such
    # as anomaly detection, where the anomalous class is small by definition, but we report it
    # due to its widespread use in the literature.
    # https://dasha.ai/en-us/blog/auc-roc
    # https://www.analyticsvidhya.com/blog/2020/06/auc-roc-curve-machine-learning/
    # A Comparative Study on Unsupervised Anomaly Detection for Time Series: Experiments and Analysis - https://arxiv.org/pdf/2209.04635.pdf
    # https://machinelearningmastery.com/roc-curves-and-precision-recall-curves-for-imbalanced-classification/
    # Added tP_rate for auc_roc
    if isinstance(tP, int):
        if isinstance(fN, int) and tP != 0:
            tP_rate = tP / (tP + fN)
            results['tP_rate'] = round(tP_rate, 4)
        if isinstance(tP, int):
            if tP == 0:
                tP_rate = 0.0
                results['tP_rate'] = tP_rate
        current_logger.info('%s :: tP_rate: %s for: %s' % (
            function_str, str(tP_rate), str(algorithm)))
    # Added tN_rate
    if isinstance(tN, int):
        if isinstance(fP, int) and tN != 0:
            tN_rate = tN / (fP + tN)
            results['tN_rate'] = round(tN_rate, 4)
        if isinstance(tN, int):
            if tN == 0:
                tN_rate = 0.0
                results['tN_rate'] = tN_rate
        current_logger.info('%s :: tN_rate: %s for: %s' % (
            function_str, str(tN_rate), str(algorithm)))
    # Added auc_roc
    if tP_rate and fP_rate:
        try:
            auc_roc = (((tP_rate * fP_rate) / 2) + (tP_rate * (1 - fP_rate))) + (((1 - tP_rate) * (1 - fP_rate)) / 2)
            results['auc_roc'] = round(auc_roc, 4)
        except Exception as err:
            current_logger.info('%s :: cannot calculate auc_roc for: %s' % (
                function_str, str(algorithm)))
            auc_roc = None
    if not isinstance(auc_roc, float):
        if tP_rate == 1 and fP_rate == 0:
            auc_roc = 1.0
            results['auc_roc'] = round(auc_roc, 4)
    current_logger.info('%s :: %s results: %s' % (
        function_str, str(algorithm), str(results)))
    # TODO add auc_pr
    # Tried to also add PR AUC but... there is not a simple formula for that.
    # Altough there is a @from sklearn.metrics import precision_recall_curve, auc@
    # method, the current data does not shoehorn into it.

    return results
