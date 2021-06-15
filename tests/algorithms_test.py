import unittest2 as unittest
from mock import Mock, patch
from time import time
import os.path
import sys

# from skyline.analyzer import algorithms
# from skyline import settings

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.join(os.path.dirname(os.path.realpath(current_dir)))
skyline_dir = parent_dir + '/skyline'
sys.path.append(skyline_dir)

if True:
    from analyzer import algorithms
    import settings


class TestAlgorithms(unittest.TestCase):
    """
    Test all algorithms with a common, simple/known anomalous data set
    """

    def _addSkip(self, test, tail_avg, reason):
        # @modified 20200808 - Bug #3666: Failing algorithm_tests on Python 3.8.3
        # print reason
        print(reason)

    def data(self, ts):
        """
        Mostly ones (1), with a final value of 1000
        """
        # @modified 20200808 - Bug #3666: Failing algorithm_tests on Python 3.8.3
        # timeseries = map(list, zip(map(float, range(int(ts) - 86400, int(ts) + 1)), [1] * 86401))
        timeseries = list(map(list, zip(map(float, range(int(ts) - 86400, int(ts) + 1)), [1] * 86401)))
        timeseries[-1][1] = 1000
        timeseries[-2][1] = 1
        timeseries[-3][1] = 1
        return ts, timeseries

    def test_tail_avg(self):
        _, timeseries = self.data(time())
        self.assertEqual(algorithms.tail_avg(timeseries), 334)

    def test_grubbs(self):
        _, timeseries = self.data(time())
        self.assertTrue(algorithms.grubbs(timeseries))

    @patch.object(algorithms, 'time')
    def test_first_hour_average(self, timeMock):
        timeMock.return_value, timeseries = self.data(time())
        self.assertTrue(algorithms.first_hour_average(timeseries))

    def test_stddev_from_average(self):
        _, timeseries = self.data(time())
        self.assertTrue(algorithms.stddev_from_average(timeseries))

    def test_stddev_from_moving_average(self):
        _, timeseries = self.data(time())
        self.assertTrue(algorithms.stddev_from_moving_average(timeseries))

    def test_mean_subtraction_cumulation(self):
        _, timeseries = self.data(time())
        self.assertTrue(algorithms.mean_subtraction_cumulation(timeseries))

    @patch.object(algorithms, 'time')
    def test_least_squares(self, timeMock):
        timeMock.return_value, timeseries = self.data(time())
        self.assertTrue(algorithms.least_squares(timeseries))

    def test_histogram_bins(self):
        _, timeseries = self.data(time())
        self.assertTrue(algorithms.histogram_bins(timeseries))

    @patch.object(algorithms, 'time')
    def test_run_selected_algorithm(self, timeMock):
        timeMock.return_value, timeseries = self.data(time())
        # @modified 20200206 - Feature #3400: Identify air gaps in the metric data
        # Added the airgapped_metrics list
        # result, ensemble, datapoint = algorithms.run_selected_algorithm(timeseries, "test.metric")
        airgapped_metrics = ['test.metric.airgapped.1', 'test.metric.airgapped.2']
        # @modified 20200520 - Feature #3400: Identify air gaps in the metric data
        #                      Feature #3508: ionosphere.untrainable_metrics
        #                      Feature #3504: Handle airgaps in batch metrics
        # Added airgapped_metrics_filled, run_negatives_present and
        # check_for_airgaps_only
        # result, ensemble, datapoint = algorithms.run_selected_algorithm(timeseries, "test.metric", airgapped_metrics)
        airgapped_metrics_filled = []
        run_negatives_present = False
        check_for_airgaps_only = False
        # @modified 20200604 - Feature #3566: custom_algorithms
        # Added algorithms_run
        # @modified 20210519 - Feature #4076: CUSTOM_STALE_PERIOD
        # Added custom_stale_metrics_dict
        custom_stale_metrics_dict = {}
        result, ensemble, datapoint, negatives_found, algorithms_run = algorithms.run_selected_algorithm(timeseries, 'test.metric', airgapped_metrics, airgapped_metrics_filled, run_negatives_present, check_for_airgaps_only, custom_stale_metrics_dict)

        self.assertTrue(result)
        # @modified 20200808 - Bug #3666: Failing algorithm_tests on Python 3.8.3
        # self.assertTrue(len(filter(None, ensemble)) >= settings.CONSENSUS)
        self.assertTrue(len(list(filter(None, ensemble))) >= settings.CONSENSUS)
        self.assertEqual(datapoint, 1000)

    @unittest.skip('Fails inexplicable in certain environments.')
    @patch.object(algorithms, 'CONSENSUS')
    @patch.object(algorithms, 'ALGORITHMS')
    @patch.object(algorithms, 'time')
    def test_run_selected_algorithm_runs_novel_algorithm(self, timeMock,
                                                         algorithmsListMock, consensusMock):
        """
        Assert that a user can add their own custom algorithm.

        This mocks out settings.ALGORITHMS and settings.CONSENSUS to use only a
        single custom-defined function (alwaysTrue)
        """
        algorithmsListMock.__iter__.return_value = ['alwaysTrue']
        consensusMock = 1
        timeMock.return_value, timeseries = self.data(time())

        alwaysTrue = Mock(return_value=True)
        with patch.dict(algorithms.__dict__, {'alwaysTrue': alwaysTrue}):
            result, ensemble, tail_avg = algorithms.run_selected_algorithm(timeseries)

        alwaysTrue.assert_called_with(timeseries)
        self.assertTrue(result)
        self.assertEqual(ensemble, [True])
        self.assertEqual(tail_avg, 334)


if __name__ == '__main__':
    unittest.main()
