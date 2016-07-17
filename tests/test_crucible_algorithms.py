import unittest2 as unittest
from mock import Mock, patch
from time import time
import os.path
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.join(os.path.dirname(os.path.realpath(current_dir)))
skyline_dir = parent_dir + '/skyline'
sys.path.append(skyline_dir)

from crucible import crucible_algorithms
import settings


class TestAlgorithms(unittest.TestCase):
    """
    Test all algorithms with a common, simple/known anomalous data set
    """

    def _addSkip(self, test, tail_avg, reason):
        print reason

    def anomaly_json(self, ts):
        """
        Mostly ones (1), with a final value of 1000
        """
        timeseries = map(list, zip(map(float, range(int(ts) - 86400, int(ts) + 1)), [1] * 86401))
        timeseries[-1][1] = 1000
        timeseries[-2][1] = 1
        timeseries[-3][1] = 1

# IN PROGRESS need to make a json anomaly file and a crucible anomaly file
        converted = []
        for datapoint in timeseries:
            try:
                new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                converted.append(new_datapoint)
            except:
                continue

        with open(anomaly_json, 'w') as f:
            f.write(json.dumps(converted))
        if python_version == 2:
            os.chmod(anomaly_json, 0644)
        if python_version == 3:
            os.chmod(anomaly_json, 0o644)

    def data(self, ts):
        """
        Mostly ones (1), with a final value of 1000
        """
        timeseries = map(list, zip(map(float, range(int(ts) - 86400, int(ts) + 1)), [1] * 86401))
        timeseries[-1][1] = 1000
        timeseries[-2][1] = 1
        timeseries[-3][1] = 1

        return ts, timeseries

    def test_tail_avg(self):
        _, timeseries = self.data(time())
        self.assertEqual(crucible_algorithms.tail_avg(timeseries), 334)

    def test_grubbs(self):
#        _, timeseries = self.data(time())
        now = time()
        _, timeseries = self.data(now)
        start_timestamp = timeseries[0][0]
        end_timestamp = timeseries[-1][0]
        full_duration = end_timestamp - start_timestamp
        self.assertTrue(crucible_algorithms.grubbs(timeseries, end_timestamp, full_duration))

    @patch.object(crucible_algorithms, 'time')
    def test_first_hour_average(self, timeMock):
        timeMock.return_value, timeseries = self.data(time())
        self.assertTrue(crucible_algorithms.first_hour_average(timeseries))

    def test_stddev_from_average(self):
        # _, timeseries = self.data(time())
        # self.assertTrue(crucible_algorithms.stddev_from_average(timeseries))
        now = time()
        _, timeseries = self.data(now)
        start_timestamp = timeseries[0][0]
        end_timestamp = timeseries[-1][0]
        full_duration = end_timestamp - start_timestamp

        _, timeseries = self.data(time())
        self.assertTrue(crucible_algorithms.stddev_from_average(timeseries, end_timestamp, full_duration))

    def test_stddev_from_moving_average(self):
        _, timeseries = self.data(time())
        self.assertTrue(crucible_algorithms.stddev_from_moving_average(timeseries))

    def test_mean_subtraction_cumulation(self):
        _, timeseries = self.data(time())
        self.assertTrue(crucible_algorithms.mean_subtraction_cumulation(timeseries))

    @patch.object(crucible_algorithms, 'time')
    def test_least_squares(self, timeMock):
        timeMock.return_value, timeseries = self.data(time())
        self.assertTrue(crucible_algorithms.least_squares(timeseries))

    def test_histogram_bins(self):
        _, timeseries = self.data(time())
        self.assertTrue(crucible_algorithms.histogram_bins(timeseries))

    @patch.object(crucible_algorithms, 'time')
    def test_run_selected_algorithm(self, timeMock):
        timeMock.return_value, timeseries = self.data(time())
        result, ensemble, datapoint = crucible_algorithms.run_selected_algorithm(timeseries, "test.metric")
        self.assertTrue(result)
        self.assertTrue(len(filter(None, ensemble)) >= settings.CONSENSUS)
        self.assertEqual(datapoint, 1000)

    @unittest.skip('Fails inexplicable in certain environments.')
    @patch.object(crucible_algorithms, 'CONSENSUS')
    @patch.object(crucible_algorithms, 'ALGORITHMS')
    @patch.object(crucible_algorithms, 'time')
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
        with patch.dict(crucible_algorithms.__dict__, {'alwaysTrue': alwaysTrue}):
            result, ensemble, tail_avg = algorithms.run_selected_algorithm(timeseries)

        alwaysTrue.assert_called_with(timeseries)
        self.assertTrue(result)
        self.assertEqual(ensemble, [True])
        self.assertEqual(tail_avg, 334)


if __name__ == '__main__':
    unittest.main()
