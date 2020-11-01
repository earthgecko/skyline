class TooShort(Exception):
    pass


class Stale(Exception):
    pass


class Boring(Exception):
    pass


# added 20201020 - Feature #3792: algorithm_exceptions - EmptyTimeseries
class EmptyTimeseries(Exception):
    """
    An algorithms_exceptions class to handle metrics with empty time series
    """
    pass
