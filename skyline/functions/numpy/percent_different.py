from numpy import array as np_array
from numpy import diff as np_diff
from numpy import isnan as np_isnan
from numpy import isinf as np_isinf


# @added 20210424 - Feature #4014: Ionosphere - inference
def get_percent_different(base_value, compare_value, always_return_as_positive=True):
    """
    Given a value, determine what percentage another value is as a percent of
    the given value.  This will always return the difference as a positive
    value (or None), meaning -1.234 % will be returned as 1.234 %, UNLESS
    otherwise specified with the always_return_as_positive argument.
    This is because in the original implementation in Ionosphere, ionosphere
    does not care if it is -26% or 26%, any value of 26 whether positive or
    negative is not a match.
    In its further implementation in inference and areas under curves
    comparisons, the same is true.

    """
    percent_different = None
    sums_array = np_array([base_value, compare_value], dtype=float)
    calc_percent_different = np_diff(sums_array) / sums_array[:-1] * 100.
    percent_different = calc_percent_different[0]
    if percent_different < 0:
        new_pdiff = percent_different * -1
        percent_different = new_pdiff
    if np_isnan(percent_different):
        # 0 / 0 == nan
        # 0 of 0 % different from 0, they are the same thing so the percentage
        # between them is 0.
        # What is thee percent difference between 1 and 1?
        # 0%
        # What is thee percent difference between 0 and 0?
        # 0%
        # nan, but it should be a number.
        percent_different = 0
    if np_isinf(percent_different):
        # 0 / 1
        percent_different = None
    return percent_different
