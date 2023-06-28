# STUMPY
# Copyright 2019 TD Ameritrade. Released under the terms of the 3-Clause BSD license.  # noqa: E501
# STUMPY is a trademark of TD Ameritrade IP Company, Inc. All rights reserved.

import warnings
import functools
import inspect
import math

import numpy as np
from numba import njit, prange
# from scipy.signal import convolve
# from scipy.ndimage import maximum_filter1d, minimum_filter1d
# from scipy import linalg
# from scipy.spatial.distance import cdist
# import tempfile

from . import config


def _compare_parameters(norm, non_norm, exclude=None):
    """
    Compare if the parameters in `norm` and `non_norm` are the same

    Parameters
    ----------
    norm : object
        The normalized function (or class) that is complementary to the
        non-normalized function (or class)

    non_norm : object
        The non-normalized function (or class) that is complementary to the
        z-normalized function (or class)

    exclude : list
        A list of parameters to exclude for the comparison

    Returns
    -------
    is_same_params : bool
        `True` if parameters from both `norm` and `non-norm` are the same. `False`
        otherwise.
    """
    norm_params = list(inspect.signature(norm).parameters.keys())
    non_norm_params = list(inspect.signature(non_norm).parameters.keys())

    if exclude is not None:
        for param in exclude:
            if param in norm_params:
                norm_params.remove(param)
            if param in non_norm_params:
                non_norm_params.remove(param)

    is_same_params = set(norm_params) == set(non_norm_params)
    if not is_same_params:
        msg = ""
        if exclude is not None or (isinstance(exclude, list) and len(exclude)):
            msg += f"Excluding `{exclude}` parameters, "
        msg += f"function `{norm.__name__}({norm_params}) and "
        msg += f"function `{non_norm.__name__}({non_norm_params}) "
        msg += "have different arguments/parameters."
        warnings.warn(msg)

    return is_same_params


def non_normalized(non_norm, exclude=None, replace=None):
    """
    Decorator for swapping a z-normalized function (or class) for its complementary
    non-normalized function (or class) as defined by `non_norm`. This requires that
    the z-normalized function (or class) has a `normalize` parameter.

    With the exception of `normalize` parameter, the `non_norm` function (or class)
    must have the same siganture as the `norm` function (or class) signature in order
    to be compatible. Please use a combination of the `exclude` and/or `replace`
    parameters when necessary.

    ```
    def non_norm_func(Q, T, A_non_norm):
        ...
        return


    @non_normalized(
        non_norm_func,
        exclude=["normalize", "p", "A_norm", "A_non_norm"],
        replace={"A_norm": "A_non_norm", "other_norm": None},
    )
    def norm_func(Q, T, A_norm=None, other_norm=None, normalize=True, p=2.0):
        ...
        return
    ```

    Parameters
    ----------
    non_norm : object
        The non-normalized function (or class) that is complementary to the
        z-normalized function (or class)

    exclude : list, default None
        A list of function (or class) parameter names to exclude when comparing the
        function (or class) signatures. When `exlcude is None`, this parameter is
        automatically set to `exclude = ["normalize", "p"]` by default.

    replace : dict, default None
        A dictionary of function (or class) parameter key-value pairs. Each key that
        is found as a parameter name in the `norm` function (or class) will be replaced
        by its corresponding or complementary parameter name in the `non_norm` function
        (or class) (e.g., {"norm_param": "non_norm_param"}). To remove any parameter in
        the `norm` function (or class) that does not exist in the `non_norm` function,
        simply set the value to `None` (i.e., {"norm_param": None}).

    Returns
    -------
    outer_wrapper : object
        The desired z-normalized/non-normalized function (or class)
    """
    if exclude is None:
        exclude = ["normalize", "p"]

    @functools.wraps(non_norm)
    def outer_wrapper(norm):
        @functools.wraps(norm)
        def inner_wrapper(*args, **kwargs):
            is_same_params = _compare_parameters(norm, non_norm, exclude=exclude)
            if not is_same_params or kwargs.get("normalize", True):
                return norm(*args, **kwargs)
            else:
                kwargs = {k: v for k, v in kwargs.items() if k != "normalize"}
                if replace is not None:
                    for k, v in replace.items():
                        if k in kwargs.keys():
                            if v is None:  # pragma: no cover
                                _ = kwargs.pop(k)
                            else:
                                kwargs[v] = kwargs.pop(k)
                return non_norm(*args, **kwargs)

        return inner_wrapper

    return outer_wrapper


def get_pkg_name():  # pragma: no cover
    """
    Return package name.
    """
    return __name__.split(".")[0]


def rolling_window(a, window):
    """
    Use strides to generate rolling/sliding windows for a numpy array.

    Parameters
    ----------
    a : numpy.ndarray
        numpy array

    window : int
        Size of the rolling window

    Returns
    -------
    output : numpy.ndarray
        This will be a new view of the original input array.
    """
    a = np.asarray(a)
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)

    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)


def check_dtype(a, dtype=np.float64):  # pragma: no cover
    """
    Check if the array type of `a` is of type specified by `dtype` parameter.

    Parameters
    ----------
    a : numpy.ndarray
        NumPy array

    dtype : dtype, default np.float64
        NumPy `dtype`

    Raises
    ------
    TypeError
        If the array type does not match `dtype`
    """
    if dtype == int:
        dtype = np.int64
    if dtype == float:
        dtype = np.float64
    if not np.issubdtype(a.dtype, dtype):
        msg = f"{dtype} dtype expected but found {a.dtype} in input array\n"
        msg += "Please change your input `dtype` with `.astype(dtype)`"
        raise TypeError(msg)

    return True


def transpose_dataframe(df):  # pragma: no cover
    """
    Check if the input is a column-wise Pandas `DataFrame`. If `True`, return a
    transpose dataframe since stumpy assumes that each row represents data from a
    different dimension while each column represents data from the same dimension.
    If `False`, return `a` unchanged. Pandas `Series` do not need to be transposed.

    Note that this function has zero dependency on Pandas (not even a soft dependency).

    Parameters
    ----------
    df : numpy.ndarray
        Pandas dataframe

    Returns
    -------
    output : df
        If `df` is a Pandas `DataFrame` then return `df.T`. Otherwise, return `df`
    """
    if type(df).__name__ == "DataFrame":
        return df.T

    return df


def are_arrays_equal(a, b):  # pragma: no cover
    """
    Check if two arrays are equal; first by comparing memory addresses,
    and secondly by their values.

    Parameters
    ----------
    a : numpy.ndarray
        NumPy array

    b : numpy.ndarray
        NumPy array

    Returns
    -------
    output : bool
        Returns `True` if the arrays are equal and `False` otherwise.
    """
    if id(a) == id(b):
        return True

    # For numpy >= 1.19
    # return np.array_equal(a, b, equal_nan=True)

    if a.shape != b.shape:
        return False

    return bool(((a == b) | (np.isnan(a) & np.isnan(b))).all())


def are_distances_too_small(a, threshold=10e-6):  # pragma: no cover
    """
    Check the distance values from a matrix profile.

    If the values are smaller than the threshold (i.e., less than 10e-6) then
    it could suggest that this is a self-join.

    Parameters
    ----------
    a : numpy.ndarray
        NumPy array

    threshold : float, default 10e-6
        Minimum value in which to compare the matrix profile to

    Returns
    -------
    output : bool
        Returns `True` if the matrix profile distances are all below the
        threshold and `False` if they are all above the threshold.
    """
    if a.mean() < threshold or np.all(a < threshold):
        return True

    return False


def check_window_size(m, max_size=None):
    """
    Check the window size and ensure that it is greater than or equal to 3 and, if
    `max_size` is provided, ensure that the window size is less than or equal to the
    `max_size`

    Parameters
    ----------
    m : int
        Window size

    max_size : int, default None
        The maximum window size allowed

    Returns
    -------
    None
    """
    if m <= 2:
        raise ValueError(
            "All window sizes must be greater than or equal to three",
            """A window size that is less than or equal to two is meaningless when
            it comes to computing the z-normalized Euclidean distance. In the case of
            `m=1` produces a standard deviation of zero. In the case of `m=2`, both
            the mean and standard deviation for any given subsequence are identical
            and so the z-normalization for any sequence will either be [-1., 1.] or
            [1., -1.]. Thus, the z-normalized Euclidean distance will be (very likely)
            zero between any subsequence and its nearest neighbor (assuming that the
            time series is large enough to contain both scenarios).
            """,
        )

    if max_size is not None and m > max_size:
        raise ValueError(f"The window size must be less than or equal to {max_size}")


# @njit(
#     # This parallel is what breaks stump when _parallel_rolling_func is compiled
#     # with the cache=True, maybe if the function was declare it would not.
#     # parallel=True,
#     fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
#     cache=config.STUMPY_NUMBA_CACHE,
# )
# def _parallel_rolling_func(a, w, func):
#     """
#     Compute the (embarrassingly parallel) rolling metric by applying a user defined
#     function on a 1-D array
#
#     Parameters
#     ----------
#     a : numpy.ndarray
#         The input array
#
#     w : int
#         The rolling window size
#
#     func : function
#         The numpy function to apply
#
#     Returns
#     -------
#     out : numpy.ndarray
#         Rolling window result when the `func` is applied to each window
#     """
#     lvar = a.shape[0] - w + 1
#     out = np.empty(lvar)
#     for i in prange(lvar):
#         out[i] = func(a[i:(i + w)])
#
#     return out


@njit(
    # "f8[:](f8[:], i8, b1[:])",
    fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
    cache=config.STUMPY_NUMBA_CACHE,
)
def _welford_nanvar(a, w, a_subseq_isfinite):
    """
    Compute the rolling variance for a 1-D array while ignoring NaNs using a modified
    version of Welford's algorithm but is much faster than using `np.nanstd` with stride
    tricks.

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : int
        The rolling window size

    a_subseq_isfinite : numpy.ndarray
        A boolean array that describes whether each subequence of length `w` within `a`
        is finite.

    Returns
    -------
    all_variances : numpy.ndarray
        Rolling window nanvar
    """
    all_variances = np.empty(a.shape[0] - w + 1, dtype=np.float64)
    prev_mean = 0.0
    prev_var = 0.0

    for start_idx in range(a.shape[0] - w + 1):
        prev_start_idx = start_idx - 1
        stop_idx = start_idx + w  # Exclusive index value
        last_idx = start_idx + w - 1  # Last inclusive index value

        if (
            start_idx == 0
            or not a_subseq_isfinite[prev_start_idx]
            or not a_subseq_isfinite[start_idx]
        ):
            curr_mean = np.nanmean(a[start_idx:stop_idx])
            curr_var = np.nanvar(a[start_idx:stop_idx])
        else:
            curr_mean = prev_mean + (a[last_idx] - a[prev_start_idx]) / w
            curr_var = (
                prev_var
                + (a[last_idx] - a[prev_start_idx])
                * (a[last_idx] - curr_mean + a[prev_start_idx] - prev_mean)
                / w
            )

        all_variances[start_idx] = curr_var

        prev_mean = curr_mean
        prev_var = curr_var

    return all_variances


def welford_nanvar(a, w=None):
    """
    Compute the rolling variance for a 1-D array while ignoring NaNs using a modified
    version of Welford's algorithm but is much faster than using `np.nanstd` with stride
    tricks.

    This is a convenience wrapper around the `_welford_nanvar` function.

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : numpy.ndarray, default None
        The rolling window size

    Returns
    -------
    output : numpy.ndarray
        Rolling window nanvar.
    """
    if w is None:
        w = a.shape[0]

    a_subseq_isfinite = rolling_isfinite(a, w)

    return _welford_nanvar(a, w, a_subseq_isfinite)


def welford_nanstd(a, w=None):
    """
    Compute the rolling standard deviation for a 1-D array while ignoring NaNs using
    a modified version of Welford's algorithm but is much faster than using `np.nanstd`
    with stride tricks.

    This a convenience wrapper around `welford_nanvar`.

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : numpy.ndarray, default None
        The rolling window size

    Returns
    -------
    output : numpy.ndarray
        Rolling window nanstd.
    """
    if w is None:
        w = a.shape[0]

    return np.sqrt(np.clip(welford_nanvar(a, w), a_min=0, a_max=None))


@njit(
    parallel=True,
    fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
    cache=config.STUMPY_NUMBA_CACHE,
)
def _rolling_nanstd_1d(a, w):
    """
    A Numba JIT-compiled and parallelized function for computing the rolling standard
    deviation for 1-D array while ignoring NaN.

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : int
        The rolling window size

    Returns
    -------
    out : numpy.ndarray
        This 1D array has the length of `a.shape[0]-w+1`. `out[i]`
        contains the stddev value of `a[i : i + w]`
    """
    n = a.shape[0] - w + 1
    out = np.empty(n, dtype=np.float64)
    for i in prange(n):
        out[i] = np.nanstd(a[i:(i + w)])

    return out


def rolling_nanstd(a, w, welford=False):
    """
    Compute the rolling standard deviation over the last axis of `a` while ignoring
    NaNs.

    This essentially replaces:
        `np.nanstd(rolling_window(a[..., start:stop], w), axis=a.ndim)`

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : numpy.ndarray
        The rolling window size

    welford : bool, default False
        When False (default), the computation is parallelized and the stddev of
        each subsequence is calculated on its own. When `welford==True`, the
        welford method is used to reduce the computing time at the cost of slightly
        reduced precision.

    Returns
    -------
    out : numpy.ndarray
        Rolling window nanstd
    """
    axis = a.ndim - 1  # Account for rolling
    if welford:
        return np.apply_along_axis(
            lambda a_row, w: welford_nanstd(a_row, w), axis=axis, arr=a, w=w
        )
    else:
        return np.apply_along_axis(
            lambda a_row, w: _rolling_nanstd_1d(a_row, w), axis=axis, arr=a, w=w
        )


def compute_mean_std(T, m):
    """
    Compute the sliding mean and standard deviation for the array `T` with
    a window size of `m`

    Parameters
    ----------
    T : numpy.ndarray
        Time series or sequence

    m : int
        Window size

    Returns
    -------
    M_T : numpy.ndarray
        Sliding mean. All nan values are replaced with np.inf

    Σ_T : numpy.ndarray
        Sliding standard deviation

    Notes
    -----
    `DOI: 10.1109/ICDM.2016.0179 \
    <https://www.cs.ucr.edu/~eamonn/PID4481997_extend_Matrix%20Profile_I.pdf>`__

    See Table II

    DOI: 10.1145/2020408.2020587

    See Page 2 and Equations 1, 2

    DOI: 10.1145/2339530.2339576

    See Page 4

    http://www.cs.unm.edu/~mueen/FastestSimilaritySearch.html

    Note that Mueen's algorithm has an off-by-one bug where the
    sum for the first subsequence is omitted and we fixed that!
    """
    num_chunks = config.STUMPY_MEAN_STD_NUM_CHUNKS
    max_iter = config.STUMPY_MEAN_STD_MAX_ITER

    if T.ndim > 2:  # pragma nocover
        raise ValueError("T has to be one or two dimensional!")

    for iteration in range(max_iter):
        try:
            chunk_size = math.ceil((T.shape[-1] + 1) / num_chunks)
            if chunk_size < m:
                chunk_size = m

            mean_chunks = []
            std_chunks = []
            for chunk in range(num_chunks):
                start = chunk * chunk_size
                stop = min(start + chunk_size + m - 1, T.shape[-1])
                if stop - start < m:
                    break

                tmp_mean = np.mean(rolling_window(T[..., start:stop], m), axis=T.ndim)
                mean_chunks.append(tmp_mean)
                tmp_std = rolling_nanstd(T[..., start:stop], m)
                std_chunks.append(tmp_std)

            M_T = np.hstack(mean_chunks)
            Σ_T = np.hstack(std_chunks)
            break

        except MemoryError:  # pragma nocover
            num_chunks *= 2

    if iteration < max_iter - 1:
        M_T[np.isnan(M_T)] = np.inf
        Σ_T[np.isnan(Σ_T)] = 0

        return M_T, Σ_T
    else:  # pragma nocover
        raise MemoryError(
            "Could not calculate mean and standard deviation. "
            "Increase the number of chunks or maximal iterations."
        )


def _preprocess(T):
    """
    Creates a copy of the time series, transposes all dataframes, converts to
    `numpy.ndarray`, and checks the `dtype`

    Parameters
    ----------
    T : numpy.ndarray
        Time series or sequence

    Returns
    -------
    T : numpy.ndarray
        Modified time series
    """
    T = T.copy()
    T = transpose_dataframe(T)
    T = np.asarray(T)
    check_dtype(T)

    return T


def preprocess(T, m):
    """
    Creates a copy of the time series where all NaN and inf values
    are replaced with zero. Also computes mean and standard deviation
    for every subsequence. Every subsequence that contains at least
    one NaN or inf value, will have a mean of np.inf. For the standard
    deviation these values are ignored. If all values are illegal, the
    standard deviation will be 0 (see `core.compute_mean_std`)

    Parameters
    ----------
    T : numpy.ndarray
        Time series or sequence

    m : int
        Window size

    Returns
    -------
    T : numpy.ndarray
        Modified time series
    M_T : numpy.ndarray
        Rolling mean
    Σ_T : numpy.ndarray
        Rolling standard deviation
    """
    T = _preprocess(T)
    check_window_size(m, max_size=T.shape[-1])
    T[np.isinf(T)] = np.nan
    M_T, Σ_T = compute_mean_std(T, m)
    T[np.isnan(T)] = 0

    return T, M_T, Σ_T


def preprocess_non_normalized(T, m):
    """
    Preprocess a time series that is to be used when computing a non-normalized (i.e.,
    without z-normalization) distance matrix.

    Creates a copy of the time series where all NaN and inf values
    are replaced with zero. Every subsequence that contains at least
    one NaN or inf value will have a `False` value in its `T_subseq_isfinite` `bool`
    array.

    Parameters
    ----------
    T : numpy.ndarray
        Time series or sequence

    m : int
        Window size

    Returns
    -------
    T : numpy.ndarray
        Modified time series

    T_subseq_isfinite : numpy.ndarray
        A boolean array that indicates whether a subsequence in `T` contains a
        `np.nan`/`np.inf` value (False)

    T_subseq_isconstant : numpy.ndarray
        A boolean array that indicates whether a subsequence in `T` is constant
        (True)
    """
    T = _preprocess(T)
    check_window_size(m, max_size=T.shape[-1])
    T_subseq_isfinite = rolling_isfinite(T, m)
    T[~np.isfinite(T)] = 0.0
    T_subseq_isconstant = rolling_isconstant(T, m)

    return T, T_subseq_isfinite, T_subseq_isconstant


def preprocess_diagonal(T, m):
    """
    Preprocess a time series that is to be used when traversing the diagonals of a
    distance matrix.

    Creates a copy of the time series where all NaN and inf values are replaced
    with zero. Also computes means, `M_T` and `M_T_m_1`, for every subsequence
    using a window size of `m` and `m-1`, respectively, and the inverse standard
    deviation, `Σ_T_inverse`. Every subsequence that contains at least one NaN or
    inf value will have a `False` value in its `T_subseq_isfinite` `bool` array.
    Additionally, the inverse standard deviation, σ_inverse, will also be computed
    and returned. Finally, constant subsequences (i.e., subsequences with a standard
    deviation of zero), will have a corresponding `True` value in its
    `T_subseq_isconstant` array.

    Parameters
    ----------
    T : numpy.ndarray
        Time series or sequence

    m : int
        Window size

    Returns
    -------
    T : numpy.ndarray
        Modified time series

    M_T : numpy.ndarray
        Rolling mean with a subsequence length of `m`

    Σ_T_inverse : numpy.ndarray
        Inverted rolling standard deviation

    M_T_m_1 : numpy.ndarray
        Rolling mean with a subsequence length of `m-1`

    T_subseq_isfinite : numpy.ndarray
        A boolean array that indicates whether a subsequence in `T` contains a
        `np.nan`/`np.inf` value (False)

    T_subseq_isconstant : numpy.ndarray
        A boolean array that indicates whether a subsequence in `T` is constant (True)
    """
    T, T_subseq_isfinite, T_subseq_isconstant = preprocess_non_normalized(T, m)
    M_T, Σ_T = compute_mean_std(T, m)
    Σ_T[T_subseq_isconstant] = 1.0  # Avoid divide by zero in next inversion step
    Σ_T_inverse = 1.0 / Σ_T
    M_T_m_1, _ = compute_mean_std(T, m - 1)

    return T, M_T, Σ_T_inverse, M_T_m_1, T_subseq_isfinite, T_subseq_isconstant


@njit(
    # "i8[:](i8[:], i8, i8, i8)",
    fastmath=True,
    cache=config.STUMPY_NUMBA_CACHE,
)
def _count_diagonal_ndist(diags, m, n_A, n_B):
    """
    Count the number of distances that would be computed for each diagonal index
    referenced in `diags`

    Parameters
    ----------
    diags : numpy.ndarray
        The diagonal indices of interest

    m : int
        Window size

    n_A : int
        The length of time series `T_A`

    n_B : int
        The length of time series `T_B`

    Returns
    -------
    diag_ndist_counts : numpy.ndarray
        Counts of distances computed along each diagonal of interest
    """
    diag_ndist_counts = np.zeros(diags.shape[0], dtype=np.int64)
    for diag_idx in range(diags.shape[0]):
        k = diags[diag_idx]
        if k >= 0:
            diag_ndist_counts[diag_idx] = min(n_B - m + 1 - k, n_A - m + 1)
        else:
            diag_ndist_counts[diag_idx] = min(n_B - m + 1, n_A - m + 1 + k)

    return diag_ndist_counts


@njit(
    # "i8[:, :](i8[:], i8, b1)"
    cache=config.STUMPY_NUMBA_CACHE,
)
def _get_array_ranges(a, n_chunks, truncate):
    """
    Given an input array, split it into `n_chunks`.

    Parameters
    ----------
    a : numpy.ndarray
        An array to be split

    n_chunks : int
        Number of chunks to split the array into

    truncate : bool
        If `truncate=True`, truncate the rows of `array_ranges` if there are not enough
        elements in `a` to be chunked up into `n_chunks`.  Otherwise, if
        `truncate=False`, all extra chunks will have their start and stop indices set
        to `a.shape[0]`.

    Returns
    -------
    array_ranges : numpy.ndarray
        A two column array where each row consists of a start and (exclusive) stop index
        pair. The first column contains the start indices and the second column
        contains the stop indices.
    """
    array_ranges = np.zeros((n_chunks, 2), dtype=np.int64)
    if a.shape[0] > 0 and n_chunks > 0:
        cumsum = a.cumsum() / a.sum()
        insert = np.linspace(0, 1, n_chunks + 1)[1:-1]
        idx = 1 + np.searchsorted(cumsum, insert)
        array_ranges[1:, 0] = idx  # Fill the first column with start indices
        array_ranges[:-1, 1] = idx  # Fill the second column with exclusive stop indices
        array_ranges[-1, 1] = a.shape[0]  # Handle the stop index for the final chunk

        diff_idx = np.diff(idx)
        if np.any(diff_idx == 0):
            row_truncation_idx = np.argmin(diff_idx) + 2
            array_ranges[row_truncation_idx:, 0] = a.shape[0]
            array_ranges[(row_truncation_idx - 1):, 1] = a.shape[0]
            if truncate:
                array_ranges = array_ranges[:row_truncation_idx]

    return array_ranges


def _rolling_isfinite_1d(a, w):
    """
    Determine if all elements in each rolling window `isfinite`

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : int
        The length of the rolling window

    Return
    ------
    output : numpy.ndarray
        A boolean array of length `a.shape[0] - w + 1` that records whether each
        rolling window subsequence contain all finite values
    """
    if a.dtype == np.dtype("bool"):
        a_isfinite = a.copy()
    else:
        a_isfinite = np.isfinite(a)
    a_subseq_isfinite = rolling_window(a_isfinite, w)

    # Process first subsequence
    a_first_subseq = ~a_isfinite[:w]
    if a_first_subseq.any():
        a_isfinite[: np.flatnonzero(a_first_subseq).max()] = False

    # Shift `a_isfinite` and fill forward by `w`
    a_subseq_isfinite[~a_isfinite[(w - 1):]] = False

    return a_isfinite[: a_isfinite.shape[0] - w + 1]


def rolling_isfinite(a, w):
    """
    Compute the rolling `isfinite` for 1-D and 2-D arrays.

    This a convenience wrapper around `_rolling_isfinite_1d`.

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : numpy.ndarray
        The rolling window size

    Returns
    -------
    output : numpy.ndarray
        Rolling window nanmax.
    """
    axis = a.ndim - 1  # Account for rolling
    return np.apply_along_axis(
        lambda a_row, w: _rolling_isfinite_1d(a_row, w), axis=axis, arr=a, w=w
    )


@njit(
    parallel=True,
    fastmath={"nsz", "arcp", "contract", "afn", "reassoc"},
    cache=config.STUMPY_NUMBA_CACHE,
)
def _rolling_isconstant(a, w):
    """
    Compute the rolling isconstant for 1-D and 2-D arrays.

    This is accomplished by comparing the min and max within each window and
    assigning `True` when the min and max are equal and `False` otherwise. If
    a subsequence contains at least one NaN, then the subsequence is not constant.

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : numpy.ndarray
        The rolling window size

    Returns
    -------
    output : numpy.ndarray
        Rolling window isconstant.
    """
    # out = _parallel_rolling_func(a, w, np.ptp)
    l = a.shape[0] - w + 1
    out = np.empty(l)
    for i in prange(l):
        out[i] = np.ptp(a[i : i + w])

    return np.where(out == 0.0, True, False)


def rolling_isconstant(a, w):
    """
    Compute the rolling isconstant for 1-D and 2-D arrays.

    This is accomplished by comparing the min and max within each window and
    assigning `True` when the min and max are equal and `False` otherwise. If
    a subsequence contains at least one NaN, then the subsequence is not constant.

    Parameters
    ----------
    a : numpy.ndarray
        The input array

    w : numpy.ndarray
        The rolling window size

    Returns
    -------
    output : numpy.ndarray
        Rolling window isconstant.
    """
    axis = a.ndim - 1
    return np.apply_along_axis(
        lambda a_row, w: _rolling_isconstant(a_row, w), axis=axis, arr=a, w=w
    )


@njit(
    cache=config.STUMPY_NUMBA_CACHE,
)
def _merge_topk_ρI(ρA, ρB, IA, IB):
    """
    Merge two top-k pearson profiles `ρA` and `ρB`, and update `ρA` (in place).
    When the inputs are 1D arrays, ρA[i] is updated if it is less than ρB[i] and
    IA[i] != IB[i]. In such case, ρA[i] and IA[i] are replaced with ρB[i] and IB[i],
    respectively. (Note that it might happen that IA[i]==IB[i] but ρA[i] != ρB[i].
    This situation can occur if there is slight imprecision in numerical calculations.
    In that case, we do not update ρA[i] and IA[i]. While updating ρA[i] and IA[i]
    is harmless in this case, we avoid doing that so to be consistent with the merging
    process when the inputs are 2D arrays)
    When the inputs are 2D arrays, we always prioritize the values of `ρA` over
    the values of `ρB` in case of ties. (i.e., values from `ρB` are always inserted
    to the left of values from `ρA`). Also, update `IA` accordingly. In case of
    overlapping values between two arrays IA[i] and IB[i], the ones in IB[i] (and
    their corresponding values in ρB[i]) are ignored throughout the updating process
    of IA[i] (and ρA[i]).

    Unlike `_merge_topk_PI`, where `top-k` smallest values are kept, this function
    keeps `top-k` largest values.

    Parameters
    ----------
    ρA : numpy.ndarray
        A (top-k) pearson profile where values in each row are sorted in ascending
        order. `ρA` must be 1- or 2-dimensional.

    ρB : numpy.ndarray
        A (top-k) pearson profile, where values in each row are sorted in ascending
        order. `ρB` must have the same shape as `ρA`.

    IA : numpy.ndarray
        A (top-k) matrix profile indices corresponding to `ρA`

    IB : numpy.ndarray
        A (top-k) matrix profile indices corresponding to `ρB`

    Returns
    -------
    None
    """
    if ρA.ndim == 1:
        mask = (ρB > ρA) & (IB != IA)
        ρA[mask] = ρB[mask]
        IA[mask] = IB[mask]
    else:
        k = ρA.shape[1]
        tmp_ρ = np.empty(k, dtype=np.float64)
        tmp_I = np.empty(k, dtype=np.int64)
        last_idx = k - 1
        for i in range(len(ρA)):
            overlap = set(IB[i]).intersection(set(IA[i]))
            aj, bj = last_idx, last_idx
            idx = last_idx
            # 2 * k iterations are required to traverse both A and B if needed.
            for _ in range(2 * k):
                if idx < 0:
                    break
                if bj >= 0 and ρB[i, bj] > ρA[i, aj]:
                    if IB[i, bj] not in overlap:
                        tmp_ρ[idx] = ρB[i, bj]
                        tmp_I[idx] = IB[i, bj]
                        idx -= 1
                    bj -= 1
                else:
                    tmp_ρ[idx] = ρA[i, aj]
                    tmp_I[idx] = IA[i, aj]
                    idx -= 1
                    aj -= 1

            ρA[i] = tmp_ρ
            IA[i] = tmp_I


@njit(
    cache=config.STUMPY_NUMBA_CACHE,
)
def _shift_insert_at_index(a, idx, v, shift="right"):
    """
    If `shift=right` (default), all elements in `a[idx:]` are shifted to the right by
    one element, `v` in inserted at index `idx` and the last element is discarded.
    If `shift=left`, all elements in `a[:idx]` are shifted to the left by one element,
    `v` in inserted at index `idx-1`, and the first element is discarded. In both cases,
    `a` is updated in place and its length remains unchanged.

    Note that all unrecognized `shift` inputs will default to `shift=right`.


    Parameters
    ----------
    a : numpy.ndarray
        A 1d array

    idx : int
        The index at which the value `v` should be inserted. This can be any
        integer number from `0` to `len(a)`. When `idx=len(a)` and `shift="right"`,
        OR when `idx=0` and `shift="left"`, then no change will occur on
        the input array `a`.

    v : float
        The value that should be inserted into array `a` at index `idx`

    shift : str, default "right"
        The value that indicates whether the shifting of elements should be towards
        the right or left. If `shift="right"` (default), all elements in `a[idx:]`
        are shifted to the right by one element. If `shift="left"`, all elements
        in `a[:idx]` are shifted to the left by one element.

    Returns
    -------
    None
    """
    if shift == "left":
        if 0 < idx <= len(a):
            a[: idx - 1] = a[1:idx]
            # elements were shifted to the left, thus the insertion index becomes
            # `idx-1`
            a[idx - 1] = v
    else:
        if 0 <= idx < len(a):
            a[(idx + 1):] = a[idx:-1]
            a[idx] = v


def _check_P(P, threshold=1e-6):
    """
    Check if the 1-dimensional matrix profile values are too small and
    log a warning the if true.

    Parameters
    ----------
    P : numpy.ndarray
        A 1-dimensional matrix profile

    threshold : float, default 1e-6
        A distance threshold

    Returns
    -------
        None
    """
    if P.ndim != 1:
        raise ValueError("`P` was {P.ndim}-dimensional and must be 1-dimensional")
    if are_distances_too_small(P, threshold=threshold):  # pragma: no cover
        msg = f"A large number of values in `P` are smaller than {threshold}.\n"
        msg += "For a self-join, try setting `ignore_trivial=True`."
        warnings.warn(msg)


def check_ignore_trivial(T_A, T_B, ignore_trivial):
    """
    Check inputs and verify the appropriateness for self-joins vs AB-joins and
    provides relevant warnings.

    Note that the warnings will output the first occurrence of matching warnings
    for each location (module + line number) where the warning is issued

    Parameters
    ----------
    T_A : numpy.ndarray
        The time series or sequence for which to compute the matrix profile

    T_B : numpy.ndarray
        The time series or sequence that will be used to annotate T_A. For every
        subsequence in T_A, its nearest neighbor in T_B will be recorded. Default is
        `None` which corresponds to a self-join.

    ignore_trivial : bool
        Set to `True` if this is a self-join. Otherwise, for AB-join, set this
        to `False`.

    Returns
    -------
    ignore_trivial : bool
        The (corrected) ignore_trivial value

    Notes
    -----
    These warnings may be supresse by using a context manager
    ```
    import stumpy
    import numpy as np
    import warnings

    T = np.random.rand(10_000)
    m = 50
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Arrays T_A, T_B are equal")
        for _ in range(5):
            stumpy.stump(T, m, T, ignore_trivial=False)
    ```
    """
    if ignore_trivial is False and are_arrays_equal(T_A, T_B):  # pragma: no cover
        msg = "Arrays T_A, T_B are equal, which implies a self-join. "
        msg += "Try setting `ignore_trivial = True`."
        warnings.warn(msg)

    if ignore_trivial and are_arrays_equal(T_A, T_B) is False:  # pragma: no cover
        msg = "Arrays T_A, T_B are not equal, which implies an AB-join. "
        msg += "`ignore_trivial` has been automatically set to `False`."
        warnings.warn(msg)
        ignore_trivial = False

    return ignore_trivial
