# STUMPY
# Copyright 2019 TD Ameritrade. Released under the terms of the 3-Clause BSD license.
# STUMPY is a trademark of TD Ameritrade IP Company, Inc. All rights reserved.

from os import environ as os_environ

import numpy as np

os_environ['NUMBA_CACHE_DIR'] = '/opt/skyline/.cache/numba'

STUMPY_THREADS_PER_BLOCK = 512
STUMPY_MEAN_STD_NUM_CHUNKS = 1
STUMPY_MEAN_STD_MAX_ITER = 10
STUMPY_DENOM_THRESHOLD = 1e-14
STUMPY_STDDEV_THRESHOLD = 1e-7
STUMPY_P_NORM_THRESHOLD = 1e-14
STUMPY_TEST_PRECISION = 5
STUMPY_MAX_P_NORM_DISTANCE = np.finfo(np.float64).max
STUMPY_MAX_DISTANCE = np.sqrt(STUMPY_MAX_P_NORM_DISTANCE)
STUMPY_EXCL_ZONE_DENOM = 4
# Allow the user to enable numba jit caching by setting the NUMBA_CACHE_DIR
# EVN variable.
STUMPY_NUMBA_CACHE = False
if os_environ.get('NUMBA_CACHE_DIR'):
    STUMPY_NUMBA_CACHE = True
