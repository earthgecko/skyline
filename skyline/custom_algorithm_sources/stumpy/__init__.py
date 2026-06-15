import os.path
# @modified 20241107 - Task #5535: Update custom_algorithm_sources.stumpy
#                      Task #5526: Build v5.0.0 and upgrade deps
#from pkg_resources import get_distribution, DistributionNotFound
from importlib.metadata import distribution
from site import getsitepackages

from .stump import stump  # noqa: F401
from .aamp import aamp  # noqa: F401
from . import core

try:
    # @modified 20241107 - Task #5535: Update custom_algorithm_sources.stumpy
    #                      Task #5526: Build v5.0.0 and upgrade deps
    #_dist = get_distribution("stumpy")
    # Normalize case for Windows systems
    _dist = distribution("stumpy")
    # Normalize case for Windows systems
    dist_loc = os.path.normcase(getsitepackages()[0])
    here = os.path.normcase(__file__)
    if not here.startswith(os.path.join(dist_loc, "stumpy")):
        # not installed, but there is another version that *is*
        raise ModuleNotFoundError  # pragma: no cover
except ModuleNotFoundError:  # pragma: no cover
    __version__ = "Please install this project with setup.py"
else:  # pragma: no cover
    __version__ = _dist.version
