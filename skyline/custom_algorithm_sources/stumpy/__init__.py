import os.path
from pkg_resources import get_distribution, DistributionNotFound
from .stump import stump  # noqa: F401
from .aamp import aamp  # noqa: F401
from . import core

try:
    _dist = get_distribution("stumpy")
    # Normalize case for Windows systems
    dist_loc = os.path.normcase(_dist.location)
    here = os.path.normcase(__file__)
    if not here.startswith(os.path.join(dist_loc, "stumpy")):
        # not installed, but there is another version that *is*
        raise DistributionNotFound  # pragma: no cover
except DistributionNotFound:  # pragma: no cover
    __version__ = "Please install this project with setup.py"
else:  # pragma: no cover
    __version__ = _dist.version
