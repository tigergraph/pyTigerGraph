from importlib.metadata import version as _pkg_version, PackageNotFoundError

from pyTigerGraph.pyTigerGraph import TigerGraphConnection
from pyTigerGraph.pytgasync.pyTigerGraph import AsyncTigerGraphConnection
from pyTigerGraph.common.exception import TigerGraphException

try:
    __version__ = _pkg_version("pyTigerGraph")
except PackageNotFoundError:
    __version__ = "2.0.2"

__license__ = "Apache 2"

__all__ = [
    "TigerGraphConnection",
    "AsyncTigerGraphConnection",
    "TigerGraphException",
]
