import warnings

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pyTigerGraph.pyTigerGraphAuth import pyTigerGraphAuth
from pyTigerGraph.pyTigerGraphEdge import pyTigerGraphEdge
from pyTigerGraph.pyTigerGraphLoading import pyTigerGraphLoading
from pyTigerGraph.pyTigerGraphPath import pyTigerGraphPath
from pyTigerGraph.pyTigerGraphUDT import pyTigerGraphUDT
from pyTigerGraph.pyTigerGraphVertex import pyTigerGraphVertex

from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .gds import gds

# Added pyTigerDriver Client

warnings.filterwarnings("default", category=DeprecationWarning)


# TODO Proper deprecation handling; import deprecation?

class TigerGraphConnection(pyTigerGraphVertex, pyTigerGraphEdge, pyTigerGraphUDT, pyTigerGraphAuth,
    pyTigerGraphLoading, pyTigerGraphPath):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs"""

    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph",
            username: str = "tigergraph", password: str = "tigergraph",
            restppPort: Union[int, str] = "9000", gsPort: Union[int, str] = "14240", gsqlVersion: str = "",
            version: str = "", apiToken: str = "", useCert: bool = True, certPath: str = None,
            debug: bool = False, sslPort: Union[int, str] = "443", gcp: bool = False):
        super().__init__(host, graphname, username, password, restppPort
            , gsPort, gsqlVersion, version, apiToken, useCert, certPath, debug, sslPort, gcp)
        try:
            from .gds import gds
            self.gds = gds.GDS(self) # Placeholder attribute for GDS functionality
        except:
            self.gds = None

# EOF
