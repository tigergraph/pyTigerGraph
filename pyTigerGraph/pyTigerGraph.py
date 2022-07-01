import warnings
from typing import TYPE_CHECKING, Union

import urllib3

from pyTigerGraph.pyTigerGraphAuth import pyTigerGraphAuth
from pyTigerGraph.pyTigerGraphEdge import pyTigerGraphEdge
from pyTigerGraph.pyTigerGraphLoading import pyTigerGraphLoading
from pyTigerGraph.pyTigerGraphPath import pyTigerGraphPath
from pyTigerGraph.pyTigerGraphUDT import pyTigerGraphUDT
from pyTigerGraph.pyTigerGraphVertex import pyTigerGraphVertex
from pyTigerGraph.pyTigerGraphGSQL import pyTigerGraphGSQL

if TYPE_CHECKING:
    from .gds import gds

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("default", category=DeprecationWarning)


# TODO Proper deprecation handling; import deprecation?

class TigerGraphConnection(pyTigerGraphVertex, pyTigerGraphEdge, pyTigerGraphUDT, pyTigerGraphAuth,
    pyTigerGraphLoading, pyTigerGraphPath, object):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs"""

    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph", gsqlSecret: str = "",
            username: str = "tigergraph", password: str = "tigergraph", tgCloud: bool = False,
            restppPort: Union[int, str] = "9000", gsPort: Union[int, str] = "14240",
            gsqlVersion: str = "", version: str = "", apiToken: str = "", useCert: bool = True,
            certPath: str = None, debug: bool = False, sslPort: Union[int, str] = "443",
            gcp: bool = False):
        super().__init__(host, graphname, gsqlSecret, username, password, tgCloud, restppPort, gsPort, gsqlVersion,
            version, apiToken, useCert, certPath, debug, sslPort, gcp)

        self.gds = None

    def __getattribute__(self, name):
        if name == "gds":
            if super().__getattribute__(name) is None:
                try:
                    from .gds import gds
                    self.gds = gds.GDS(self)
                    return super().__getattribute__(name)
                except:
                    raise Exception("Please install the GDS package requirements to use the GDS functionality."
                                    "Check the https://docs.tigergraph.com/pytigergraph/current/getting-started/install#_install_pytigergraphgds for more details.")
            else:
                return super().__getattribute__(name)
        else:
            return super().__getattribute__(name)

# EOF
