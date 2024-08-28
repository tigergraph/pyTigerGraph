import sys
import warnings
from typing import TYPE_CHECKING, Union

import urllib3

# from pyTigerGraph.pyTigerGraphEdge import pyTigerGraphEdge
# from pyTigerGraph.pyTigerGraphLoading import pyTigerGraphLoading
# from pyTigerGraph.pyTigerGraphPath import pyTigerGraphPath
# from pyTigerGraph.pyTigerGraphUDT import pyTigerGraphUDT
# from pyTigerGraph.pyTigerGraphVertex import pyTigerGraphVertex
# from pyTigerGraph.pyTigerGraphDataset import pyTigerGraphDataset

# from .pyTigerGraphAuth import AsyncPyTigerGraphAuth
# from .pyTigerGraphGSQL import AsyncPyTigerGraphGSQL
# from .pyTigerGraphBase import AsyncPyTigerGraphBase
# from .pyTigerGraphSchema import AsyncPyTigerGraphSchema
# from .pyTigerGraphUtils import AsyncPyTigerGraphUtils
from .pyTigerGraphVertex import AsyncPyTigerGraphVertex
from .pyTigerGraphDataset import AsyncPyTigerGraphDataset
# from .pyTigerGraphQuery import AsyncPyTigerGraphQuery
from .pyTigerGraphEdge import AsyncPyTigerGraphEdge
from .pyTigerGraphLoading import AsyncPyTigerGraphLoading
from .pyTigerGraphPath import AsyncPyTigerGraphPath
from .pyTigerGraphUDT import AsyncPyTigerGraphUDT

if TYPE_CHECKING:
    from ..gds import gds

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if not sys.warnoptions:
    warnings.filterwarnings("once", category=DeprecationWarning)


# TODO Proper deprecation handling; import deprecation?

class AsyncTigerGraphConnection(AsyncPyTigerGraphDataset, AsyncPyTigerGraphEdge, AsyncPyTigerGraphVertex,
                                AsyncPyTigerGraphLoading, AsyncPyTigerGraphPath, AsyncPyTigerGraphUDT, object):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs"""

    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph",
                 gsqlSecret: str = "", username: str = "tigergraph", password: str = "tigergraph",
                 tgCloud: bool = False, restppPort: Union[int, str] = "9000",
                 gsPort: Union[int, str] = "14240", gsqlVersion: str = "", version: str = "",
                 apiToken: str = "", useCert: bool = None, certPath: str = None, debug: bool = None,
                 sslPort: Union[int, str] = "443", gcp: bool = False, jwtToken: str = ""):
        super().__init__(host, graphname, gsqlSecret, username, password, tgCloud, restppPort,
                         gsPort, gsqlVersion, version, apiToken, useCert, certPath, debug, sslPort, gcp, jwtToken)

        self.gds = None
        self.ai = None

    def __getattribute__(self, name):
        if name == "gds":
            if super().__getattribute__(name) is None:
                try:
                    from ..gds import gds
                    self.gds = gds.GDS(self)
                    return super().__getattribute__(name)
                except:
                    raise Exception(
                        "Please install the GDS package requirements to use the GDS functionality."
                        "Check the https://docs.tigergraph.com/pytigergraph/current/getting-started/install#_install_pytigergraphgds for more details.")
            else:
                return super().__getattribute__(name)
        elif name == "ai":
            if super().__getattribute__(name) is None:
                try:
                    from ..ai import ai
                    self.ai = ai.AI(self)
                    return super().__getattribute__(name)
                except Exception as e:
                    raise Exception(
                        "Error importing AI submodule. "+str(e)
                    )
            else:
                return super().__getattribute__(name)
        else:
            return super().__getattribute__(name)

# EOF
