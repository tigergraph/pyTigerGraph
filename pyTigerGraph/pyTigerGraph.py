import sys
import warnings
from typing import TYPE_CHECKING, Union

import urllib3

from pyTigerGraph.pyTigerGraphEdge import pyTigerGraphEdge
from pyTigerGraph.pyTigerGraphLoading import pyTigerGraphLoading
from pyTigerGraph.pyTigerGraphPath import pyTigerGraphPath
from pyTigerGraph.pyTigerGraphUDT import pyTigerGraphUDT
from pyTigerGraph.pyTigerGraphVertex import pyTigerGraphVertex
from pyTigerGraph.pyTigerGraphDataset import pyTigerGraphDataset

if TYPE_CHECKING:
    from .gds import gds

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if not sys.warnoptions:
    warnings.filterwarnings("once", category=DeprecationWarning)


# TODO Proper deprecation handling; import deprecation?

class TigerGraphConnection(pyTigerGraphVertex, pyTigerGraphEdge, pyTigerGraphUDT,
                           pyTigerGraphLoading, pyTigerGraphPath, pyTigerGraphDataset, object):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs"""

    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "",
                 gsqlSecret: str = "", username: str = "tigergraph", password: str = "tigergraph",
                 tgCloud: bool = False, restppPort: Union[int, str] = "9000",
                 gsPort: Union[int, str] = "14240", gsqlVersion: str = "", version: str = "",
                 apiToken: str = "", useCert: bool = None, certPath: str = None, debug: bool = None,
                 sslPort: Union[int, str] = "443", gcp: bool = False, jwtToken: str = ""):
        super().__init__(host, graphname, gsqlSecret, username, password, tgCloud, restppPort,
                         gsPort, gsqlVersion, version, apiToken, useCert, certPath, debug, sslPort, gcp, jwtToken)

        self.gds = None
        self.ai = None
        self.mcp_server = None

    def __getattribute__(self, name):
        if name == "gds":
            if super().__getattribute__(name) is None:
                try:
                    from .gds import gds
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
                    from .ai import ai
                    self.ai = ai.AI(self)
                    return super().__getattribute__(name)
                except Exception as e:
                    raise Exception(
                        "Error importing AI submodule. "+str(e)
                    )
            else:
                return super().__getattribute__(name)
        elif name == "mcp":
            # Optional MCP server support
            if super().__getattribute__("mcp_server") is None:
                try:
                    from .mcp import ConnectionManager
                    # Set this connection as the default for MCP tools
                    ConnectionManager.set_default_connection(self)
                    super().__setattr__("mcp_server", True)
                except ImportError:
                    raise Exception(
                        "MCP support requires the 'mcp' extra. "
                        "Install with: pip install pyTigerGraph[mcp]"
                    )
            return super().__getattribute__(name)
        else:
            return super().__getattribute__(name)

    def start_mcp_server(self):
        """Start an MCP server using this connection.

        This method creates an async connection from this sync connection
        and sets it as the default for MCP tools.

        Note: This requires the 'mcp' extra to be installed.
        Install with: pip install pyTigerGraph[mcp]
        """
        try:
            from .mcp import ConnectionManager
            from .pytgasync import AsyncTigerGraphConnection
            # Create async connection from sync connection parameters
            # Get gsqlSecret if it exists (it's set in base class if provided)
            gsql_secret = ""
            if hasattr(self, 'username') and self.username == "__GSQL__secret":
                gsql_secret = self.password
            else:
                gsql_secret = getattr(self, 'gsqlSecret', '')

            async_conn = AsyncTigerGraphConnection(
                host=self.host,
                graphname=self.graphname,
                username=self.username if self.username != "__GSQL__secret" else "tigergraph",
                password=self.password if self.username != "__GSQL__secret" else "tigergraph",
                gsqlSecret=gsql_secret,
                apiToken=self.apiToken,
                jwtToken=self.jwtToken,
                restppPort=self.restppPort,
                gsPort=self.gsPort,
                sslPort=self.sslPort,
                tgCloud=self.tgCloud,
                certPath=self.certPath,
            )
            ConnectionManager.set_default_connection(async_conn)
            return True
        except ImportError:
            raise Exception(
                "MCP support requires the 'mcp' extra. "
                "Install with: pip install pyTigerGraph[mcp]"
            )

# EOF
