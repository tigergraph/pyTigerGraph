"""Object-Oriented Schema
The Object-Oriented Schema functionality allows users to manipulate schema elements in the database in an object-oriented approach in Python.

To add an AccountHolder vertex and a HOLDS_ACCOUNT edge to the Ethereum dataset, simply:

```
from pyTigerGraph import AsyncTigerGraphConnection
from pyTigerGraph.schema import Graph, Vertex, Edge

from datetime import datetime
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, fields

conn = AsyncTigerGraphConnection(host="http://YOUR_HOSTNAME_HERE", graphname="Ethereum")

g = await AsyncGraph.create(conn)


@dataclass
class AccountHolder(Vertex):
    name: str
    address: str
    accounts: List[str]
    dob: datetime
    some_map: Dict[str, int]
    some_double: "DOUBLE"
    primary_id: str = "name"  # always of type string, corresponds to the desired primary ID attribute.
    primary_id_as_attribute: bool = True

@dataclass
class HOLDS_ACCOUNT(Edge):
    opened_on: datetime
    from_vertex: Union[AccountHolder, g.vertex_types["Account"]]
    to_vertex: g.vertex_types["Account"]
    is_directed: bool = True
    reverse_edge: str = "ACCOUNT_HELD_BY"
    discriminator: str = "opened_on"

await g.add_vertex_type(AccountHolder)

await g.add_edge_type(HOLDS_ACCOUNT)

await g.commit_changes()
```

Users can define an entire graph schema in the approach below. Using the Cora dataset example, the schema would look something like this:

```
from pyTigerGraph import AsyncTigerGraphConnection
from pyTigerGraph.schema import AsyncGraph, Vertex, Edge

conn = AsyncTigerGraphConnection("http://YOUR_HOSTNAME_HERE")

g = await AsyncGraph.create()

@dataclass
class Paper(Vertex):
    id: int
    y: int
    x: List[int]
    primary_id: str = "id"
    primary_id_as_attribute: bool = True

@dataclass
class CITES(Edge):
    from_vertex: Paper
    to_vertex: Paper
    is_directed: bool = True
    reverse_edge: str = "R_CITES"

await g.add_vertex_type(Paper)
await g.add_edge_type(CITES)

await g.commit_changes(conn)
```
"""

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraph.pyTigerGraph import TigerGraphConnection
from pyTigerGraph.schema import Graph
from dataclasses import dataclass, make_dataclass, fields, _MISSING_TYPE
from typing import List, Dict, Union, get_origin, get_args
from typing_extensions import Self
from datetime import datetime

BASE_TYPES = ["string", "int", "uint", "float", "double", "bool", "datetime"]
PRIMARY_ID_TYPES = ["string", "int", "uint", "datetime"]
COLLECTION_TYPES = ["list", "set", "map"]
COLLECTION_VALUE_TYPES = ["int", "double",
                          "float", "string", "datetime", "udt"]
MAP_KEY_TYPES = ["int", "string", "datetime"]


def _parse_type(attr):
    """NO DOC: function to parse gsql complex types"""
    collection_types = ""
    if attr["AttributeType"].get("ValueTypeName"):
        if attr["AttributeType"].get("KeyTypeName"):
            collection_types += "<" + attr["AttributeType"].get(
                "KeyTypeName") + "," + attr["AttributeType"].get("ValueTypeName") + ">"
        else:
            collection_types += "<" + \
                attr["AttributeType"].get("ValueTypeName") + ">"
    attr_type = (attr["AttributeType"]["Name"] + collection_types).upper()
    return attr_type


def _get_type(attr_type):
    """NO DOC: function to convert GSQL type to Python type"""
    if attr_type == "STRING":
        return str
    elif attr_type == "INT":
        return int
    elif attr_type == "FLOAT":
        return float
    elif "LIST" in attr_type:
        val_type = attr_type.split("<")[1].strip(">")
        return List[_get_type(val_type)]
    elif "MAP" in attr_type:
        key_val = attr_type.split("<")[1].strip(">")
        key_type = key_val.split(",")[0]
        val_type = key_val.split(",")[1]
        return Dict[_get_type(key_type), _get_type(val_type)]
    elif attr_type == "BOOL":
        return bool
    elif attr_type == "datetime":
        return datetime
    else:
        return attr_type


def _py_to_tg_type(attr_type):
    """NO DOC: function to convert Python type to GSQL type"""
    if attr_type == str:
        return "STRING"
    elif attr_type == int:
        return "INT"
    elif attr_type == float:
        return "FLOAT"
    elif attr_type == list:
        raise TigerGraphException("Must define value type within list")
    elif attr_type == dict:
        raise TigerGraphException(
            "Must define key and value types within dictionary/map")
    elif attr_type == datetime:
        return "DATETIME"
    elif (str(type(attr_type)) == "<class 'typing._GenericAlias'>") and attr_type._name == "List":
        val_type = _py_to_tg_type(attr_type.__args__[0])
        if val_type.lower() in COLLECTION_VALUE_TYPES:
            return "LIST<"+val_type+">"
        else:
            raise TigerGraphException(
                val_type + " not a valid type for the value type in LISTs.")
    elif (str(type(attr_type)) == "<class 'typing._GenericAlias'>") and attr_type._name == "Dict":
        key_type = _py_to_tg_type(attr_type.__args__[0])
        val_type = _py_to_tg_type(attr_type.__args__[1])
        if key_type.lower() in MAP_KEY_TYPES:
            if val_type.lower() in COLLECTION_VALUE_TYPES:
                return "MAP<"+key_type+","+val_type+">"
            else:
                raise TigerGraphException(
                    val_type + " not a valid type for the value type in MAPs.")
        else:
            raise TigerGraphException(
                key_type + " not a valid type for the key type in MAPs.")
    else:
        if str(attr_type).lower() in BASE_TYPES:
            return str(attr_type).upper()
        else:
            raise TigerGraphException(
                attr_type+"not a valid TigerGraph datatype.")


class AsyncGraph(Graph):
    """Graph Object

    The graph object can be used in conjunction with a TigerGraphConnection to retrieve the schema of the connected graph.
    Serves as the way to collect the definitions of Vertex and Edge types.

    To instantiate the graph object with a connection to an existing graph, use:
    ```
    from pyTigerGraph.schema import Graph

    g = Graph(conn)
    ```
    """
    # __init__ is being replaced by create() so we can make it async to call getSchema()
    @classmethod
    async def create(cls, conn: TigerGraphConnection = None) -> Self:
        """Graph class for schema representation.

        Args:
            conn (TigerGraphConnection, optional):
                Connection to a TigerGraph database. Defaults to None.
        """
        self = cls()
        self._vertex_types = {}
        self._edge_types = {}
        self._vertex_edits = {"ADD": {}, "DELETE": {}}
        self._edge_edits = {"ADD": {}, "DELETE": {}}
        if conn:
            db_rep = await conn.getSchema(force=True)
            self.setUpConn(conn, db_rep)
        return self

    def __init__(self):
        pass

    async def commit_changes(self, conn: TigerGraphConnection = None):
        """Commit schema changes to the graph.
        Args:
            conn (TigerGraphConnection, optional):
                Connection to the database to edit the schema of.
                Not required if the Graph was instantiated with a connection object.        
        """
        if not conn:
            if self.conn:
                conn = self.conn
            else:
                raise TigerGraphException(
                    "No Connection Defined. Please instantiate a TigerGraphConnection to the database to commit the schema.")

        if "does not exist." in await conn.gsql("USE GRAPH "+conn.graphname):
            await conn.gsql("CREATE GRAPH "+conn.graphname+"()")
        start_gsql = self._parsecommit_changes(conn)
        res = await conn.gsql(start_gsql)
        if "updated to new version" in res:
            # reset self
            self._vertex_types = {}
            self._edge_types = {}
            self._vertex_edits = {"ADD": {}, "DELETE": {}}
            self._edge_edits = {"ADD": {}, "DELETE": {}}
            db_rep = await conn.getSchema(force=True)
            self.setUpConn(conn, db_rep)
        else:
            raise TigerGraphException(
                "Schema change failed with message:\n"+res)
