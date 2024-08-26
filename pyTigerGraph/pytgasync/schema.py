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
import json
import hashlib
import warnings

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


@dataclass
class Vertex(object):
    """Vertex Object

    Abstract parent class for other types of vertices to be inherited from.
    Contains class methods to edit the attributes associated with the vertex type.

    When defining new vertex types, make sure to include the `primary_id` and `primary_id_as_attribute` class attributes, as these are necessary to define the vertex in TigerGraph.

    For example, to define an AccountHolder vertex type, use:

    ```
    @dataclass
    class AccountHolder(Vertex):
        name: str
        address: str
        accounts: List[str]
        dob: datetime
        some_map: Dict[str, int]
        some_double: "DOUBLE"
        primary_id: str = "name"
        primary_id_as_attribute: bool = True
    ```
    """
    def __init_subclass__(cls):
        """NO DOC: placeholder for class variables"""
        cls.incoming_edge_types = {}
        cls.outgoing_edge_types = {}
        cls._attribute_edits = {"ADD": {}, "DELETE": {}}
        cls.primary_id: Union[str, List[str]]
        cls.primary_id_as_attribute: bool

    @classmethod
    def _set_attr_edit(self, add: dict = None, delete: dict = None):
        """NO DOC: internal updating function for attributes"""
        if add:
            self._attribute_edits["ADD"].update(add)
        if delete:
            self._attribute_edits["DELETE"].update(delete)

    @classmethod
    def _get_attr_edit(self):
        """NO DOC: get attribute edits internal function"""
        return self._attribute_edits

    @classmethod
    def add_attribute(self, attribute_name: str, attribute_type, default_value=None):
        """Function to add an attribute to the given vertex type.

        Args:
            attribute_name (str):
                The name of the attribute to add
            attribute_type (Python type):
                The Python type of the attribute to add. 
                For types that are not supported in Python but are in GSQL, wrap them in quotes; e.g. "DOUBLE"
            default_value (type of attribute, default None):
                The desired default value of the attribute. Defaults to None.
        """
        if attribute_name in self._get_attr_edit()["ADD"].keys():
            warnings.warn(
                attribute_name + " already in staged edits. Overwriting previous edits.")
        for attr in self.attributes:
            if attr == attribute_name:
                raise TigerGraphException(
                    attribute_name + " already exists as an attribute on "+self.__name__ + " vertices")
        attr_type = _py_to_tg_type(attribute_type)
        gsql_add = "ALTER VERTEX "+self.__name__ + \
            " ADD ATTRIBUTE ("+attribute_name+" "+attr_type
        if default_value:
            if attribute_type == str:
                gsql_add += " DEFAULT '"+default_value+"'"
            else:
                gsql_add += " DEFAULT "+str(default_value)
        gsql_add += ");"
        self._set_attr_edit(add={attribute_name: gsql_add})

    @classmethod
    def remove_attribute(self, attribute_name):
        """Function to remove an attribute from the given vertex type.

        Args:
            attribute_name (str):
                The name of the attribute to remove from the vertex.
        """
        if self.primary_id_as_attribute:
            if attribute_name == self.primary_id:
                raise TigerGraphException(
                    "Cannot remove primary ID attribute: "+self.primary_id+".")
        removed = False
        for attr in self.attributes:
            if attr == attribute_name:
                self._set_attr_edit(delete={
                                    attribute_name: "ALTER VERTEX "+self.__name__+" DROP ATTRIBUTE ("+attribute_name+");"})
                removed = True
        if not (removed):
            raise TigerGraphException("An attribute of " + attribute_name +
                                      " is not an attribute on " + self.__name__ + " vertices")

    @classmethod
    @property
    def attributes(self):
        """Class attribute to view the attributes and types of the vertex."""
        return self.__annotations__

    def __getattr__(self, attr):
        if self.attributes.get(attr):
            return self.attributes.get(attr)
        else:
            raise TigerGraphException(
                "No attribute named " + attr + "for vertex type " + self.vertex_type)

    def __eq__(self, lhs):
        return isinstance(lhs, Vertex) and lhs.vertex_type == self.vertex_type

    def __repr__(self):
        return self.vertex_type


@dataclass
class Edge:
    """Edge Object

    Abstract parent class for other types of edges to be inherited from.
    Contains class methods to edit the attributes associated with the edge type.

    When defining new vertex types, make sure to include the required `from_vertex`, `to_vertex`, `reverse_edge`, `is_directed` attributes and optionally the `discriminator` attribute, as these are necessary to define the vertex in TigerGraph.

    For example, to define an HOLDS_ACCOUNT edge type, use:

    ```
    @dataclass
    class HOLDS_ACCOUNT(Edge):
        opened_on: datetime
        from_vertex: Union[AccountHolder, g.vertex_types["Account"]]
        to_vertex: g.vertex_types["Account"]
        is_directed: bool = True
        reverse_edge: str = "ACCOUNT_HELD_BY"
        discriminator: str = "opened_on"
    ```
    """

    def __init_subclass__(cls):
        """NO DOC: placeholder for class variables"""
        cls._attribute_edits = {"ADD": {}, "DELETE": {}}
        cls.is_directed: bool
        cls.reverse_edge: Union[str, bool]
        cls.from_vertex_types: Union[Vertex, List[Vertex]]
        cls.to_vertex_types: Union[Vertex, List[Vertex]]
        cls.discriminator: Union[str, List[str]]

    @classmethod
    def _set_attr_edit(self, add: dict = None, delete: dict = None):
        """NO DOC: function to edit attributes"""
        if add:
            self._attribute_edits["ADD"].update(add)
        if delete:
            self._attribute_edits["DELETE"].update(delete)

    @classmethod
    def _get_attr_edit(self):
        """NO DOC: getter for attribute edits"""
        return self._attribute_edits

    @classmethod
    def add_attribute(self, attribute_name, attribute_type, default_value=None):
        """Function to add an attribute to the given edge type.

        Args:
            attribute_name (str):
                The name of the attribute to add.
            attribute_type (Python type):
                The Python type of the attribute to add. 
                For types that are not supported in Python but are in GSQL, wrap them in quotes; e.g. "DOUBLE"
            default_value (type of attribute, default None):
                The desired default value of the attribute. Defaults to None.
        """
        if attribute_name in self._get_attr_edit()["ADD"].keys():
            warnings.warn(
                attribute_name + " already in staged edits. Overwriting previous edits.")
        for attr in self.attributes:
            if attr == attribute_name:
                raise TigerGraphException(
                    attribute_name + " already exists as an attribute on "+self.__name__ + " edges")
        attr_type = _py_to_tg_type(attribute_type)
        gsql_add = "ALTER EDGE "+self.__name__ + \
            " ADD ATTRIBUTE ("+attribute_name+" "+attr_type
        if default_value:
            if attribute_type == str:
                gsql_add += " DEFAULT '"+default_value+"'"
            else:
                gsql_add += " DEFAULT "+str(default_value)
        gsql_add += ");"
        self._set_attr_edit(add={attribute_name: gsql_add})

    @classmethod
    def remove_attribute(self, attribute_name):
        """Function to remove an attribute from the given edge type.

        Args:
            attribute_name (str):
                The name of the attribute to remove from the edge.
        """
        removed = False
        for attr in self.attributes:
            if attr == attribute_name:
                self._set_attr_edit(delete={
                                    attribute_name: "ALTER EDGE "+self.__name__+" DROP ATTRIBUTE ("+attribute_name+");"})
                removed = True
        if not (removed):
            raise TigerGraphException(
                "An attribute of " + attribute_name + " is not an attribute on " + self.__name__ + " edges")

    @classmethod
    @property
    def attributes(self):
        """Class attribute to view the attributes and types of the vertex."""
        return self.__annotations__

    def __getattr__(self, attr):
        if self.attributes.get(attr):
            return self.attributes.get(attr)
        else:
            raise TigerGraphException(
                "No attribute named " + attr + "for edge type " + self.edge_type)

    def __eq__(self, lhs):
        return isinstance(lhs, Edge) and lhs.edge_type == self.edge_type and lhs.from_vertex_type == self.from_vertex_type and lhs.to_vertex_type == self.to_vertex_type

    def __repr__(self):
        return self.edge_type


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
