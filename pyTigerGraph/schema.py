"""Object-Oriented Schema
The Object-Oriented Schema functionality allows users to manipulate schema elements in the database in an object-oriented approach in Python.

To add an AccountHolder vertex and a HOLDS_ACCOUNT edge to the Ethereum dataset, simply:

```py
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.schema import Graph, Vertex, Edge

from datetime import datetime
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, fields

conn = TigerGraphConnection(host="http://YOUR_HOSTNAME_HERE", graphname="Ethereum")

g = Graph(conn)


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

g.add_vertex_type(AccountHolder)

g.add_edge_type(HOLDS_ACCOUNT)

g.commit_changes()
```

Users can define an entire graph schema in the approach below. Using the Cora dataset example, the schema would look something like this:

```
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.schema import Graph, Vertex, Edge

conn = TigerGraphConnection("http://YOUR_HOSTNAME_HERE")

g = Graph()

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

g.add_vertex_type(Paper)
g.add_edge_type(CITES)

g.commit_changes(conn)
```
"""

import json
import hashlib
import warnings

from typing import List, Dict, Union, get_origin, get_args
from datetime import datetime
from dataclasses import dataclass, make_dataclass, fields, _MISSING_TYPE

from pyTigerGraph.common.exception import TigerGraphException
from pyTigerGraph.pyTigerGraph import TigerGraphConnection


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


class Graph():
    """Graph Object

    The graph object can be used in conjunction with a TigerGraphConnection to retrieve the schema of the connected graph.
    Serves as the way to collect the definitions of Vertex and Edge types.

    To instantiate the graph object with a connection to an existing graph, use:
    ```
    from pyTigerGraph.schema import Graph

    g = Graph(conn)
    ```
    """

    def __init__(self, conn: TigerGraphConnection = None):
        """Graph class for schema representation.

        Args:
            conn (TigerGraphConnection, optional):
                Connection to a TigerGraph database. Defaults to None.
        """
        self._vertex_types = {}
        self._edge_types = {}
        self._vertex_edits = {"ADD": {}, "DELETE": {}}
        self._edge_edits = {"ADD": {}, "DELETE": {}}
        if conn:
            db_rep = conn.getSchema(force=True)
            self.setUpConn(conn, db_rep)

    def setUpConn(self, conn, db_rep):
        """NO DOC: function to set up the connection"""
        self.graphname = db_rep["GraphName"]
        for v_type in db_rep["VertexTypes"]:
            vert = make_dataclass(v_type["Name"],
                                  [(attr["AttributeName"], _get_type(_parse_type(attr)), None) for attr in v_type["Attributes"]] +
                                  [(v_type["PrimaryId"]["AttributeName"], _get_type(_parse_type(v_type["PrimaryId"])), None),
                                   ("primary_id", str,
                                    v_type["PrimaryId"]["AttributeName"]),
                                   ("primary_id_as_attribute", bool, v_type["PrimaryId"].get("PrimaryIdAsAttribute", False))],
                                  bases=(Vertex,), repr=False)
            self._vertex_types[v_type["Name"]] = vert

        for e_type in db_rep["EdgeTypes"]:
            if e_type["FromVertexTypeName"] == "*":
                source_vertices = [self._vertex_types[x["From"]]
                                   for x in e_type["EdgePairs"]]
            else:
                source_vertices = self._vertex_types[e_type["FromVertexTypeName"]]
            if e_type["ToVertexTypeName"] == "*":
                target_vertices = [self._vertex_types[x["To"]]
                                   for x in e_type["EdgePairs"]]
            else:
                target_vertices = self._vertex_types[e_type["ToVertexTypeName"]]

            e = make_dataclass(e_type["Name"],
                               [(attr["AttributeName"], _get_type(_parse_type(attr)), None) for attr in e_type["Attributes"]] +
                               [("from_vertex", source_vertices, None),
                                ("to_vertex", target_vertices, None),
                                ("is_directed", bool,
                                 e_type["IsDirected"]),
                                ("reverse_edge", str, e_type["Config"].get("REVERSE_EDGE"))],
                               bases=(Edge,), repr=False)
            if isinstance(target_vertices, list):
                for tgt_v in target_vertices:
                    tgt_v.incoming_edge_types[e_type["Name"]] = e
            else:
                target_vertices.incoming_edge_types[e_type["Name"]] = e
            if isinstance(source_vertices, list):
                for src_v in source_vertices:
                    src_v.outgoing_edge_types[e_type["Name"]] = e
            else:
                source_vertices.outgoing_edge_types[e_type["Name"]] = e

            self._edge_types[e_type["Name"]] = e
        self.conn = conn

    def add_vertex_type(self, vertex: Vertex, outdegree_stats=True):
        """Add a vertex type to the list of changes to commit to the graph.

        Args:
            vertex (Vertex):
                The vertex type definition to add to the addition cache.
            outdegree_stats (bool, optional):
                Whether or not to include "WITH OUTEGREE_STATS=TRUE" in the schema definition.
                Used for caching outdegree, defaults to True.
        """
        if vertex.__name__ in self._vertex_types.keys():
            raise TigerGraphException(
                vertex.__name__+" already exists in the database")
        if vertex.__name__ in self._vertex_edits.keys():
            warnings.warn(
                vertex.__name__ + " already in staged edits. Overwriting previous edits.")
        gsql_def = "ADD VERTEX "+vertex.__name__+"("
        attrs = vertex.attributes
        primary_id = None
        primary_id_as_attribute = None
        primary_id_type = None
        for field in fields(vertex):
            if field.name == "primary_id":
                primary_id = field.default
                primary_id_type = field.type
            if field.name == "primary_id_as_attribute":
                primary_id_as_attribute = field.default

        if not (primary_id):
            raise TigerGraphException(
                "primary_id of vertex type "+str(vertex.__name__)+" not defined")

        if not (primary_id_as_attribute):
            raise TigerGraphException(
                "primary_id_as_attribute of vertex type "+str(vertex.__name__)+" not defined")

        if not (_py_to_tg_type(primary_id_type).lower() in PRIMARY_ID_TYPES):
            raise TigerGraphException(
                str(primary_id_type), "is not a supported type for primary IDs.")

        gsql_def += "PRIMARY_ID "+primary_id + \
            " "+_py_to_tg_type(primary_id_type)
        for attr in attrs.keys():
            if attr == primary_id or attr == "primary_id" or attr == "primary_id_as_attribute":
                continue
            else:
                gsql_def += ", "
                gsql_def += attr + " "+_py_to_tg_type(attrs[attr])
        gsql_def += ")"
        if outdegree_stats:
            gsql_def += ' WITH STATS="OUTDEGREE_BY_EDGETYPE"'
        if outdegree_stats and primary_id_as_attribute:
            gsql_def += ", "
        if primary_id_as_attribute:
            gsql_def += 'PRIMARY_ID_AS_ATTRIBUTE="true"'
        gsql_def += ";"
        self._vertex_edits["ADD"][vertex.__name__] = gsql_def

    def add_edge_type(self, edge: Edge):
        """Add an edge type to the list of changes to commit to the graph.

        Args:
            edge (Edge):
                The edge type definition to add to the addition cache.
        """
        if edge in self._edge_types.values():
            raise TigerGraphException(
                edge.__name__+" already exists in the database")
        if edge in self._edge_edits.values():
            warnings.warn(
                edge.__name__ + " already in staged edits. Overwriting previous edits")
        attrs = edge.attributes
        is_directed = None
        reverse_edge = None
        discriminator = None
        for field in fields(edge):
            if field.name == "is_directed":
                is_directed = field.default
            if field.name == "reverse_edge":
                reverse_edge = field.default

            if field.name == "discriminator":
                discriminator = field.default

        if not (reverse_edge) and is_directed:
            raise TigerGraphException(
                "Reverse edge definition not set. Set the reverse_edge variable to a boolean or string.")
        if is_directed is None:
            raise TigerGraphConnection(
                "is_directed variable not defined. Define is_directed as a class variable to the desired setting.")

        if not (edge.attributes.get("from_vertex", None)):
            raise TigerGraphException(
                "from_vertex is not defined. Define from_vertex class variable.")

        if not (edge.attributes.get("to_vertex", None)):
            raise TigerGraphException(
                "to_vertex is not defined. Define to_vertex class variable.")

        gsql_def = ""
        if is_directed:
            gsql_def += "ADD DIRECTED EDGE "+edge.__name__+"("
        else:
            gsql_def += "ADD UNDIRECTED EDGE "+edge.__name__+"("

        if not (get_origin(edge.attributes["from_vertex"]) is Union) and not (get_origin(edge.attributes["to_vertex"]) is Union):
            from_vert = edge.attributes["from_vertex"].__name__
            to_vert = edge.attributes["to_vertex"].__name__
            gsql_def += "FROM "+from_vert+", "+"TO "+to_vert
        elif get_origin(edge.attributes["from_vertex"]) is Union and not (get_origin(edge.attributes["to_vertex"]) is Union):
            print(get_args(edge.attributes["from_vertex"]))
            for v in get_args(edge.attributes["from_vertex"]):
                from_vert = v.__name__
                to_vert = edge.attributes["to_vertex"].__name__
                gsql_def += "FROM "+from_vert+", "+"TO "+to_vert + "|"
            gsql_def = gsql_def[:-1]
        elif not (get_origin(edge.attributes["from_vertex"]) is Union) and get_origin(edge.attributes["to_vertex"]) is Union:
            for v in get_args(edge.attributes["to_vertex"]):
                from_vert = edge.attributes["from_vertex"].__name__
                to_vert = v.__name__
                gsql_def += "FROM "+from_vert+", "+"TO "+to_vert + "|"
            gsql_def = gsql_def[:-1]
        elif get_origin(edge.attributes["from_vertex"]) is Union and get_origin(edge.attributes["to_vertex"]) is Union:
            if len(get_args(edge.attributes["from_vertex"])) != len(get_args(edge.attributes["to_vertex"])):
                raise TigerGraphException(
                    "from_vertex and to_vertex list have different lengths.")
            else:
                for i in range(len(get_args(edge.attributes["from_vertex"]))):
                    from_vert = get_args(edge.attributes["from_vertex"])[
                        i].__name__
                    to_vert = get_args(edge.attributes["to_vertex"])[
                        i].__name__
                    gsql_def += "FROM "+from_vert+", "+"TO "+to_vert + "|"
                gsql_def = gsql_def[:-1]
        else:
            raise TigerGraphException(
                "from_vertex and to_vertex parameters have to be of type Union[Vertex, Vertex, ...] or Vertex")

        if discriminator:
            if isinstance(discriminator, list):
                gsql_def += ", DISCRIMINATOR("
                for attr in discriminator:
                    attr + " "+_py_to_tg_type(attrs[attr]) + ", "
                gsql_def = gsql_def[:-2]
                gsql_def += ")"
            elif isinstance(discriminator, str):
                gsql_def += ", DISCRIMINATOR("+discriminator + \
                    " "+_py_to_tg_type(attrs[discriminator])+")"
            else:
                raise TigerGraphException(
                    "Discriminator definitions can only be of type string (one discriminator) or list (compound discriminator)")
        for attr in attrs.keys():
            if attr == "from_vertex" or attr == "to_vertex" or attr == "is_directed" or attr == "reverse_edge" or (discriminator and attr in discriminator) or attr == "discriminator":
                continue
            else:
                gsql_def += ", "
                gsql_def += attr + " "+_py_to_tg_type(attrs[attr])
        gsql_def += ")"
        if reverse_edge:
            if isinstance(reverse_edge, str):
                gsql_def += ' WITH REVERSE_EDGE="'+reverse_edge+'"'
            elif isinstance(reverse_edge, bool):
                gsql_def += ' WITH REVERSE_EDGE="reverse_'+edge.__name__+'"'
            else:
                raise TigerGraphException(
                    "Reverse edge name of type: "+str(type(attrs["reverse_edge"])+" is not supported."))
        gsql_def += ";"
        self._edge_edits["ADD"][edge.__name__] = gsql_def

    def remove_vertex_type(self, vertex: Vertex):
        """Add a vertex type to the list of changes to remove from the graph.

        Args:
            vertex (Vertex):
                The vertex type definition to add to the removal cache.
        """
        gsql_def = "DROP VERTEX "+vertex.__name__+";"
        self._vertex_edits["DELETE"][vertex.__name__] = gsql_def

    def remove_edge_type(self, edge: Edge):
        """Add an edge type to the list of changes to remove from the graph.

        Args:
            edge (Edge):
                The edge type definition to add to the removal cache.
        """
        gsql_def = "DROP EDGE "+edge.__name__+";"
        self._edge_edits["DELETE"][edge.__name__] = gsql_def

    def _parsecommit_changes(self, conn):
        all_attr = [x._attribute_edits for x in list(
            self._vertex_types.values()) + list(self._edge_types.values())]
        # need to remove the changes locally
        for elem in list(self._vertex_types.values()) + list(self._edge_types.values()):
            elem._attribute_edits = {"ADD": {}, "DELETE": {}}
        all_attribute_edits = {"ADD": {}, "DELETE": {}}
        for change in all_attr:
            all_attribute_edits["ADD"].update(change["ADD"])
            all_attribute_edits["DELETE"].update(change["DELETE"])
        md5 = hashlib.md5()
        md5.update(json.dumps(
            {**self._vertex_edits, **self._edge_edits, **all_attribute_edits}).encode())
        job_name = "pytg_change_"+md5.hexdigest()
        start_gsql = "USE GRAPH "+conn.graphname+"\n"
        start_gsql += "DROP JOB "+job_name+"\n"
        start_gsql += "CREATE SCHEMA_CHANGE JOB " + \
            job_name + " FOR GRAPH " + conn.graphname + " {\n"
        for v_to_add in self._vertex_edits["ADD"]:
            start_gsql += self._vertex_edits["ADD"][v_to_add] + "\n"
        for e_to_add in self._edge_edits["ADD"]:
            start_gsql += self._edge_edits["ADD"][e_to_add] + "\n"
        for v_to_drop in self._vertex_edits["DELETE"]:
            start_gsql += self._vertex_edits["DELETE"][v_to_drop] + "\n"
        for e_to_drop in self._edge_edits["DELETE"]:
            start_gsql += self._edge_edits["DELETE"][e_to_drop] + "\n"
        for attr_to_add in all_attribute_edits["ADD"]:
            start_gsql += all_attribute_edits["ADD"][attr_to_add] + "\n"
        for attr_to_drop in all_attribute_edits["DELETE"]:
            start_gsql += all_attribute_edits["DELETE"][attr_to_drop] + "\n"
        start_gsql += "}\n"
        start_gsql += "RUN SCHEMA_CHANGE JOB "+job_name
        return start_gsql

    def commit_changes(self, conn: TigerGraphConnection = None):
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

        if "does not exist." in conn.gsql("USE GRAPH "+conn.graphname):
            conn.gsql("CREATE GRAPH "+conn.graphname+"()")
        start_gsql = self._parsecommit_changes(conn)
        res = conn.gsql(start_gsql)
        if "updated to new version" in res:
            self.__init__(conn)
        else:
            raise TigerGraphException(
                "Schema change failed with message:\n"+res)

    @property
    def vertex_types(self):
        """Vertex types property."""
        return self._vertex_types

    @property
    def edge_types(self):
        """Edge types property."""
        return self._edge_types
