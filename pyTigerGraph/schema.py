from pyTigerGraph.pyTigerGraphException import TigerGraphException
from dataclasses import dataclass, make_dataclass, fields, _MISSING_TYPE
from typing import List, Dict, Union
from datetime import datetime
import warnings

BASE_TYPES  = ["string", "int", "uint", "float", "double", "bool", "datetime"]
PRIMARY_ID_TYPES = ["string", "int", "uint", "datetime"]
COLLECTION_TYPES = ["list", "set", "map"]
COLLECTION_VALUE_TYPES = ["int", "double", "float", "string", "datetime", "udt"]
MAP_KEY_TYPES = ["int", "string", "datetime"]

def _get_type(attr):
    collection_types = ""
    if attr["AttributeType"].get("ValueTypeName"):
        if attr["AttributeType"].get("KeyTypeName"):
            collection_types += "<"+ attr["AttributeType"].get("KeyTypeName") + "," + attr["AttributeType"].get("ValueTypeName") + ">"
        else:
            collection_types += "<"+attr["AttributeType"].get("ValueTypeName") + ">"
    attr_type = (attr["AttributeType"]["Name"] + collection_types).upper()
    if attr_type == "STRING":
        return str
    elif attr_type == "INT":
        return int
    elif attr_type == "FLOAT":
        return float
    elif "LIST" in attr_type:
        return list
    elif "MAP" in attr_type:
        return dict
    elif attr_type == "BOOL":
        return bool
    elif attr_type == "datetime":
        return datetime
    else:
        return attr_type


def _py_to_tg_type(attr_type):
    if attr_type == str:
        return "STRING"
    elif attr_type == int:
        return "INT"
    elif attr_type == float:
        return "FLOAT"
    elif attr_type == list:
        raise TigerGraphException("Must define value type within list")
    elif attr_type == dict:
        raise TigerGraphException("Must define key and value types within dictionary/map")
    elif attr_type == datetime:
        return "DATETIME"
    elif (str(type(attr_type)) == "<class 'typing._GenericAlias'>") and attr_type._name == "List":
        val_type = _py_to_tg_type(attr_type.__args__[0])
        if val_type.lower() in COLLECTION_VALUE_TYPES:
            return "LIST<"+val_type+">"
        else:
            raise TigerGraphException(val_type + " not a valid type for the value type in LISTs.")
    elif (str(type(attr_type)) == "<class 'typing._GenericAlias'>") and attr_type._name == "Dict":
        key_type = _py_to_tg_type(attr_type.__args__[0])
        val_type = _py_to_tg_type(attr_type.__args__[1])
        if key_type.lower() in MAP_KEY_TYPES:
            if val_type.lower() in COLLECTION_VALUE_TYPES:
                return "MAP<"+key_type+","+val_type+">"
            else:
                raise TigerGraphException(val_type + " not a valid type for the value type in MAPs.")
        else:
            raise TigerGraphException(key_type + " not a valid type for the key type in MAPs.")
    else:
        print(attr_type)
        if str(attr_type).lower() in BASE_TYPES:
            return str(attr_type).upper()
        else:
            raise TigerGraphException(attr_type+"not a valid TigerGraph datatype.")



@dataclass
class Vertex(object):
    attribute_edits = {"ADD": [], "DELETE": []}
    incoming_edge_types = []
    outgoing_edge_types = []

    @classmethod
    def define_primary_id(self, primary_id, primary_id_as_attribute):
        self.primary_id = primary_id
        self.primary_id_as_attribute = primary_id_as_attribute
        self.primary_id_type = self.__annotations__[primary_id]
        if not(_py_to_tg_type(self.primary_id_type).lower() in PRIMARY_ID_TYPES):
            raise TigerGraphException(self.primary_id_type, "is not a supported type for primary IDs.")

    @classmethod
    def add_attribute(self, attribute, attribute_type):
        for attr in self.attributes.values():
            if attr == attribute:
                raise TigerGraphException(attribute + " already exists as an attribute on "+self.vertex_type + " vertices")
        self.attribute_edits["ADD"].append(attribute)

    @classmethod
    def remove_attribute(self, attribute_name):
        if self.primary_id_as_attribute:
            if attribute_name == self.primary_id:
                raise TigerGraphException("Cannot remove primary ID attribute: "+self.primary_id+".")
        removed = False
        for attr in self.attributes.values():
            if attr.attribute_name == attribute_name:
                self.attribute_edits["DELETE"].append(attribute_name)
                removed = True
        if not(removed):
            raise TigerGraphException("An attribute of "+ attribute_name + " is not an attribute on "+ self.vertex_type + " vertices")

    @classmethod
    @property
    def attributes(self):
        return self.__annotations__

    def __getattr__(self, attr):
        if self.attributes.get(attr):
            return self.attributes.get(attr)
        else:
            raise TigerGraphException("No attribute named "+ attr + "for vertex type " + self.vertex_type)

    def __eq__(self, lhs):
        return isinstance(lhs, Vertex) and lhs.vertex_type == self.vertex_type
    
    def __repr__(self):
        return self.vertex_type

@dataclass
class Edge:
    attribute_edits = {"ADD": [], "DELETE": []}
    is_directed = None
    reverse_edge = None
    from_vertex_types = []
    to_vertex_types = []

    def add_attribute(self, attribute):
        for attr in self.attributes.values():
            if attr == attribute:
                raise TigerGraphException(attribute + " already exists as an attribute on "+self.edge_type + " edges")
        self.attribute_edits["ADD"].append(attribute)

    def remove_attribute(self, attribute_name):
        removed = False
        for attr in self.attributes.values():
            if attr.attribute_name == attribute_name:
                self.attribute_edits["DELETE"].append(attribute_name)
                removed = True
        if not(removed):
            raise TigerGraphException("An attribute of "+ attribute_name + " is not an attribute on "+ self.edge_type + " edges")

    @classmethod
    @property
    def attributes(self):
        return self.__annotations__

    def __getattr__(self, attr):
        if self.attributes.get(attr):
            return self.attributes.get(attr)
        else:
            raise TigerGraphException("No attribute named "+ attr + "for edge type " + self.edge_type)

    def __eq__(self, lhs):
        return isinstance(lhs, Edge) and lhs.edge_type == self.edge_type and lhs.from_vertex_type == self.from_vertex_type and lhs.to_vertex_type == self.to_vertex_type

    def __repr__(self):
        return self.edge_type

class Graph():
    def __init__(self, conn=None):
        self._vertex_types = {}
        self._edge_types = {}
        self._vertex_edits = {"ADD": {}, "DELETE": {}}
        self._edge_edits = {"ADD": [], "DELETE": []}
        if conn:
            db_rep = conn.getSchema()
            self.graphname = db_rep["GraphName"]
            for v_type in db_rep["VertexTypes"]:
                vert = make_dataclass(v_type["Name"],
                                    [(attr["AttributeName"], _get_type(attr), None) for attr in v_type["Attributes"]] + [(v_type["PrimaryId"]["AttributeName"], _get_type(v_type["PrimaryId"]), None)],
                                    bases=(Vertex,), repr=False)
                vert.define_primary_id(v_type["PrimaryId"]["AttributeName"], v_type["PrimaryId"]["PrimaryIdAsAttribute"])
                self._vertex_types[v_type["Name"]] = vert

            for e_type in db_rep["EdgeTypes"]:
                e = make_dataclass(e_type["Name"],
                                    [(attr["AttributeName"], _get_type(attr), None) for attr in v_type["Attributes"]],
                                    bases=(Edge,), repr=False)

                self._edge_types[(e_type["FromVertexTypeName"],
                                 e_type["Name"], 
                                 e_type["ToVertexTypeName"])] = e
                
                e.is_directed = e_type["IsDirected"]
                e.reverse_edge = e_type["Config"].get("REVERSE_EDGE")
                e.from_vertex_types.append(self._vertex_types[e_type["FromVertexTypeName"]])
                e.to_vertex_types.append(self._vertex_types[e_type["ToVertexTypeName"]])

            for e in list(self._edge_types.keys()):
                src_vt = e[0]
                dest_vt = e[2]
                self._vertex_types[src_vt].outgoing_edge_types.append(self._edge_types[e])
                self._vertex_types[dest_vt].incoming_edge_types.append(self._edge_types[e])

    def create_vertex_type(self, vertex: Vertex, outdegree_stats=True):
        if vertex.__name__ in self._vertex_types.keys():
            raise TigerGraphException(vertex.__name__+" already exists in the database")
        if vertex.__name__ in self._vertex_edits.keys():
            warnings.warn(vertex.__name__ + " already in staged edits. Overwriting previous edits.")
        gsql_def = "ADD VERTEX "+vertex.__name__+"("
        attrs = vertex.attributes
        try:
            primary_id = vertex.primary_id
            primary_id_type = vertex.primary_id_type
            primary_id_as_attr = vertex.primary_id_as_attribute
        except:
            raise TigerGraphException("Primary ID not defined. Run Vertex.define_primary_id() to define the Primary ID")
        gsql_def += "PRIMARY_ID "+primary_id+" "+_py_to_tg_type(primary_id_type)
        for attr in attrs.keys():
            if attr == primary_id:
                continue
            else:
                gsql_def += ", "
                gsql_def += attr + " "+_py_to_tg_type(attrs[attr])
        gsql_def += ")"
        if outdegree_stats:
            gsql_def += " WITH STATS='OUTDEGREE_BY_EDGETYPE'"
        if outdegree_stats and primary_id_as_attr:
            gsql_def += ", "
        if primary_id_as_attr:
            gsql_def += "PRIMARY_ID_AS_ATTRIBUTE='true'"
        gsql_def += ";"
        self._vertex_edits["ADD"][vertex.__name__] = gsql_def

    def create_edge_type(self, edge: Edge, directed: bool=True, reverse_edge: Union[str, bool]=True):
        if edge in self._edge_types.values():
            raise TigerGraphException(edge.__name__+" already exists in the database")
        if edge in self._edge_edits.values():
            warnings.warn(edge.__name__ + " already in staged edits. Overwriting previous edits")
        gsql_def = ""
        if directed:
            gsql_def += "ADD DIRECTED EDGE "+edge.__name__+"("
        else:
            gsql_def += "ADD UNDIRECTED EDGE "+edge.__name__+"("
        from_vert = edge.from_vertex_types
        to_vert = edge.to_vertex_types

        if len(from_vert) < 1:
            raise TigerGraphException(edge.__name__+ " has no from vertex types defined.")
        if len(to_vert) < 1:
            raise TigerGraphException(edge.__name__+ " has no to vertex types defined.")
        if len(from_vert) != len(to_vert):
            raise TigerGraphException(edge.__name__+ " has an uneven pairing of to and from vertices.")

        for pair in zip(from_vert, to_vert):
            gsql_def += "FROM "+pair[0].__name__+", "+"TO "+pair[1].__name__ + " | "
        
        gsql_def = gsql_def[:-3]
        print(gsql_def)



    @property
    def vertex_types(self):
        return self._vertex_types

    @property
    def edge_types(self):
        return self._edge_types