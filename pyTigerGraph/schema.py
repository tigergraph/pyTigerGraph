from pyTigerGraph.pyTigerGraphException import TigerGraphException
from dataclasses import dataclass, make_dataclass

BASE_TYPES  = ["string", "int", "uint", "float", "double", "bool", "datetime"]
PRIMARY_ID_TYPES = ["string", "int", "uint", "datetime"]
COLLECTION_TYPES = ["list", "set", "map"]
COLLECTION_VALUE_TYPES = ["int", "double", "float", "string", "datetime", "udt"]
MAP_KEY_TYPES = ["int", "string", "datetime"]

'''
@dataclass
class Attribute():
    def __init__(self, attribute_name, attribute_type, default_value = None):
        self.attribute_name = attribute_name
        # check if valid type
        if attribute_type.lower() in BASE_TYPES:
            self.attribute_type = attribute_type
        elif attribute_type.lower().split("<")[0] in COLLECTION_TYPES:
            ct = attribute_type.split("<")[0]
            if ct.lower() == "list" or ct.lower() == "set":
                cvt = attribute_type.split("<")[1].strip(">")
                if not(cvt.lower() in COLLECTION_VALUE_TYPES):
                    TigerGraphException(cvt + " is not a supported datatype to create attribute of type " + ct)
            elif ct.lower() == "map":
                keyt = attribute_type.split("<")[1].split(",")[0]
                valt = attribute_type.split("<")[1].split(",")[1].strip(">")
                if not(valt.lower() in COLLECTION_VALUE_TYPES):
                    TigerGraphException(valt + " is not a supported datatype to create value attribute of type " + ct)
                if not(keyt.lower() in COLLECTION_VALUE_TYPES):
                    TigerGraphException(keyt + " is not a supported datatype to create value attribute of type " + ct)
            else:
                TigerGraphException(ct + " is not a valid container datatype")
        else:
            TigerGraphException(attribute_type + " is not a recognized datatype for TigerGraph databases")
        self.attribute_type = attribute_type
    
    def __eq__(self, lhs):
        return isinstance(lhs, Attribute) and lhs.attribute_name == self.attribute_name

    def __repr__(self):
        return self.attribute_name +" of type "+self.attribute_type
'''


@dataclass
class Vertex:
    def __init__(self, vertex_type, primary_id_name, primary_id_as_attribute, primary_id_type):
        self.vertex_type = vertex_type
        self.attribute_edits = {"ADD": [], "DELETE": []}
        self.incoming_edge_types = []
        self.outgoing_edge_types = []
        self.primary_id = primary_id_name
        self.primary_id_as_attribute = primary_id_as_attribute
        self.primary_id_type = primary_id_type
        if not(primary_id_type.lower() in PRIMARY_ID_TYPES):
            raise TigerGraphException(primary_id_type, "is not a supported type for primary IDs.")
    '''
    def add_attribute(self, attribute):
        for attr in self.attributes.values():
            if attr == attribute:
                raise TigerGraphException(attribute + " already exists as an attribute on "+self.vertex_type + " vertices")
        self.attribute_edits["ADD"].append(attribute)

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
    '''
    @classmethod
    def _add_attr(self, attribute_name, attribute_type):
        self.__class__ = make_dataclass(self.__name__, fields=[(attribute_name, attribute_type)], bases=(self, ))

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
    def __init__(self, edge_type, from_vertex_type, to_vertex_type, is_directed, reverse_edge):
        self.edge_type = edge_type
        self.attribute_edits = {"ADD": [], "DELETE": []}
        self.attributes = {}
        self.is_directed = is_directed
        self.reverse_edge = reverse_edge
        self.from_vertex_type = from_vertex_type
        self.to_vertex_type = to_vertex_type

    '''
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

    def update_attributes(self, attributes):
        self.attributes = []
        for attr in attributes:
            self.attributes.append(Attribute(attr["AttributeName"], attr["AttributeType"]))
    '''
    @classmethod
    def _add_attr(self, attribute_name, attribute_type):
        self.__class__ = make_dataclass(self.__name__, fields=[(attribute_name, attribute_type)], bases=(self, ))

    def __getattr__(self, attr):
        if self.attributes.get(attr):
            return self.attributes.get(attr)
        else:
            raise TigerGraphException("No attribute named "+ attr + "for edge type " + self.edge_type)

    def __eq__(self, lhs):
        return isinstance(lhs, Edge) and lhs.edge_type == self.edge_type and lhs.from_vertex_type == self.from_vertex_type and lhs.to_vertex_type == self.to_vertex_type

    def __repr__(self):
        return self.edge_type

@dataclass
class Graph():
    def __init__(self, conn=None):
        self._vertex_types = {}
        self._edge_types = {}
        self._vertex_edits = {"ADD": [], "DELETE": []}
        self._edge_edits = {"ADD": [], "DELETE": []}
        if conn:
            db_rep = conn.getSchema()
            self.graphname = db_rep["GraphName"]
            for v_type in db_rep["VertexTypes"]:
                v = type(v_type["Name"], (Vertex, object), {})
                self._vertex_types[v_type["Name"]] = v(v_type["Name"],
                                                      v_type["PrimaryId"]["AttributeName"],
                                                      v_type["PrimaryId"]["PrimaryIdAsAttribute"],
                                                      v_type["PrimaryId"]["AttributeType"]["Name"])
                for attr in v_type["Attributes"]:
                    '''
                    a = type(attr["AttributeName"], (Attribute, object), {})
                    collection_types = ""
                    if attr["AttributeType"].get("ValueTypeName"):
                        if attr["AttributeType"].get("KeyTypeName"):
                            collection_types += "<"+ attr["AttributeType"].get("KeyTypeName") + "," + attr["AttributeType"].get("ValueTypeName") + ">"
                        else:
                            collection_types += "<"+attr["AttributeType"].get("ValueTypeName") + ">"
                    attr_type = attr["AttributeType"]["Name"] + collection_types
                    self._vertex_types[v_type["Name"]].attributes[attr["AttributeName"]] = a(attr["AttributeName"],
                                                                                            attr_type)
                    '''
                    self._vertex_types[v_type["Name"]]._add_attr(attr["AttributeName"], str)
            for e_type in db_rep["EdgeTypes"]:
                e = type(e_type["Name"], (Edge, object), {})
                self._edge_types[(e_type["FromVertexTypeName"],
                                 e_type["Name"], 
                                 e_type["ToVertexTypeName"])] = e(e_type["Name"],
                                                                  self._vertex_types[e_type["FromVertexTypeName"]],
                                                                  self._vertex_types[e_type["ToVertexTypeName"]],
                                                                  e_type["IsDirected"],
                                                                  e_type["Config"])
                for attr in e_type["Attributes"]:
                    '''
                    a = type(attr["AttributeName"], (Attribute, object), {})
                    collection_types = ""
                    if attr["AttributeType"].get("ValueTypeName"):
                        if attr["AttributeType"].get("KeyTypeName"):
                            collection_types += "<"+ attr["AttributeType"].get("KeyTypeName") + "," + attr["AttributeType"].get("ValueTypeName") + ">"
                        else:
                            collection_types += "<"+attr["AttributeType"].get("ValueTypeName") + ">"
                    attr_type = attr["AttributeType"]["Name"] + collection_types
                    self._edge_types[(e_type["FromVertexTypeName"],
                                     e_type["Name"], 
                                     e_type["ToVertexTypeName"])].attributes[attr["AttributeName"]] = a(attr["AttributeName"],
                                                                                                        attr_type)
                    '''
                    self._edge_types[(e_type["FromVertexTypeName"],
                                 e_type["Name"], 
                                 e_type["ToVertexTypeName"])]._add_attr(attr["AttributeName"], str)
            for e in list(self._edge_types.keys()):
                src_vt = e[0]
                dest_vt = e[2]
                self._vertex_types[src_vt].outgoing_edge_types.append(self._edge_types[e])
                self._vertex_types[dest_vt].incoming_edge_types.append(self._edge_types[e])


    @property
    def vertex_types(self):
        return self._vertex_types

    @property
    def edge_types(self):
        return self._edge_types