from pyTigerGraph.pyTigerGraphException import TigerGraphException

BASE_TYPES  = ["string", "int", "uint", "float", "double", "bool", "datetime"]
PRIMARY_ID_TYPES = ["string", "int", "uint", "datetime"]
COLLECTION_TYPES = ["list", "set", "map"]
COLLECTION_VALUE_TYPES = ["int", "double", "float", "string", "datetime", "udt"]
MAP_KEY_TYPES = ["int", "string", "datetime"]

class Attribute():
    def __init__(self, attribute_name, attribute_type, attribute_value_type = None, attribute_key_type = None):
        self.attribute_name = attribute_name
        # check if valid type
        if attribute_type.lower() in BASE_TYPES:
            self.attribute_type = attribute_type
        elif attribute_type.lower() in COLLECTION_TYPES:
            ct = attribute_type
            if ct.lower() == "list" or ct.lower() == "set":
                cvt = attribute_value_type
                if cvt.lower() in COLLECTION_VALUE_TYPES:
                    self.attribute_type = attribute_type+"<"+attribute_value_type+">"
                else:
                    TigerGraphException(cvt + " is not a supported datatype to create attribute of type " + ct)
            elif ct == "map":
                keyt = attribute_key_type
                valt = attribute_value_type
                if not(valt.lower() in COLLECTION_VALUE_TYPES):
                    TigerGraphException(valt + " is not a supported datatype to create value attribute of type " + ct)
                if not(keyt.lower() in COLLECTION_VALUE_TYPES):
                    TigerGraphException(keyt + " is not a supported datatype to create value attribute of type " + ct)
                self.attribute_type = attribute_type+"<"+attribute_key_type+","+attribute_value_type+">"
            else:
                TigerGraphException(ct + " is not a valid container datatype")
        else:
            TigerGraphException(attribute_type + " is not a recognized datatype for TigerGraph databases")
    
    def __repr__(self):
        return self.attribute_name+" of type "+self.attribute_type

class Vertex():
    def __init__(self, vertex_type, primary_id_name, primary_id_as_attribute, primary_id_type):
        self.vertex_type = vertex_type
        self.attributes = {}
        self.attribute_edits = {"ADD": [], "DELETE": []}
        self.incoming_edge_types = []
        self.outgoing_edge_types = []
        self.primary_id = primary_id_name
        self.primary_id_as_attribute = primary_id_as_attribute
        self.primary_id_type = primary_id_type
        if not(primary_id_type.lower() in PRIMARY_ID_TYPES):
            raise TigerGraphException(primary_id_type, "is not a supported type for primary IDs.")
        if self.primary_id_as_attribute:
            self.attributes[primary_id_name] = Attribute(primary_id_name, primary_id_type)

    def add_attribute(self, attribute):
        #verify valid attribute here
        self.attribute_edits["ADD"].append(attribute)

    def remove_attribute(self, attribute):
        if self.primary_id_as_attribute:
            if attribute.attribute_name == self.primary_id:
                raise TigerGraphException("Cannot remove primary ID attribute: "+self.primary_id+".")
        self.attribute_edits["DELETE"].append(attribute)

    def __repr__(self):
        return self.vertex_type


class Edge():
    def __init__(self, edge_type, from_vertex_type, to_vertex_type, is_directed, reverse_edge):
        self.edge_type = edge_type
        self.attribute_edits = {"ADD": [], "DELETE": []}
        self.attributes = {}
        self.is_directed = is_directed
        self.reverse_edge = reverse_edge
        self.from_vertex_type = from_vertex_type
        self.to_vertex_type = to_vertex_type

    def add_attribute(self, attribute):
        # Verify valid attribute here
        self.attribute_edits["ADD"].append(attribute)

    def remove_attribute(self, attribute_name):
        self.attribute_edits["DELETE"].append(attribute_name)

    def update_attributes(self, attributes):
        self.attributes = []
        for attr in attributes:
            self.attributes.append(Attribute(attr["AttributeName"], attr["AttributeType"]))

    def __repr__(self):
        return self.edge_type


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
                    self._vertex_types[v_type["Name"]].attributes[attr["AttributeName"]] = Attribute(attr["AttributeName"],
                                                                                                    attr["AttributeType"]["Name"],
                                                                                                    attr["AttributeType"].get("ValueTypeName"),
                                                                                                    attr["AttributeType"].get("KeyTypeName"))
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
                    self._edge_types[(e_type["FromVertexTypeName"],
                                     e_type["Name"], 
                                     e_type["ToVertexTypeName"])].attributes[attr["AttributeName"]] = Attribute(attr["AttributeName"],
                                                                                                                attr["AttributeType"]["Name"],
                                                                                                                attr["AttributeType"].get("ValueTypeName"),
                                                                                                                attr["AttributeType"].get("KeyTypeName"))

            for e in list(self._edge_types.keys()):
                src_vt = e[0]
                dest_vt = e[2]
                self._vertex_types[src_vt].outgoing_edge_types.append(self._edge_types[e])
                self._vertex_types[dest_vt].incoming_edge_types.append(self._edge_types[e])


    @property
    def vertex_types(self):
        return list(self._vertex_types.values())

    @property
    def edge_types(self):
        return list(self._edge_types.values())