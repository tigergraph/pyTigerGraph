"""Schema Functions.

The functions in this page retrieve information about the graph schema.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging

from typing import Union


logger = logging.getLogger(__name__)

def _get_attr_type(attrType: dict) -> str:
    """Returns attribute data type in simple format.

    Args:
        attribute:
            The details of the attribute's data type.

    Returns:
        Either "(scalar_type)" or "(complex_type, scalar_type)" string.
    """
    ret = attrType["Name"]
    if "KeyTypeName" in attrType:
        ret += "<" + attrType["KeyTypeName"] + \
            "," + attrType["ValueTypeName"] + ">"
    elif "ValueTypeName" in attrType:
        ret += "<" + attrType["ValueTypeName"] + ">"

    return ret

def _upsert_attrs(attributes: dict) -> dict:
    """Transforms attributes (provided as a table) into a hierarchy as expected by the upsert
        functions.

    Args:
        attributes: A dictionary of attribute/value pairs (with an optional operator) in this
            format:
                {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}

    Returns:
        A dictionary in this format:
            {
                <attribute_name>: {"value": <attribute_value>},
                <attribute_name>: {"value": <attribute_value>, "op": <operator>}
            }

    Documentation:
        xref:tigergraph-server:API:built-in-endpoints.adoc#operation-codes[Operation codes]
    """
    logger.debug("entry: _upsertAttrs")
    logger.debug("params: " + str(locals()))

    if not isinstance(attributes, dict):
        return {}
        # TODO Should return something else or raise exception?
    vals = {}
    for attr in attributes:
        val = attributes[attr]
        if isinstance(val, tuple):
            vals[attr] = {"value": val[0], "op": val[1]}
        elif isinstance(val, dict):
            vals[attr] = {"value": {"keylist": list(
                val.keys()), "valuelist": list(val.values())}}
        else:
            vals[attr] = {"value": val}

    if logger.level == logging.DEBUG:
        logger.debug("return: " + str(vals))
    logger.debug("exit: _upsertAttrs")

    return vals

def _prep_upsert_data(data: Union[str, object],
                      atomic: bool = False,
                      ackAll: bool = False,
                      newVertexOnly: bool = False,
                      vertexMustExist: bool = False,
                      updateVertexOnly: bool = False):
    if not isinstance(data, str):
        data = json.dumps(data)
    headers = {}
    if atomic:
        headers["gsql-atomic-level"] = "atomic"
    params = {}
    if ackAll:
        params["ack"] = "all"
    if newVertexOnly:
        params["new_vertex_only"] = True
    if vertexMustExist:
        params["vertex_must_exist"] = True
    if updateVertexOnly:
        params["update_vertex_only"] = True

    return data, headers, params

def _prep_get_endpoints(restppUrl: str,
                        graphname: str,
                        builtin,
                        dynamic,
                        static):
    """Builds url starter and preps parameters of getEndpoints"""
    ret = {}
    if not (builtin or dynamic or static):
        bui = dyn = sta = True
    else:
        bui = builtin
        dyn = dynamic
        sta = static
    url = restppUrl + "/endpoints/" + graphname + "?"
    return bui, dyn, sta, url, ret
