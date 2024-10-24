"""Visualization Functions

Contains functions to visualize graphs.
"""
try:
    import ipycytoscape
except:
    raise Exception(
        "Please install ipycytoscape to use visualization functions")


def drawSchema(schema: dict, style: list = []):
    """Visualize a graph schema.

    Args:
        schema (dict):
            A dictionary that describes a graph schema. This can be obtained by running `conn.getSchema()`
        style (list, optional):
            A list of styles that complies with ipycytoscape standards.
    """
    cytoscape_style = [
        {
            "selector": "node",
            "css": {
                "font-size": "10px",
                "content": "data(id)",
                "text-valign": "center",
                "color": "white",
                "text-outline-width": 2,
                "text-outline-color": "green",
                "background-color": "green",
            },
        },
        {
            "selector": ":selected",
            "css": {
                "background-color": "black",
                "line-color": "black",
                "target-arrow-color": "black",
                "source-arrow-color": "black",
                "text-outline-color": "black",
            },
        },
        {
            "selector": "edge",
            "style": {
                "font-family": "arial",
                "font-size": "7px",
                "font-color": "blue",
                "width": 1,
                "label": "data(label)",
            },
        },
        {
            "selector": "edge.directed",
            "style": {
                "curve-style": "bezier",
                "target-arrow-shape": "triangle",
            },
        },
        {
            "selector": "edge[label]",
            "css": {
                "label": "data(label)",
                "text-rotation": "autorotate",
            },
        },
        # {'width': '100%', 'height': '400px',}
    ]
    if style:
        cytoscape_style = style

    # build json
    tg_graph_json = _convert_schema_for_ipycytoscape(schema)

    # plot
    ipycytoscape_obj = ipycytoscape.CytoscapeWidget()
    ipycytoscape_obj.graph.add_graph_from_json(tg_graph_json, directed=True)
    ipycytoscape_obj.set_style(cytoscape_style)
    ipycytoscape_obj.set_layout(animate=True, name="circle", padding=1)

    return ipycytoscape_obj


def _convert_schema_for_ipycytoscape(schema: dict):
    nodes = schema["VertexTypes"]
    edges = schema["EdgeTypes"]
    tg_graph_json = {"nodes": [], "edges": []}

    for node in nodes:
        cytoscape_node = dict()
        cytoscape_node["data"] = node
        cytoscape_node["data"]["id"] = node["Name"]
        tg_graph_json["nodes"].append(cytoscape_node)

    for edge in edges:
        if "EdgePairs" in edge:
            for edgePair in edge["EdgePairs"]:
                cytoscape_edge = dict()
                cytoscape_edge["data"] = dict()
                cytoscape_edge["data"]["id"] = (
                    edge["Name"] + ":" + edgePair["From"] +
                    ":" + edgePair["To"]
                )
                cytoscape_edge["data"]["source"] = edgePair["From"]
                cytoscape_edge["data"]["target"] = edgePair["To"]
                cytoscape_edge["data"]["label"] = edge["Name"]
                tg_graph_json["edges"].append(cytoscape_edge)
        else:
            cytoscape_edge = dict()
            cytoscape_edge["data"] = dict()
            cytoscape_edge["data"]["id"] = (
                edge["Name"]
                + ":"
                + edge["FromVertexTypeName"]
                + ":"
                + edge["ToVertexTypeName"]
            )
            cytoscape_edge["data"]["source"] = edge["FromVertexTypeName"]
            cytoscape_edge["data"]["target"] = edge["ToVertexTypeName"]
            cytoscape_edge["data"]["label"] = edge["Name"]
            tg_graph_json["edges"].append(cytoscape_edge)

    return tg_graph_json
