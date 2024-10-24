"""PyTorch Geometric Transforms"""


class BasePyGTransform():
    """NO DOC"""

    def __call__(self, data):
        return data

    def __repr__(self):
        return f'{self.__class__.__name__}()'


class TemporalPyGTransform(BasePyGTransform):
    """TemporalPyGTransform.
    The TemporalPyGTransform creates a sequence of subgraph batches out of a single batch of data produced by a NeighborLoader or HGTLoader. It assumes that there are datetime attributes on vertices and edges. If vertex attributes change over time, children vertex attributes are moved to the appropriate parent, and then the children are removed from the graph.
    """

    def __init__(self,
                 vertex_start_attrs: dict,
                 vertex_end_attrs: dict,
                 edge_start_attrs: dict,
                 edge_end_attrs: dict,
                 start_dt: int,
                 end_dt: int,
                 feature_transforms: dict = {},
                 timestep: int = 86400):
        """Instantiate a TemporalPyGTransform.
        Args:
            vertex_start_attrs (str, dict):
                If using on a homogeneous graph, string of the attribute storing the timestamp of when a vertex becomes valid to include.
                If using on a heterogenous graph, dictionary that describes the attribute storing the timestamp of when a vertex becomes a valid vertex to include in the graph.
                In the format of {"VERTEX_TYPE": "attribute_name"}.
            vertex_end_attrs (str, dict):
                If using on a homogeneous graph, string of the attribute storing the timestamp of when a vertex stops being valid to include.
                If using on a heterogenous graph, dictionary that describes the attribute storing the timestamp of when a vertex stops being a valid vertex to include in the graph.
                In the format of {"VERTEX_TYPE": "attribute_name"}
            edge_start_attrs (str, dict):
                If using on a homogeneous graph, string of the attribute storing the timestamp of when an edge becomes valid to include.
                If using on a heterogenous graph, dictionary that describes the attribute storing the timestamp of when an edge becomes a valid edge to include in the graph.
                Uses the PyG edge format of ("SourceVertexType", "EdgeName", "DestinationVertexType").
                In the format of {("SourceVertexType", "EdgeName", "DestinationVertexType"): "attribute_name"}.
            edge_end_attrs (str, dict):
                If using on a homogeneous graph, string of the attribute storing the timestamp of when an edge stops being valid to include.
                If using on a heterogenous graph, dictionary that describes the attribute storing the timestamp of when an edge stops being a valid edge to include in the graph.
                Uses the PyG edge format of ("SourceVertexType", "EdgeName", "DestinationVertexType").
                In the format of {("SourceVertexType", "EdgeName", "DestinationVertexType"): "attribute_name"}
            start_dt (int):
                The UNIX epoch time to start generating the sequence of subgraphs.
            end_dt (int):
                The UNIX epoch time to stop generating the sequence of subgraphs.
            feature_transforms (dict, optional):
                Only available on heterogeneous graphs. Moves temporally dynamic features from "children" vertices to "parent" vertices when
                modelling temporal attributes in TigerGraph. 
                The key of the dictionary is the edge to move the attributes from the child type to the parent type, and the value is a list of attributes to move.
                In the fromat of {("ItemInstance", "reverse_DESCRIBED_BY", "Item"): ["x"]}
            timestep (int, optional):
                The number of seconds to use in between timesteps. Defaults to 86400 seconds (1 day).
        """
        self.vertex_start = vertex_start_attrs
        self.vertex_end = vertex_end_attrs
        self.edge_start = edge_start_attrs
        self.edge_end = edge_end_attrs
        self.feat_tr = feature_transforms
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.timestep = timestep
        try:
            import torch_geometric as pyg
            import torch
            if (int(pyg.__version__.split(".")[1]) < 3 and int(pyg.__version__.split(".")[0]) == 2) or int(pyg.__version__.split(".")[0]) < 2:
                raise Exception(
                    "PyTorch Geometric version must be 2.3.0 or greater")
        except:
            raise Exception(
                "PyTorch Geometric required to use PyG models. Please install PyTorch Geometric")

    def __call__(self, data) -> list:
        """Perform the transform. Returns a list of PyTorch Geometric data objects, a sequence of snapshots in time of the graph.
            Edges are removed between vertices that do not have connections at the given time. All vertices are in each snapshot, but are marked
            as present with the "vertex_present" attribute in the produced data objects.
            Args:
                data (pyg.data.HeteroData or pyg.data.Data):
                    Takes in a PyTorch Geometric data object, such as ones produced by the dataloaders.   
        """
        import torch_geometric as pyg
        import torch
        if isinstance(data, pyg.data.HeteroData):
            sequence = []
            for i in range(self.start_dt, self.end_dt, self.timestep):
                v_to_keep = {}
                for v_type in data.node_types:
                    if v_type in self.vertex_start.keys():
                        v_start_attr = self.vertex_start[v_type]
                        if v_type in self.vertex_end.keys():
                            v_end_attr = self.vertex_end[v_type]
                            v_to_keep[v_type] = torch.logical_and(data[v_type][v_start_attr] <= i, torch.logical_or(
                                data[v_type][v_end_attr] > i, data[v_type][v_end_attr] == -1))
                        else:
                            v_to_keep[v_type] = data[v_type][v_start_attr] <= i
                    elif v_type in self.vertex_end.keys():
                        v_end_attr = self.vertex_end[v_type]
                        if v_type not in self.vertex_start[v_type]:
                            v_to_keep[v_type] = torch.logical_or(
                                data[v_type][v_end_attr] >= i, data[v_type][v_end_attr] == -1)
                    else:
                        v_to_keep[v_type] = torch.tensor(
                            [i for i in data[v_type].is_seed])
                    data[v_type]["vertex_present"] = v_to_keep[v_type]

                e_to_keep = {}
                for e_type in data.edge_types:
                    v_src_type = e_type[0]
                    v_dest_type = e_type[-1]
                    src_idx_to_keep = torch.argwhere(
                        v_to_keep[v_src_type]).flatten()
                    dest_idx_to_keep = torch.argwhere(
                        v_to_keep[v_dest_type]).flatten()
                    edges = data.edge_index_dict[e_type]
                    filtered_edges = torch.logical_and(torch.tensor([True if i in src_idx_to_keep else False for i in edges[0]]), torch.tensor([
                                                       True if i in dest_idx_to_keep else False for i in edges[1]]))
                    if e_type in self.edge_start.keys():
                        filtered_edges = torch.logical_and(
                            filtered_edges, data[e_type][self.edge_start[e_type]] <= i)
                    if e_type in self.edge_end.keys():
                        filtered_edges = torch.logical_and(
                            filtered_edges, data[e_type][self.edge_end[e_type]] >= i)
                    e_to_keep[e_type] = filtered_edges

                subgraph = data.edge_subgraph(e_to_keep)

                for triple in self.feat_tr.keys():
                    for feat in self.feat_tr[triple]:
                        subgraph[triple[-1]][str(triple[0])+"_"+str(feat)] = torch.zeros(
                            subgraph[triple[-1]]["vertex_present"].size(), dtype=subgraph[triple[0]][feat].dtype)
                        subgraph[triple[-1]][str(triple[0])+"_"+str(feat)][subgraph[triple].edge_index[1]
                                                                           ] = subgraph[triple[0]][feat][subgraph[triple].edge_index[0]]

                for triple in self.feat_tr.keys():
                    del subgraph[triple[0]]
                    for es in subgraph.edge_types:
                        if es[0] == triple[0] or es[-1] == triple[0]:
                            del subgraph[es]

                sequence.append(subgraph)
            return sequence
        elif isinstance(data, pyg.data.Data):
            if self.feat_tr:
                raise Exception(
                    "No feature transformations are supported on homogeneous data")
            sequence = []
            for i in range(self.start_dt, self.end_dt, self.timestep):
                v_to_keep = torch.logical_and(data[self.vertex_start] <= i, torch.logical_or(
                    data[self.vertex_end] > i, data[self.vertex_end] == -1))
                src_idx_to_keep = torch.argwhere(v_to_keep).flatten()
                dest_idx_to_keep = torch.argwhere(v_to_keep).flatten()
                edges = data.edge_index
                filtered_edges = torch.logical_and(torch.tensor([True if i in src_idx_to_keep else False for i in edges[0]]), torch.tensor([
                                                   True if i in dest_idx_to_keep else False for i in edges[1]]))
                if self.edge_start:
                    filtered_edges = torch.logical_and(
                        filtered_edges, data[self.edge_start] <= i)
                if self.edge_end:
                    filtered_edges = torch.logical_and(
                        filtered_edges, data[self.edge_end] >= i)
                e_to_keep = filtered_edges
                subgraph = data.edge_subgraph(e_to_keep)
                subgraph.vertex_present = v_to_keep
                sequence.append(subgraph)
            return sequence
        else:
            raise Exception(
                "Passed batch of data must be of type torch_geometric.data.Data or torch_geometric.data.HeteroData")
