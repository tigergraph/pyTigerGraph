class BasePyGTransform():
    def __call__(self, data):
        return data

    def __repr__(self):
        return f'{self.__class__.__name__}()'

class TemporalPyGTransform(BasePyGTransform):
    def __init__(self,
                 vertex_start_attrs: dict,
                 vertex_end_attrs: dict,
                 edge_start_attrs: dict,
                 edge_end_attrs: dict,
                 feature_transforms: dict,
                 start_dt: int,
                 end_dt: int,
                 timestep=1):
        self.vertex_start = vertex_start_attrs
        self.vertex_end = vertex_end_attrs
        self.edge_start = edge_start_attrs
        self.edge_end = edge_end_attrs
        self.feat_tr = feature_transforms,
        self.start_dt = start_dt
        self.end_dt = end_dt,
        self.timestep = timestep

    def __call__(self, data):
        try:
            import torch_geometric as pyg
            import torch
        except:
            raise Exception("PyTorch Geometric required to use PyG models. Please install PyTorch Geometric")

        if isinstance(data, pyg.data.HeteroData):
            sequence = []
            for i in range(self.start_dt, self.end_dt, self.timestep):
                v_to_keep = {}
                for v_type in data.node_types:
                    if v_type in self.vertex_start.keys():
                        v_start_attr = self.vertex_start[v_type]
                        if v_type in self.vertex_end.keys():
                            v_end_attr = self.vertex_end[v_type]
                            v_to_keep[v_type] = torch.logical_and(data[v_type][v_start_attr] <= i, torch.logical_or(data[v_type][v_end_attr] > i, data[v_type][v_end_attr] == -1))
                        else:
                            v_to_keep[v_type] = data[v_type][v_start_attr] <= i
                    elif v_type in self.vertex_end.keys():
                        v_end_attr = self.vertex_end[v_type]
                        if v_type not in self.vertex_start[v_type]:
                            v_to_keep[v_type] = torch.logical_or(data[v_type][v_end_attr] >= i, data[v_type][v_end_attr] == -1)
                    else:
                        v_to_keep[v_type] = torch.tensor([i for i in data[v_type].is_seed])
                    data[v_type]["vertex_present"] = v_to_keep[v_type]
                
                
                e_to_keep = {}
                for e_type in data.edge_types:
                    v_src_type = e_type[0]
                    v_dest_type = e_type[-1]
                    src_idx_to_keep = torch.argwhere(v_to_keep[v_src_type]).flatten()
                    dest_idx_to_keep = torch.argwhere(v_to_keep[v_dest_type]).flatten()
                    edges = data.edge_index_dict[e_type]
                    filtered_edges = torch.logical_and(torch.tensor([True if i in src_idx_to_keep else False for i in edges[0]]), torch.tensor([True if i in dest_idx_to_keep else False for i in edges[1]]))
                    if e_type in self.edge_start.keys():
                        filtered_edges = torch.logical_and(filtered_edges, data[e_type][self.edge_start[e_type]] <= i)
                    if e_type in self.edge_end.keys():
                        filtered_edges = torch.logical_and(filtered_edges, data[e_type][self.edge_end[e_type]] >= i)
                    e_to_keep[e_type] = filtered_edges
                
                subgraph = data.edge_subgraph(e_to_keep)
                    
                for triple in self.feat_tr.keys():
                    for feat in self.feat_tr[triple]:
                        subgraph[triple[-1]][str(triple[0])+"_"+str(feat)] = torch.zeros(subgraph[triple[-1]]["vertex_present"].size(), dtype=subgraph[triple[0]][feat].dtype)
                        subgraph[triple[-1]][str(triple[0])+"_"+str(feat)][subgraph[triple].edge_index[1]] = subgraph[triple[0]][feat][subgraph[triple].edge_index[0]]
            
                for triple in self.feat_tr.keys():
                    del subgraph[triple[0]]
                    for es in subgraph.edge_types:
                        if es[0] == triple[0] or es[-1] == triple[0]:
                            del subgraph[es]

                sequence.append(subgraph)
            return sequence
        elif isinstance(data, pyg.data.Data):
            if self.feat_tr:
                raise Exception("No feature transformations are supported on homogeneous data")
            sequence = []
            for i in range(self.start_dt, self.end_dt, self.timestep):
                copy = data.clone()
                copy.edge_index = copy.edge_index.T[torch.logical_and(data[self.vertex_start] >= i, data[self.vertex_end] < i+self.timestep)].T
                if self.edge_start and self.edge_end:
                    copy.edge_index = copy.edge_index.T[torch.logical_and(data[self.edge_start] >= i, data[self.edge_end] < i+self.timestep)].T
                copy.vertex_mask = torch.logical_and(data[self.vertex_start] <= i+self.timestep, data[self.vertex_end] > i)
                sequence.append(copy)
            return sequence
        else:
            raise Exception("Passed batch of data must be of type torch_geometric.data.Data or torch_geometric.data.HeteroData")