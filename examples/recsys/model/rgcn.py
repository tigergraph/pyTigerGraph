"""
R-GCN model for heterogeneous graphs
"""

import torch
from torch_geometric.nn import RGCNConv
from torch_geometric.data import Data as PyGData

from recsys.utils.metrics import recall_at_k


class RGCN(torch.nn.Module):
    """User-Item link prediction RGCN model
    """
    def __init__(self, embedding_dim: int, num_nodes: int, num_users: int, num_items: int, num_layers: int, num_relations: int):
        super().__init__()
        
        self.embedding_dim = embedding_dim  # Number of input features
        self.num_nodes = num_nodes
        self.num_users = num_users
        self.num_items = num_items
        self.num_layers = num_layers

        self.embeddings = torch.nn.Embedding(
            num_embeddings=self.num_nodes, embedding_dim=self.embedding_dim
        )
        torch.nn.init.normal_(self.embeddings.weight, std=0.1)

        self.layers = torch.nn.ModuleList()  # RGCNConv layers
        for _ in range(self.num_layers):
            self.layers.append(RGCNConv(embedding_dim, embedding_dim, num_relations))

        self.sigmoid = torch.sigmoid

    def forward(self):
        raise NotImplementedError("forward() has not been implemented for the GNN class. Do not use")

    def gnn_propagation(self, edge_index_mp: torch.Tensor, edge_type: torch.Tensor) -> torch.Tensor:
        """Embedding propagation instead of `forward` function
        :param edge_index_mp: Edgelist for message passing
        :param edge_type: Edge type list
        :return: Node embeddings as the output of this GNN model
        """
        x = self.embeddings.weight  # layer-0 embeddings
        x_at_each_layer = [x]  # stores embeddings from each layer
        for i in range(self.num_layers):  # now performing the GNN propagation
            x = self.layers[i](x, edge_index_mp, edge_type)
            x_at_each_layer.append(x)
        final_embs = torch.stack(x_at_each_layer, dim=0).mean(dim=0)  # take average
        return final_embs
    
    def extract_user_item_edges(self, edge_index: torch.Tensor):
        srcs = edge_index[0, :]  # source node IDs
        dsts = edge_index[1, :]  # target node IDs
        
        user_src_mask = srcs < self.num_users  # user node ID: [0, num_users -1]
        item_dst_mask = (self.num_users <= dsts) & (dsts < self.num_users + self.num_items) # item node ID: [num_users, num_users + num_items - 1]
        user_item_mask = user_src_mask & item_dst_mask
        user_item_edge_index = edge_index[:, user_item_mask]
        return user_item_edge_index

    def predict_scores(self, edge_index: torch.Tensor, embs: torch.Tensor):
        user_item_edges = self.extract_user_item_edges(edge_index)
        scores = embs[user_item_edges[0, :], :] * embs[user_item_edges[1, :], :]  # dot product of user-item embeddings
        scores = scores.sum(dim=1)
        scores = self.sigmoid(scores)
        return scores

    def calc_loss(self, data_mp: PyGData, data_pos: PyGData, data_neg: PyGData):
        """
        The main training step
        Perform GNN propagation on message passing edges to get multi-scale embeddings.
        Predict scores for each training example and calculates BPR loss.
        :param data_mp: Message passing edges for multi-scale embedding propagation
        :param data_pos: Positive (existing) edges
        :param data_neg: Negative (non-existing) edges
        :return: Training loss
        """
        # Perform GNN propagation on message passing edges to get final embeddings
        node_embs = self.gnn_propagation(data_mp.edge_index, data_mp.edge_type)
        # Get edge prediction scores for all positive and negative evaluation edges
        pos_scores = self.predict_scores(data_pos.edge_index, node_embs)
        neg_scores = self.predict_scores(data_neg.edge_index, node_embs)
        
        loss = -torch.log(self.sigmoid(pos_scores - neg_scores)).mean()
        return loss

    def evaluation(self, data_mp: PyGData, data_pos: PyGData, k: int):
        """Evaluate model performance (recall@k) for users
        :param data_mp: Message passing edges for multi-scale embedding propagation
        :param data_pos: Current existing edges
        :param k: Top-k
        :return: Recall@k per user
        """
        # Run propagation on the message-passing edges to get multi-scale embeddings
        final_embs = self.gnn_propagation(data_mp.edge_index, data_mp.edge_type)
        
        # Get embeddings of all unique users in the batch of evaluation edges
        user_item_edge_index = self.extract_user_item_edges(data_pos.edge_index)
        unique_users = torch.unique(user_item_edge_index[0, :])
        unique_items = torch.unique(user_item_edge_index[1, :])
        
        user_emb = final_embs[unique_users, :]  # has shape [number of users in batch, embedding size]
        item_emb = final_embs[self.num_users:self.num_users+self.num_items, :]  # has shape [total number of users in dataset, embedding size]
        ratings = self.sigmoid(torch.matmul(user_emb, item_emb.t())).cpu()
        result = recall_at_k(
            ratings,
            k,
            self.num_users,
            user_item_edge_index.cpu(),  # ground-truth user-item edges
            unique_users.cpu(),  # unique user IDs
        )  # Calculate recall@k
        return result
