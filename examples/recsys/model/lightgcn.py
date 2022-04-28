import torch
from torch_geometric.nn import MessagePassing
from torch_geometric.utils import degree
from torch_geometric.data import Data as PyGData
import inspect
from icecream import ic as iprint

from recsys.utils.metrics import recall_at_k


class LGCN(torch.nn.Module):
    def __init__(self, embedding_dim: int, num_nodes: int, num_users: int, num_layers: int):
        super().__init__()
        
        self.embedding_dim = embedding_dim
        self.num_nodes = num_nodes
        self.num_customers = num_users
        self.num_layers = num_layers

        self.embeddings = torch.nn.Embedding(
            num_embeddings=self.num_nodes, embedding_dim=self.embedding_dim
        )
        torch.nn.init.normal_(self.embeddings.weight, std=0.1)

        self.layers = torch.nn.ModuleList()  # LightGCN layers
        for _ in range(self.num_layers):
            self.layers.append(LightGCN())

        self.sigmoid = torch.sigmoid

    def forward(self):
        raise NotImplementedError("forward() has not been implemented for the GNN class. Do not use")

    def gnn_propagation(self, edge_index_mp: torch.Tensor) -> torch.Tensor:
        """Embedding propagation instead of `forward` function
        :param edge_index_mp: Edgelist for message passing
        :return: Node embeddings as the output of this GNN model
        """
        # print(inspect.currentframe())
        x = self.embeddings.weight  # layer-0 embeddings
        # print("edge_index_mp:", edge_index_mp.shape, edge_index_mp)
        x_at_each_layer = [x]  # stores embeddings from each layer
        # iprint("x0:", x)
        for i in range(self.num_layers):  # now performing the GNN propagation
            x = self.layers[i](x, edge_index_mp)
            # iprint("x{}:".format(i+1), x)
            x_at_each_layer.append(x)
        # iprint("output:", x)
        final_embs = torch.stack(x_at_each_layer, dim=0).mean(dim=0)  # take average
        return final_embs

    def predict_scores(self, edge_index: torch.Tensor, embs: torch.Tensor):
        # src_nodes = edge_index[0, :]
        # dst_nodes = edge_index[1, :]
        # print("src_nodes:", src_nodes)
        # print("dst_nodes:", dst_nodes)
        # first_embs = embs[:, 0]  # First element of embeddings for each node
        # print("first_embs:", first_embs)
        # print("NaN count:", sum(torch.isnan(first_embs)))
        # src_embs = embs[edge_index[0, :], :]
        # dst_embs = embs[edge_index[1, :], :]
        # print("src_embs:", src_embs)
        # print("dst_embs:", dst_embs)
        scores = embs[edge_index[0, :], :] * embs[edge_index[1, :], :]  # dot product
        # print("edge_scores:", scores)
        scores = scores.sum(dim=1)
        # print("sum_scores:", scores)
        scores = self.sigmoid(scores)
        # print("sigmoid_scores:", scores)
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
        node_embs = self.gnn_propagation(data_mp.edge_index)
        # iprint(node_embs)
        # Get edge prediction scores for all positive and negative evaluation edges
        pos_scores = self.predict_scores(data_pos.edge_index, node_embs)
        neg_scores = self.predict_scores(data_neg.edge_index, node_embs)
        # iprint(pos_scores, neg_scores)
        
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
        final_embs = self.gnn_propagation(data_mp.edge_index)
        
        # Get embeddings of all unique customers in the batch of evaluation edges
        unique_users = torch.unique_consecutive(data_pos.edge_index[0, :])
        user_emb = final_embs[unique_users, :]  # has shape [number of customers in batch, 64]
        item_emb = final_embs[self.num_customers :, :]  # has shape [total number of articles in dataset, 64]
        ratings = self.sigmoid(torch.matmul(user_emb, item_emb.t())).cpu()
        # iprint(ratings)
        result = recall_at_k(
            ratings,
            k,
            self.num_customers,
            data_pos.edge_index.cpu(),
            unique_users.cpu(),
        )  # Calculate recall@k
        return result


class LightGCN(MessagePassing):
    """
    A single LightGCN layer. Extends the MessagePassing class from PyTorch Geometric
    """

    def __init__(self):
        super(LightGCN, self).__init__(aggr="add")  # aggregation function is 'add'

    def message(self, x_j: torch.Tensor, norm: torch.Tensor) -> torch.Tensor:
        """
        Specifies how to perform message passing during GNN propagation.
        For LightGCN, we simply pass along each source node's embedding to the target node,
        normalized by the normalization term for that node.
        """
        return norm.view(-1, 1) * x_j

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        """
        LightGCN message passing/aggregation/update to get updated node embeddings
        """
        row, col = edge_index
        deg = degree(col)
        deg_inv_sqrt = deg.pow(-0.5)
        norm = deg_inv_sqrt[row] * deg_inv_sqrt[col]
        return self.propagate(edge_index, x=x, norm=norm)
