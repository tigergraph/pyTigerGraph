import torch
from torch_geometric.nn import MessagePassing
from torch_geometric_temporal.nn.recurrent import EvolveGCNH
from icecream import ic as iprint

from recsys.utils.metrics import recall_at_k


class EGCN(torch.nn.Module):
    def __init__(self, embedding_dim, num_nodes, num_users, num_layers):
        super().__init__()

        self.embedding_dim = embedding_dim  # Number of input features
        self.num_nodes = num_nodes
        self.num_users = num_users
        self.num_layers = 1

        self.embeddings = torch.nn.Embedding(
            num_embeddings=self.num_nodes, embedding_dim=self.embedding_dim
        )
        torch.nn.init.normal_(self.embeddings.weight, std=0.1)

        self.layers = torch.nn.ModuleList()  # EvolveGCN Layers
        for _ in range(self.num_layers):
            self.layers.append(EvolveGCNH(num_nodes, embedding_dim))
        self.sigmoid = torch.sigmoid
        # iprint(self.embeddings.weight.shape)
        # iprint(num_nodes, embedding_dim)

    def forward(self):
        raise NotImplementedError("forward() has not been implemented for the GNN class. Do not use")

    def _propagation_node_emb(self, edge_index_mp: torch.Tensor) -> torch.Tensor:
        h = self.embeddings.weight  # layer-0 embeddings
        for i in range(self.num_layers):  # now performing the GNN propagation
            h = self.layers[i](h, edge_index_mp)
        # iprint(h)
        return h
    
    def predict_scores(self, edge_index, embs):
        scores = embs[edge_index[0, :], :] * embs[edge_index[1, :], :]  # dot product
        scores = scores.sum(dim=1)
        scores = self.sigmoid(scores)  # edge-based scores
        return scores

    def calc_loss(self, data_mp, data_pos, data_neg):
        """
        The main training step.
        """
        # print(data_mp.edge_index)
        # Perform GNN propagation on message passing edges to get final embeddings
        final_node_embs = self._propagation_node_emb(data_mp.edge_index)
        # iprint(final_node_embs)
        # Get edge prediction scores for all positive and negative evaluation edges
        pos_scores = self.predict_scores(data_pos.edge_index, final_node_embs)
        neg_scores = self.predict_scores(data_neg.edge_index, final_node_embs)
        # iprint(pos_scores)
        loss = -torch.log(self.sigmoid(pos_scores - neg_scores)).mean()
        return loss

    def evaluation(self, data_mp, data_pos, k):
        """
        Performs evaluation on validation or test set. Calculates recall@k.
        """
        final_embs = self._propagation_node_emb(data_mp.edge_index)
        unique_users = torch.unique_consecutive(data_pos.edge_index[0, :])
        user_emb = final_embs[unique_users, :]  # User ID, embeddings
        item_emb = final_embs[self.num_users:, :]  # Item ID, embeddings
        # iprint(user_emb.shape, item_emb.shape)
        ratings = self.sigmoid(torch.matmul(user_emb, item_emb.t())).cpu()
        # iprint(ratings)
        result = recall_at_k(
            all_ratings=ratings,
            k=k,
            num_users=self.num_users,
            ground_truth=data_pos.edge_index.cpu(),
            unique_users=unique_users.cpu(),
        )  # Calculate recall@k
        return result


class EGCNLayer(MessagePassing):

    def __init__(self):
        super(EGCNLayer, self).__init__(aggr="add")
    
    def message(self, x_j, norm):
        return norm.view(-1, 1) * x_j
    
    def forward(self, x, edge_index):
        return self.propagate(edge_index, x)
