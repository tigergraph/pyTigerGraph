import torch
import torch.nn.functional as F
from torch_geometric.nn import MessagePassing
from torch_geometric.nn import GCNConv

from recsys.utils.metrics import recall_at_k


class GCN(torch.nn.Module):
    def __init__(self, embedding_dim, num_nodes, num_users, num_layers, num_feats):
        super().__init__()

        self.embedding_dim = num_feats
        self.num_nodes = num_nodes
        self.num_users = num_users
        self.num_layers = num_layers

        self.embeddings = torch.nn.Embedding(
            num_embeddings=self.num_nodes, embedding_dim=self.embedding_dim
        )
        torch.nn.init.normal_(self.embeddings.weight, std=0.1)

        self.layers = torch.nn.ModuleList()  # EvolveGCN Layers
        for _ in range(self.num_layers):
            self.layers.append(GCNConv(num_nodes, num_feats))

        self.sigmoid = torch.sigmoid

    def forward(self):
        raise NotImplementedError(
            "forward() has not been implemented for the GNN class. Do not use"
        )

    def gnn_propagation(self, edge_index_mp: torch.Tensor) -> torch.Tensor:
        x = self.embeddings.weight  # layer-0 embeddings
        x_at_each_layer = [x]
        for i in range(self.num_layers):  # now performing the GNN propagation
            x = self.layers[i](x, edge_index_mp)
            x_at_each_layer.append(x)
        final_embs = torch.stack(x_at_each_layer, dim=0).mean(
            dim=0
        )  # take average to calculate multi-scale embeddings
        return final_embs

    def predict_scores(self, edge_index, embs):
        scores = embs[edge_index[0, :], :] * embs[edge_index[1, :], :]  # dot product
        scores = scores.sum(dim=1)
        scores = self.sigmoid(scores)
        return scores

    def calc_loss(self, data_mp, data_pos, data_neg):
        """
        The main training step.
        """
        # Perform GNN propagation on message passing edges to get final embeddings
        final_embs = self.gnn_propagation(data_mp.edge_index)
        # Get edge prediction scores for all positive and negative evaluation edges
        pos_scores = self.predict_scores(data_pos.edge_index, final_embs)
        neg_scores = self.predict_scores(data_neg.edge_index, final_embs)
        loss = -torch.log(self.sigmoid(pos_scores - neg_scores)).mean()
        return loss

    def evaluation(self, data_mp, data_pos, k):
        """
        Performs evaluation on validation or test set. Calculates recall@k.
        """
        final_embs = self.gnn_propagation(data_mp.edge_index)

        unique_customers = torch.unique_consecutive(data_pos.edge_index[0, :])
        customer_emb = final_embs[unique_customers, :]
        article_emb = final_embs[self.num_customers :, :]
        ratings = self.sigmoid(torch.matmul(customer_emb, article_emb.t()))
        result = recall_at_k(
            ratings.cpu(),
            k,
            self.num_customers,
            data_pos.edge_index.cpu(),
            unique_customers.cpu(),
        )  # Calculate recall@k
        return result


"""
class RecurrentGCN(torch.nn.Module):
    def __init__(self, node_count, node_features):
        super(RecurrentGCN, self).__init__()
        self.recurrent = EvolveGCNH(node_count, node_features)
        self.linear = torch.nn.Linear(node_features, 1)

    def forward(self, x, edge_index, edge_weight):
        h = self.recurrent(x, edge_index, edge_weight)
        h = F.relu(h)
        h = self.linear(h)
        return h


model = RecurrentGCN(node_features=4, node_count=20)

optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
"""
