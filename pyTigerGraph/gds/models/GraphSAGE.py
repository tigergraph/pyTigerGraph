"""GraphSAGE Models"""

from . import base_model as bm
from ..metrics import ClassificationMetrics, RegressionMetrics, LinkPredictionMetrics
try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch_geometric.nn import to_hetero
    import torch_geometric.nn as gnn
except:
    raise Exception(
        "PyTorch Geometric required to use GraphSAGE. Please install PyTorch Geometric")


class BaseGraphSAGEModel(bm.BaseModel):
    """NO DOC."""

    def __init__(self, num_layers, out_dim, hidden_dim, dropout=0.0, heterogeneous=None):
        super().__init__()
        self.dropout = dropout
        self.heterogeneous = heterogeneous
        tmp_model = gnn.GraphSAGE(-1, hidden_dim, num_layers, out_dim, dropout)
        if self.heterogeneous:
            self.model = to_hetero(tmp_model, heterogeneous)
        else:
            self.model = tmp_model

    def forward(self, batch, target_type=None):
        if self.heterogeneous:
            x = batch.x_dict
            for k in x.keys():
                x[k] = x[k].float()
            edge_index = batch.edge_index_dict
        else:
            x = batch.x.float()
            edge_index = batch.edge_index
        return self.model(x, edge_index)

    def compute_loss(self, loss_fn=None):
        raise NotImplementedError(
            "Loss computation not implemented for BaseGraphSAGEModel")


class GraphSAGEForVertexClassification(BaseGraphSAGEModel):
    """GraphSAGEForVertexClassification
    Use a GraphSAGE model to classify vertices. By default, this model collects `ClassficiationMetrics`, and uses cross entropy as its loss function.
    """

    def __init__(self, num_layers: int, out_dim: int, hidden_dim: int, dropout=0.0, heterogeneous=None, class_weights=None):
        """Initialize the GraphSAGE Vertex Classification Model.
        Args:
            num_layers (int):
                The number of layers in the model. Typically corresponds to `num_hops` in the dataloader.
            out_dim (int):
                The number of output dimensions. Corresponds to the number of classes in the classification task.
            hidden_dim (int):
                The hidden dimension to use.
            dropout (float, optional):
                The amount of dropout to apply between the layers. Defaults to 0.
            heterogeneous (tuple, optional):
                If set, use the graph metadata in the PyG heterogeneous metadata format.
                Can also retrieve this from the dataloader by calling `loader.metadata()`. Defaults to None.
            class_weights (torch.Tensor, optional):
                If set, weight the different classes in the loss function. Used in imbalanced classification tasks.
        """
        super().__init__(num_layers, out_dim, hidden_dim, dropout, heterogeneous)
        self.class_weight = class_weights
        self.metrics = ClassificationMetrics(num_classes=out_dim)

    def forward(self, batch, get_probs=False, target_type=None):
        """Make a forward pass.
        Args:
            batch (torch_geometric.Data or torch_geometric.HeteroData):
                The PyTorch Geometric data object to classify.
            get_probs (bool, optional):
                Return the softmax scores of the raw logits, which can be interpreted as probabilities. Defaults to false.
            target_type (str, optional):
                Name of the vertex type to get the logits of. Defaults to None, and will return logits for all vertex types.
        """
        logits = super().forward(batch)
        if self.heterogeneous:
            if get_probs:
                for k in logits.keys():
                    logits[k] = F.softmax(logits[k], dim=-1)
            if target_type:
                return logits[target_type]
            else:
                return logits
        else:
            if get_probs:
                return F.softmax(logits, dim=-1)
            else:
                return logits

    def compute_loss(self, logits, batch, target_type=None, loss_fn=None):
        """Compute loss.
        Args:
            logits (torch.Tensor or dict of torch.Tensor):
                The output of the forward pass.
            batch (torch_geometric.Data or torch_geometric.HeteroData):
                The PyTorch Geometric data object to classify. Assumes the target is represented in the `"y"` data object.
            target_type (str, optional):
                The string of the vertex type to compute the loss on.
            loss_fn (callable, optional):
                The function to compute the loss with. Uses cross entropy loss if not defined.
        """
        if not (loss_fn):
            loss_fn = F.cross_entropy
            if self.heterogeneous:
                loss = loss_fn(logits[batch[target_type].is_seed],
                               batch[target_type].y[batch[target_type].is_seed].long(),
                               self.class_weight)
            else:
                loss = loss_fn(
                    logits[batch.is_seed], batch.y[batch.is_seed].long(), self.class_weight)
        else:  # can't assume custom loss supports class weights
            if self.heterogeneous:
                loss = loss_fn(logits[batch[target_type].is_seed],
                               batch[target_type].y[batch[target_type].is_seed].long())
            else:
                loss = loss_fn(logits[batch.is_seed],
                               batch.y[batch.is_seed].long())
        return loss


class GraphSAGEForVertexRegression(BaseGraphSAGEModel):
    """GraphSAGEForVertexRegression
    Use GraphSAGE for vertex regression tasks. By default, this model collects `RegressionMetrics`, and uses MSE as its loss function.
    """

    def __init__(self, num_layers: int, out_dim: int, hidden_dim: int, dropout=0.0, heterogeneous=None):
        """Initialize the GraphSAGE Vertex Regression Model.
        Args:
            num_layers (int):
                The number of layers in the model. Typically corresponds to `num_hops` in the dataloader.
            out_dim (int):
                The dimension of the output. Corresponds to the size of vector to perform the regression of.
            hidden_dim (int):
                The hidden dimension to use.
            dropout (float, optional):
                The amount of dropout to apply between layers. Defaults to 0.0.
            heterogeneous (tuple, optional):
                If set, use the graph metadata in the PyG heterogeneous metadata format.
                Can also retrieve this from the dataloader by calling `loader.metadata()`. Defaults to None.
        """
        super().__init__(num_layers, out_dim, hidden_dim, dropout, heterogeneous)
        self.metrics = RegressionMetrics()

    def forward(self, batch, target_type=None):
        """Make a forward pass.
        Args:
            batch (torch_geometric.Data or torch_geometric.HeteroData):
                The PyTorch Geometric data object to classify.
            target_type (str, optional):
                Name of the vertex type to get the logits of. Defaults to None, and will return logits for all vertex types.
        """
        logits = super().forward(batch)
        if self.heterogeneous:
            if target_type:
                return logits[target_type]
        return logits

    def compute_loss(self, logits, batch, target_type=None, loss_fn=None):
        """Compute loss.
        Args:
            logits (torch.Tensor or dict of torch.Tensor):
                The output of the forward pass.
            batch (torch_geometric.Data or torch_geometric.HeteroData):
                The PyTorch Geometric data object to classify. Assumes the target is represented in the `"y"` data object.
            target_type (str, optional):
                The string of the vertex type to compute the loss on.
            loss_fn (callable, optional):
                The function to compute the loss with. Uses MSE loss if not defined.
        """
        if not (loss_fn):
            loss_fn = F.mse_loss
        if self.heterogeneous:
            loss = loss_fn(logits[target_type][batch[target_type].is_seed],
                           batch[target_type].y[batch[target_type].is_seed])
        else:
            loss = loss_fn(logits[batch.is_seed], batch.y[batch.is_seed])
        return loss


class GraphSAGEForLinkPrediction(BaseGraphSAGEModel):
    """GraphSAGEForLinkPrediction
    By default, this model collects `LinkPredictionMetrics` with k = 10, and uses binary cross entropy as its loss function.
    """

    def __init__(self, num_layers, embedding_dim, hidden_dim, dropout=0.0, heterogeneous=None):
        """Initialize the GraphSAGE Link Prediction Model.
        Args:
            num_layers (int):
                The number of layers in the model. Typically corresponds to `num_hops` in the dataloader.
            embedding_dim (int):
                The dimension of the embedding generated.
                This embedding is then used for cosine similarity between a pair of vertices to generate the prediction for the edge.
            hidden_dim (int):
                The hidden dimension to use.
            dropout (float, optional):
                The amount of dropout to apply between layers. Defaults to 0.0.
            heterogeneous (tuple, optional):
                If set, use the graph metadata in the PyG heterogeneous metadata format.
                Can also retrieve this from the dataloader by calling `loader.metadata()`. Defaults to None.
        """
        super().__init__(num_layers, embedding_dim, hidden_dim, dropout, heterogeneous)
        self.metrics = LinkPredictionMetrics(k=10)

    def forward(self, batch, target_type=None):
        """Make a forward pass.
        Args:
            batch (torch_geometric.Data or torch_geometric.HeteroData):
                The PyTorch Geometric data object to classify.
            target_type (str, optional):
                Name of the vertex type to get the logits of. Defaults to None, and will return logits for all vertex types.
        """
        logits = super().forward(batch, target_type=target_type)
        if self.heterogeneous:
            if target_type:
                pos_edges, neg_edges = self.generate_edges(batch, target_type)
                src_h = logits[target_type[0]]
                dest_h = logits[target_type[-1]]
                h = self.decode(src_h, dest_h, pos_edges, neg_edges)
        else:
            pos_edges, neg_edges = self.generate_edges(batch)
            h = self.decode(logits, logits, pos_edges, neg_edges)
        batch.y = self.get_link_labels(pos_edges, neg_edges)
        return h

    def decode(self, src_z, dest_z, pos_edge_index, neg_edge_index):
        """NO DOC."""
        edge_index = torch.cat(
            [pos_edge_index, neg_edge_index], dim=-1)  # concatenate pos and neg edges
        logits = (src_z[edge_index[0]] * dest_z[edge_index[1]]
                  ).sum(dim=-1)  # dot product
        return logits

    def get_link_labels(self, pos_edge_index, neg_edge_index):
        """NO DOC."""
        E = pos_edge_index.size(1) + neg_edge_index.size(1)
        link_labels = torch.zeros(E, dtype=torch.float)
        link_labels[:pos_edge_index.size(1)] = 1.
        return link_labels

    def generate_edges(self, batch, target_edge_type=None):
        """NO DOC."""
        if self.heterogeneous:
            pos_edges = batch[target_edge_type].edge_index[:,
                                                           batch[target_edge_type].is_seed]
            src_neg_edges = torch.randint(
                0, batch[target_edge_type[0]].x.shape[0], (pos_edges.shape[1],), dtype=torch.long)
            dest_neg_edges = torch.randint(
                0, batch[target_edge_type[-1]].x.shape[0], (pos_edges.shape[1],), dtype=torch.long)
            neg_edges = torch.stack((src_neg_edges, dest_neg_edges))
        else:
            pos_edges = batch.edge_index[:, batch.is_seed]
            neg_edges = torch.randint(
                0, batch.x.shape[0], pos_edges.size(), dtype=torch.long)
        return pos_edges, neg_edges

    def compute_loss(self, logits, batch, target_type=None, loss_fn=None):
        """Compute loss.
        Args:
            logits (torch.Tensor or dict of torch.Tensor):
                The output of the forward pass.
            batch (torch_geometric.Data or torch_geometric.HeteroData):
                The PyTorch Geometric data object to classify. Assumes the target is represented in the `"y"` data object.
            target_type (str, optional):
                The string of the edge type to compute the loss on.
            loss_fn (callable, optional):
                The function to compute the loss with. Uses binary cross entropy loss if not defined.
        """
        if not (loss_fn):
            loss_fn = F.binary_cross_entropy_with_logits
        loss = loss_fn(logits, batch.y)
        return loss

    def get_embeddings(self, batch):
        """Get embeddings.
        Args:
            batch (torch_geometric.Data or torch_geometric.HeteroData):
                Get the embeddings for all vertices in a batch.
        """
        return super().forward(batch)
