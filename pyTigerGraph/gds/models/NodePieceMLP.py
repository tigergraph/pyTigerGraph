"""NodePiece Models"""

from . import base_model as bm
from ..metrics import ClassificationMetrics

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
except:
    raise Exception(
        "PyTorch is required to use NodePiece MLPs. Please install PyTorch")


class BaseNodePieceEmbeddingTable(nn.Module):
    """NO DOC."""

    def __init__(self,
                 vocab_size: int,
                 sequence_length: int,
                 embedding_dim: int = 768):
        super().__init__()
        self.embedding_dim = embedding_dim
        self.seq_len = sequence_length
        self.embedding = nn.Embedding(vocab_size, embedding_dim)
        torch.nn.init.xavier_uniform_(self.embedding.weight)

    def forward(self, x):
        anc_emb = self.embedding(x["anchors"])
        rel_emb = self.embedding(x["relational_context"])
        anc_emb += self.embedding(x["distance"])
        out = torch.concat([anc_emb, rel_emb], dim=1)
        return out


class BaseNodePieceMLPModel(nn.Module):
    """NO DOC."""

    def __init__(self, num_layers, out_dim, hidden_dim, vocab_size, sequence_length, embedding_dim=768, dropout=0.0):
        super().__init__()
        self.embedding_dim = embedding_dim
        self.vocab_size = vocab_size
        self.sequence_length = sequence_length
        self.base_embedding = BaseNodePieceEmbeddingTable(
            vocab_size, sequence_length, embedding_dim)

        self.num_embedding_dim = embedding_dim*sequence_length
        self.in_layer = None
        self.hidden_dim = hidden_dim
        self.dropout = dropout
        self.out_layer = nn.Linear(self.hidden_dim, out_dim)
        self.hidden_layers = nn.ModuleList(
            [nn.Linear(hidden_dim, hidden_dim) for _ in range(num_layers-2)])

    def forward(self, batch):
        if not self.in_layer:
            if "features" in list(batch.keys()):
                self.in_layer = nn.Linear(
                    batch["features"].shape[1] + self.num_embedding_dim, self.hidden_dim)
            else:
                self.in_layer = nn.Linear(
                    self.num_embedding_dim, self.hidden_dim)
        x = self.base_embedding(batch)
        x = torch.flatten(x, start_dim=1)
        if "features" in list(batch.keys()):
            x = torch.cat((x, batch["features"].float()), dim=1)
        x = F.dropout(F.relu(self.in_layer(x)), p=self.dropout)
        for layer in self.hidden_layers:
            x = F.dropout(F.relu(layer(x)), p=self.dropout)
        x = self.out_layer(x)
        return x


class NodePieceMLPForVertexClassification(bm.BaseModel):
    """NodePieceMLPForVertexClassification.
    This model is for training an multi-layer perceptron (MLP) on batches produced by NodePiece dataloaders, and transformed by the `NodePieceMLPTransform`.
    The architecture is for a vertex classification task, and assumes the label of each vertex is in a batch attribute called `"y"`, such as what is produced by the `NodePieceMLPTransform`.
    By default, this model collects `ClassficiationMetrics`, and uses cross entropy as its loss function.
    """

    def __init__(self, num_layers: int, out_dim: int, hidden_dim: int, vocab_size: int, sequence_length: int, embedding_dim=768, dropout=0.0, class_weights=None):
        """Initialize a NodePieceMLPForVertexClassification.
        Initializes the model.
        Args:
            num_layers (int):
                The total number of layers in your model.
            out_dim (int):
                The output dimension of the model, a.k.a. the number of classes in the classification task.
            hidden_dim (int):
                The hidden dimension of your model.
            vocab_size (int):
                The number of tokens produced by NodePiece. Can be accessed via the dataloader using `loader.num_tokens`.
            sequence_length (int):
                The number of tokens used to represent a single data instance. Is the sum of `max_anchors` and `max_relational_context` defined in the dataloader.
            embedding_dim (int):
                The dimension to embed the tokens in.
            dropout (float):
                The percentage of dropout to be applied after every layer of the model (excluding the output layer).
            class_weights (torch.Tensor):
                Weight the importance of each class in the classification task when computing loss. Helpful in imbalanced classification tasks.
        """
        super().__init__()
        self.model = BaseNodePieceMLPModel(
            num_layers, out_dim, hidden_dim, vocab_size, sequence_length, embedding_dim, dropout)
        self.metrics = ClassificationMetrics(out_dim)
        self.class_weight = class_weights

    def forward(self, batch, get_probs=False, **kwargs):
        """Make a forward pass.
        Args:
            batch:
                The batch of data, in the same format as the data produced by `NodePieceMLPTransform`
            get_probs (bool, optional):
                Return the softmax scores of the raw logits, which can be interpreted as probabilities. Defaults to false.
        """
        logits = self.model.forward(batch)
        if get_probs:
            return F.softmax(logits, dim=1)
        else:
            return logits

    def compute_loss(self, logits, batch, loss_fn=None, **kwargs):
        """Compute loss.
        Args:
            logits (torch.Tensor):
                The output of the model.
            batch:
                The batch of data, in the same format as the data produced by `NodePieceMLPTransform`
            loss_fn:
                A PyTorch-compatible function to produce the loss of the model, which takes in logits, the labels, and optionally the class_weights.
                Defaults to Cross Entropy.
        """
        if not (loss_fn):
            loss_fn = F.cross_entropy
            loss = loss_fn(logits, batch["y"].long(), self.class_weight)
        else:
            loss = loss_fn(logits, batch["y"].long())
        return loss
