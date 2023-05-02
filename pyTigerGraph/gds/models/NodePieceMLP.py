from . import base_model as bm
from ..metrics import ClassificationMetrics

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
except:
    raise Exception("PyTorch is required to use NodePiece MLPs. Please install PyTorch")

class BaseNodePieceEmbeddingTable(nn.Module):
    def __init__(self,
                 vocab_size: int,
                 sequence_length: int,
                 embedding_dim: int=768):
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
    def __init__(self, num_layers, out_dim, hidden_dim, vocab_size, sequence_length, embedding_dim = 768, dropout = 0.0):
        super().__init__()
        self.embedding_dim = embedding_dim
        self.vocab_size = vocab_size
        self.sequence_length = sequence_length
        self.base_embedding = BaseNodePieceEmbeddingTable(vocab_size, sequence_length, embedding_dim)


        self.num_embedding_dim = embedding_dim*sequence_length
        self.in_layer = None
        self.hidden_dim = hidden_dim
        self.dropout = dropout
        self.out_layer = nn.Linear(self.hidden_dim, out_dim)
        self.hidden_layers = nn.ModuleList([nn.Linear(hidden_dim, hidden_dim) for _ in range(num_layers-2)])

    def forward(self, batch):
        if not self.in_layer:
            if "features" in list(batch.keys()): 
                self.in_layer = nn.Linear(batch["features"].shape[1] + self.num_embedding_dim, self.hidden_dim)
            else:
                self.in_layer = nn.Linear(self.num_embedding_dim, self.hidden_dim)
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
    def __init__(self, num_layers, out_dim, hidden_dim, vocab_size, sequence_length, embedding_dim = 768, dropout = 0.0, class_weights = None):
        super().__init__()
        self.model = BaseNodePieceMLPModel(num_layers, out_dim, hidden_dim, vocab_size, sequence_length, embedding_dim, dropout)
        self.metrics = ClassificationMetrics(out_dim)
        self.class_weight = class_weights

    def forward(self, batch, get_probs=False, **kwargs):
        logits = self.model.forward(batch)
        if get_probs:
            return F.softmax(logits, dim=1)
        else:
            return logits

    def compute_loss(self, logits, batch, loss_fn = None, **kwargs):
        if not(loss_fn):
            loss_fn = F.cross_entropy
            loss = loss_fn(logits, batch["y"].long(), self.class_weight)
        else:
            loss = loss_fn(logits, batch["y"].long())
        return loss