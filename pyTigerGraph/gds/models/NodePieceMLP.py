from . import base_model as bm
from ..metrics import ClassificationMetrics, LinkPredictionMetrics

try:
    import torch
    import torch.nn as nn
    import tourch.nn.functional as F
except:
    raise Exception("PyTorch is required to use NodePiece MLPs. Please install PyTorch")

class BaseNodePieceModel(bm.BaseModel):
    def __init__(self, num_layers, out_dim, dropout, embedding_dim, heterogeneous=None):
        pass