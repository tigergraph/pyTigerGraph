from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    epochs: int  # number of training epochs
    k: int  # value of k for recall@k. It is important to set this to a reasonable value!
    num_layers: int  # number of GNN layers (i.e., number of hops to consider during propagation)
    batch_size: int  # batch size. refers to the # of customers in the batch (each will come with all of its edges)
    embedding_dim: int  # dimension to use for the customer/article embeddings
    lr: float  # Learning rate
    model_name: str  # GNN model name (gcn, egcn or lgcn)
    save_emb_dir: Optional[
        str
    ]  # path to save multi-scale embeddings during test(). If None, will not save any embeddings


config = Config(
    epochs=10, k=10, num_layers=2, batch_size=1000, lr=1e-3,
    embedding_dim=64, save_emb_dir=None, model_name="lgcn"
)
