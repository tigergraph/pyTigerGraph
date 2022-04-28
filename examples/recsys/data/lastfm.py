import os
import numpy as np
import pandas as pd
import networkx as nx
import torch
from torch_geometric.data import Data, Dataset
from torch_geometric.transforms import RandomLinkSplit
from torch_geometric_temporal.signal import DynamicGraphTemporalSignal
from torch_geometric_temporal.signal import temporal_signal_split
import inspect


def add_complementary_edges(data: Data):
    """Add
    :param data:
    :return:
    """
    transform = RandomLinkSplit(is_undirected=True,
                                add_negative_train_samples=False,
                                num_val=0,
                                num_test=0,
                                neg_sampling_ratio=0)
    rand_data, _, _ = transform(data)
    # print(rand_data.edge_index)
    # print(d1.edge_index)
    # print(d2.edge_index)
    
    return rand_data


def load_snapshots(input_csv, num_snapshots):
    """Load bipartite edgelist CSV file into PyG-Temporal dataset
    :param input_csv: Input edgelist CSV
    :param num_snapshots: Number of snapshots for PyG-Temporal
    :return: PyG-Temporal dataset (DynamicGraphTemporalSignal), number of users and items
    """
    df = pd.read_csv(input_csv)
    num_user_ids = df["user_id"].unique().size
    num_item_ids = df["item_id"].unique().size
    # num_all_ids = num_user_ids + num_item_ids
    
    num_rows, num_feats = df.shape
    rows_per_snapshot = num_rows // num_snapshots
    start_index = [rows_per_snapshot * i for i in range(num_snapshots)] + [num_rows]
    
    edge_index_t = list()
    edge_label_index_t = list()
    edge_weight_t = list()
    node_feats_t = list()
    targets_t = list()
    
    for i in range(num_snapshots):
        sub_df = df.iloc[start_index[i]:start_index[i+1]]
        data, _, _ = load_dataset(sub_df)
        # data = add_complementary_edges(data)
        # edge_label_index_t.append(data.edge_label_index)
        edge_index_t.append(data.edge_index)
        edge_weight_t.append(data.edge_attr)
        node_feats_t.append(data.x)
        targets_t.append(data.y)
    
    dataset_t = DynamicGraphTemporalSignal(edge_index_t, edge_weight_t, node_feats_t, targets_t)
    
    return dataset_t, num_user_ids, num_item_ids


def split_snapshots(dataset_t, test_split=0.15, val_split=0.15):
    train_split = 1.0 - (test_split + val_split)
    train_dataset, val_test_dataset = temporal_signal_split(dataset_t, train_ratio=train_split)
    val_ratio = val_split / (val_split + test_split)
    val_dataset, test_dataset = temporal_signal_split(val_test_dataset, train_ratio=val_ratio)
    return train_dataset, val_dataset, test_dataset


def load_dataset(df, device="cpu"):
    """Load pandas DataFrame as edgelist to PyG graph data
    :param df: pandas DataFrame
    :param device: PyTorch device
    :return: PyG dataset, # of users, # of items
    """
    num_user_ids = df["user_id"].unique().size
    num_item_ids = df["item_id"].unique().size
    num_all_ids = num_user_ids + num_item_ids
    # print(num_user_ids, num_item_ids, num_all_ids)
    
    g = nx.from_pandas_edgelist(df, "user_id", "item_id", create_using=nx.MultiGraph())

    # Node-based features (degree)
    # NOTE: the number of node features must be 2 or more
    degs = {n: d for n, d in g.degree()}
    deg_feats = np.array([degs.get(i, 0.0) for i in range(num_all_ids)], dtype=np.float64)
    deg_feats /= deg_feats.max(initial=0)
    deg_feats = np.sqrt(deg_feats)
    deg_feats = torch.Tensor(np.array([deg_feats, deg_feats]).T)

    # edgelist
    src_ids = df["user_id"].tolist()
    dst_ids = df["item_id"].tolist()
    edges_index = torch.Tensor(np.array([src_ids, dst_ids])).long()

    # Construct PyG object
    lastfm_data = Data(x=deg_feats, edge_index=edges_index, edge_attr=None, y=None).to(
        device, non_blocking=True
    )
    # print(inspect.currentframe())
    # print("edge_index:", lastfm_data.edge_index)

    return lastfm_data, num_user_ids, num_item_ids


class PlainData(Data):
    """
    Custom Data class for use in PyG. Basically the same as the original Data class from PyG, but
    overrides the __inc__ method because otherwise the DataLoader was incrementing indices unnecessarily.
    Now it functions more like the original DataLoader from PyTorch itself.
    See here for more information: https://pytorch-geometric.readthedocs.io/en/latest/notes/batching.html
    """

    def __inc__(self, key, value, *args, **kwargs):
        return 0


class LastFMDataset(Dataset):
    """
    Dataset object containing the user-item supervision/evaluation edges
    to calculate loss or evaluation metrics
    """

    def __init__(self, root, edge_index, transform=None, pre_transform=None):
        self.edge_index = edge_index
        # customers will all be in row 0, b/c sorted by RandLinkSplit
        self.unique_idxs = torch.unique(edge_index[0, :]).tolist()
        self.num_nodes = len(self.unique_idxs)
        super().__init__(root, transform, pre_transform)

    def len(self):
        return self.num_nodes

    def get(self, idx):  # returns all outgoing edges associated with customer idx
        edge_index = self.edge_index[:, self.edge_index[0, :] == idx]
        return PlainData(edge_index=edge_index)
