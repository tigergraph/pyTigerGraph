from torch_geometric.transforms import RandomLinkSplit
from torch_geometric.data import Data as PyGData
from recsys.data.dataset import FashionDataset
from recsys.data.types import DataLoaderConfig, ArticleIdMap, CustomerIdMap
import torch
import json
from typing import Tuple


def train_test_val_split(
    data: PyGData, config: DataLoaderConfig
) -> Tuple[PyGData, PyGData, PyGData]:
    print("| Splitting the graph into train, val and test")
    transform = RandomLinkSplit(
        is_undirected=True,
        add_negative_train_samples=False,
        num_val=config.val_split,
        num_test=config.test_split,
        neg_sampling_ratio=0,
    )
    train_split, val_split, test_split = transform(data)

    # Confirm that every node appears in every set above
    assert (
        train_split.num_nodes == val_split.num_nodes
        and train_split.num_nodes == test_split.num_nodes
    )
    print(train_split)
    print(val_split)
    print(test_split)
    return train_split, val_split, test_split


config = DataLoaderConfig(test_split=0.15, val_split=0.15)


def create_dataloaders(
    config: DataLoaderConfig,
) -> Tuple[
    Tuple[FashionDataset, PyGData],
    Tuple[FashionDataset, PyGData],
    Tuple[FashionDataset, PyGData],
    CustomerIdMap,
    ArticleIdMap,
]:
    data = torch.load("data/derived/graph.pt")
    train_split, val_split, test_split = train_test_val_split(data, config)

    train_ev = FashionDataset("temp", edge_index=train_split.edge_label_index)
    train_mp = PyGData(edge_index=train_split.edge_index)

    val_ev = FashionDataset("temp", edge_index=val_split.edge_label_index)
    val_mp = PyGData(edge_index=val_split.edge_index)

    test_ev = FashionDataset("temp", edge_index=test_split.edge_label_index)
    test_mp = PyGData(edge_index=test_split.edge_index)

    customer_id_map = read_json("data/derived/customer_id_map_forward.json")
    article_id_map = read_json("data/derived/article_id_map_forward.json")

    return (
        (train_ev, train_mp),
        (val_ev, val_mp),
        (test_ev, test_mp),
        customer_id_map,
        article_id_map,
    )


def read_json(filename: str):
    with open(filename) as f_in:
        return json.load(f_in)
