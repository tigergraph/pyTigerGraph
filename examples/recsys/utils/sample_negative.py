import torch
from torch_geometric.data import Data as PyGData
import inspect


def sample_negative_edges(batch: PyGData, num_customers: int, num_nodes: int, device) -> PyGData:
    """Randomly generate negative (non-existing) edges
    :param batch: Batch of positive (existing) edges
    :param num_customers: # of users
    :param num_nodes: # of total nodes (users + items)
    :param device: Device for PyTorch
    :return: Generated negative (non-existing) edges
    """
    negs = []
    # print(inspect.currentframe())
    # print("batch.edge_index:", batch.edge_index)
    for i in batch.edge_index[0, :]:  # looping over customers
        assert i < num_customers, f"The source node {i} must be user's"  # Must be user ID
        rand = torch.randint(num_customers, num_nodes, (1,))  # randomly sample an item
        negs.append(rand.item())
    edge_index_negs = torch.row_stack([batch.edge_index[0, :], torch.LongTensor(negs).to(device)])
    return PyGData(edge_index=edge_index_negs)
