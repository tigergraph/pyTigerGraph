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
    neg_users = []
    neg_items = []
    # print(batch.edge_index.shape)
    for src, dst in batch.edge_index.T:  # looping over node pairs
        # assert src < num_customers, f"The source node {src} must be user's"  # Must be user ID
        if not (src < num_customers and num_customers <= dst < num_nodes):  # not user-item edges
            # neg_uid = torch.randint(0, num_customers, (1,)).item()
            continue
        else:
            neg_uid = src.item()
        neg_users.append(neg_uid)
        
        rand = torch.randint(num_customers, num_nodes, (1,))  # randomly sample an item
        neg_items.append(rand.item())  # item ID
    # edge_index_negs = torch.row_stack([batch.edge_index[0, :], torch.LongTensor(neg_items).to(device)])
    edge_index_negs = torch.row_stack([torch.LongTensor(neg_users), torch.LongTensor(neg_items)]).to(device)
    return PyGData(edge_index=edge_index_negs)
