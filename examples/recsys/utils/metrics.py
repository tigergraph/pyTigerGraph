import torch
import numpy as np
from icecream import ic as iprint


def recall_at_k(all_ratings: torch.Tensor, k: int, num_users: int, ground_truth: torch.Tensor, unique_users) -> dict:
    """
    Calculates recall@k during validation/testing for a single batch.
    args:
      all_ratings: array of shape [number of users in batch, number of items in whole dataset]
      k: the value of k to use for recall@k
      num_customers: the number of customers in the dataset
      ground_truth: array of shape [2, X] where each column is a pair of (customer_idx, positive article idx). This is the
         batch that we are calculating metrics on.
      unique_customers: 1D vector of length [number of customers in batch], which specifies which customer corresponds
         to each row of all_ratings
    returns:
      Dictionary of customer ID -> recall@k on that customer
    """
    _, top_k = torch.topk(all_ratings, k=k, dim=1)
    # Need to shift up by num_customers to get the actual article indices
    top_k += num_users  # topk returned indices of articles in ratings, which doesn't include customers.

    # Calculate recall@k
    ret = {}
    for i, user_id in enumerate(unique_users):
        pos_articles = ground_truth[1, ground_truth[0, :] == user_id]
        
        pos_articles = np.unique(pos_articles.numpy())
        k_recs = top_k[i, :]  # top k recommendations for customer
        num_tp = len(np.intersect1d(pos_articles, k_recs))
        recall = num_tp / len(pos_articles)
        ret[user_id] = recall
    return ret
