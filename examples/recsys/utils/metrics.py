import torch
import numpy as np
from icecream import ic as iprint


def recall_at_k(
    all_ratings, k: int, num_customers: int, ground_truth, unique_customers, data_mp
) -> dict:
    """
    Calculates recall@k during validation/testing for a single batch.
    args:
      all_ratings: array of shape [number of customers in batch, number of articles in whole dataset]
      k: the value of k to use for recall@k
      num_customers: the number of customers in the dataset
      ground_truth: array of shape [2, X] where each column is a pair of (customer_idx, positive article idx). This is the
         batch that we are calculating metrics on.
      unique_customers: 1D vector of length [number of customers in batch], which specifies which customer corresponds
         to each row of all_ratings
      data_mp: an array of shape [2, Y]. This is all of the known message-passing edges. We will use this to make sure we
         don't recommend articles that are already known to be in the customer.
    returns:
      Dictionary of customer ID -> recall@k on that customer
    """
    # We don't want to recommend articles that are already known to be in the customer.
    # Set those to a low rating so they won't be recommended
    known_edges = data_mp[
        :, data_mp[0, :] < num_customers
    ]  # removing duplicate edges (since data_mp is undirected). also makes it so
    # that for each column, customer idx is in row 0 and article idx is in row 1
    customer_to_idx_in_batch = {
        customer: i for i, customer in enumerate(unique_customers.tolist())
    }
    exclude_customers, exclude_articles = (
        [],
        [],
    )  # already-known customer/article links. Don't want to recommend these again
    for i in range(known_edges.shape[1]):  # looping over all known edges
        pl, article = known_edges[:, i].tolist()
        if (
            pl in customer_to_idx_in_batch
        ):  # don't need the edges in data_mp that are from customers that are not in this batch
            exclude_customers.append(customer_to_idx_in_batch[pl])
            exclude_articles.append(
                article - num_customers
            )  # subtract num_customers to get indexing into all_ratings correct
    # iprint(all_ratings.shape, len(exclude_customers), len(exclude_articles))
    all_ratings[
        exclude_customers, exclude_articles
    ] = -10000  # setting to a very low score so they won't be recommended
    
    # Get top k recommendations for each customer
    _, top_k = torch.topk(all_ratings, k=k, dim=1)
    # iprint(top_k)
    top_k += num_customers  # topk returned indices of articles in ratings, which doesn't include customers.
    # Need to shift up by num_customers to get the actual article indices

    # Calculate recall@k
    ret = {}
    for i, customer in enumerate(unique_customers):
        pos_articles = ground_truth[1, ground_truth[0, :] == customer]

        k_recs = top_k[i, :]  # top k recommendations for customer
        # if i % 200 == 0:
        #     iprint(i, pos_articles, k_recs)
        
        recall = len(np.intersect1d(pos_articles, k_recs)) / len(pos_articles)
        ret[customer] = recall
    return ret
