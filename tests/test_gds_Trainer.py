from pyTigerGraph.gds.trainer import Trainer
from pyTigerGraph.gds.models.base_model import BaseModel
import unittest

'''
import pandas as pd
import torch
from dgl import DGLGraph
from pyTigerGraphUnitTest import make_connection
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData
'''

class dummy_model(BaseModel):
    def __init__(self):
        super().__init__()

    def forward(self, batch):
        return "forward"

    def compute_loss(self):
        return 1

    def compute_metrics(self):
        return


class TestGDSBaseLoader(unittest.TestCase):
    def test_init(self):

        trainer = Trainer(gs, train_loader, valid_loader)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)