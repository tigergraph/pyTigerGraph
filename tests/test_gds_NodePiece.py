import unittest
from pyTigerGraphUnitTest import make_connection

import torch
import logging
import os
from pyTigerGraph.gds.models.NodePieceMLP import NodePieceMLPForVertexClassification


class TestHomogeneousVertexClassificationGraphSAGE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        model = NodePieceMLPForVertexClassification(num_layers=4,
                     hidden_dim=128,
                     out_dim=2,
                     dropout=0.5,
                     vocab_size=10,
                     sequence_length=20)
        self.assertEqual(len(list(model.parameters())), 7)
        self.assertEqual(model.model.base_embedding.embedding.weight.shape[0], 10)
        self.assertEqual(model.model.base_embedding.embedding.weight.shape[1], 768)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
