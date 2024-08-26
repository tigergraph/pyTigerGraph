from pyTigerGraph.gds.trainer import Trainer
from pyTigerGraph.gds.models.base_model import BaseModel
from pyTigerGraph.gds.dataloaders import NeighborLoader
from pyTigerGraph.gds.models.GraphSAGE import GraphSAGEForVertexClassification
from pyTigerGraph.gds.trainer import BaseCallback
import os
import unittest
import logging
from pyTigerGraphUnitTest import make_connection

'''
import pandas as pd
import torch
from dgl import DGLGraph
from pyTigerGraphUnitTest import make_connection
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData
'''


class TestingCallback(BaseCallback):
    def __init__(self, test_name, output_dir="./logs"):
        self.output_dir = output_dir
        self.best_loss = float("inf")
        os.makedirs(self.output_dir, exist_ok=True)
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                            filename=output_dir+'/train_results_'+test_name+'.log',
                            filemode='w',
                            encoding='utf-8',
                            level=logging.INFO)

    def on_train_step_end(self, trainer):
        logger = logging.getLogger(__name__)
        logger.info("train_step:"+str(trainer.get_train_step_metrics()))

    def on_eval_end(self, trainer):
        logger = logging.getLogger(__name__)
        logger.info("evaluation:"+str(trainer.get_eval_metrics()))

    def on_epoch_end(self, trainer):
        trainer.eval()


class TestGDSTrainer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def testHomogeneousVertexClassTraining(self):
        train = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )

        valid = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="val_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )

        gs = GraphSAGEForVertexClassification(num_layers=2,
                                              out_dim=7,
                                              dropout=.2,
                                              hidden_dim=128)

        trainer = Trainer(gs, train, valid, callbacks=[
                          TestingCallback("cora_class")])

        trainer.train(num_epochs=1)
        ifLogged = os.path.isfile("./logs/train_results_cora_class.log")
        self.assertEqual(ifLogged, True)

    def testHomogeneousVertexClassPredict(self):
        train, valid, infer = self.conn.gds.neighborLoader(
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by=["train_mask", "val_mask", ""],
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )

        gs = GraphSAGEForVertexClassification(num_layers=2,
                                              out_dim=7,
                                              dropout=.2,
                                              hidden_dim=128)

        trainer = Trainer(gs, train, valid, callbacks=[
                          TestingCallback("cora_class")])

        trainer.train(num_epochs=1)
        out, _ = trainer.predict(infer.fetch(
            [{"primary_id": 1, "type": "Paper"}]))
        self.assertEqual(out.shape[1], 7)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
