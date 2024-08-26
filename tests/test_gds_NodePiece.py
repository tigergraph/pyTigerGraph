import unittest
from pyTigerGraphUnitTest import make_connection

import logging
import os
from pyTigerGraph.gds.models.NodePieceMLP import NodePieceMLPForVertexClassification
from pyTigerGraph.gds.trainer import BaseCallback
from pyTigerGraph.gds.transforms.nodepiece_transforms import NodePieceMLPTransform


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


class TestHomogeneousVertexClassificationGraphSAGE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")

    def test_init(self):
        model = NodePieceMLPForVertexClassification(num_layers=4,
                                                    hidden_dim=128,
                                                    out_dim=7,
                                                    dropout=0.5,
                                                    vocab_size=10,
                                                    sequence_length=20)
        self.assertEqual(len(list(model.parameters())), 7)
        self.assertEqual(
            model.model.base_embedding.embedding.weight.shape[0], 10)
        self.assertEqual(
            model.model.base_embedding.embedding.weight.shape[1], 768)

    def test_fit(self):
        t = NodePieceMLPTransform(label="y")
        train_loader, valid_loader = self.conn.gds.nodepieceLoader(
            v_feats=["y"],
            target_vertex_types="Paper",
            clear_cache=True,
            compute_anchors=True,
            filter_by=["train_mask", "val_mask"],
            anchor_percentage=0.1,
            max_anchors=10,
            max_distance=10,
            num_batches=5,
            use_cache=False,
            shuffle=False,
            reverse_edge=True,
            callback_fn=lambda x: t(x),
            timeout=600_000)

        model = NodePieceMLPForVertexClassification(num_layers=4,
                                                    hidden_dim=128,
                                                    out_dim=7,
                                                    dropout=0.5,
                                                    vocab_size=train_loader.num_tokens,
                                                    sequence_length=20)

        trainer_args = {"callbacks": [TestingCallback("cora_fit_np")]}
        model.fit(train_loader, valid_loader, 2, trainer_kwargs=trainer_args)

        ifLogged = os.path.isfile("./logs/train_results_cora_fit_np.log")
        self.assertEqual(ifLogged, True)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
