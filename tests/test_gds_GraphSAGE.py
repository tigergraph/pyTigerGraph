import unittest
from pyTigerGraphUnitTest import make_connection

import logging
import os
from pyTigerGraph.gds.trainer import BaseCallback
from pyTigerGraph.gds.models.GraphSAGE import GraphSAGEForLinkPrediction, GraphSAGEForVertexClassification, GraphSAGEForVertexRegression


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
        model = GraphSAGEForVertexClassification(2, 2, 128, 0.5)
        self.assertEqual(len(list(model.parameters())), 6)

    def test_fit(self):
        train_loader = self.conn.gds.neighborLoader(
            v_in_feats=["x"],
            v_out_labels=["y"],
            num_batches=5,
            e_extra_feats=["is_train", "is_val"],
            output_format="PyG",
            num_neighbors=10,
            num_hops=2,
            filter_by="train_mask",
            shuffle=False,
        )

        valid_loader = self.conn.gds.neighborLoader(
            v_in_feats=["x"],
            v_out_labels=["y"],
            num_batches=5,
            e_extra_feats=["is_train", "is_val"],
            output_format="PyG",
            num_neighbors=10,
            num_hops=2,
            filter_by="val_mask",
            shuffle=False,
        )

        gs = GraphSAGEForVertexClassification(num_layers=2,
                                              out_dim=7,
                                              dropout=.2,
                                              hidden_dim=128)

        trainer_args = {"callbacks": [TestingCallback("cora_fit")]}
        gs.fit(train_loader, valid_loader, 2, trainer_kwargs=trainer_args)

        ifLogged = os.path.isfile("./logs/train_results_cora_fit.log")
        self.assertEqual(ifLogged, True)


class TestHeterogeneousVertexClassificationGraphSAGE(unittest.TestCase):
    def test_init(self):
        metadata = (['Actor', 'Movie', 'Director'],
                    [('Actor', 'actor_movie', 'Movie'),
                     ('Movie', 'movie_actor', 'Actor'),
                     ('Movie', 'movie_director', 'Director'),
                     ('Director', 'director_movie', 'Movie')])

        model = GraphSAGEForVertexClassification(2, 3, 256, .2, metadata)
        self.assertEqual(len(list(model.parameters())), 24)


class TestHomogeneousVertexRegression(unittest.TestCase):
    def test_init(self):
        model = GraphSAGEForVertexRegression(2, 1, 128, 0.5)
        self.assertEqual(len(list(model.parameters())), 6)


class TestHeterogeneousVertexRegression(unittest.TestCase):
    def test_init(self):
        metadata = (['Actor', 'Movie', 'Director'],
                    [('Actor', 'actor_movie', 'Movie'),
                     ('Movie', 'movie_actor', 'Actor'),
                     ('Movie', 'movie_director', 'Director'),
                     ('Director', 'director_movie', 'Movie')])
        model = GraphSAGEForVertexRegression(2, 1, 128, 0.5, metadata)
        self.assertEqual(len(list(model.parameters())), 24)


class TestHomogeneousLinkPrediction(unittest.TestCase):
    def test_init(self):
        model = GraphSAGEForLinkPrediction(2, 128, 128, 0.5)
        self.assertEqual(len(list(model.parameters())), 6)


class TestHeterogeneousLinkPrediction(unittest.TestCase):
    def test_init(self):
        metadata = (['Actor', 'Movie', 'Director'],
                    [('Actor', 'actor_movie', 'Movie'),
                     ('Movie', 'movie_actor', 'Actor'),
                     ('Movie', 'movie_director', 'Director'),
                     ('Director', 'director_movie', 'Movie')])
        model = GraphSAGEForLinkPrediction(2, 128, 128, 0.5, metadata)
        self.assertEqual(len(list(model.parameters())), 24)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
