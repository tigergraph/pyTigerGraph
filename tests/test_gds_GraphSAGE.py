import unittest

import torch
from pyTigerGraph.gds.models.GraphSAGE import GraphSAGEForLinkPrediction, GraphSAGEForVertexClassification, GraphSAGEForVertexRegression

class TestHomogeneousVertexClassificationGraphSAGE(unittest.TestCase):
    def test_init(self):
        model = GraphSAGEForVertexClassification(2, 2, 0.5, 128)
        self.assertEqual(len(list(model.parameters())), 6)

class TestHeterogeneousVertexClassificationGraphSAGE(unittest.TestCase):
    def test_init(self):
        metadata = (['Actor', 'Movie', 'Director'], 
                [('Actor', 'actor_movie', 'Movie'), 
                ('Movie', 'movie_actor', 'Actor'), 
                ('Movie', 'movie_director', 'Director'), 
                ('Director', 'director_movie', 'Movie')])  # should be able to get metadata from dataloader

        model = GraphSAGEForVertexClassification(2, 3, .2, 256, metadata)
        self.assertEqual(len(list(model.parameters())), 24)



if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
