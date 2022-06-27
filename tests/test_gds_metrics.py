import unittest

import numpy as np
from pyTigerGraph.gds.metrics import Accumulator, Accuracy, BinaryPrecision, BinaryRecall


class TestGDSAccumulator(unittest.TestCase):
    def test_init(self):
        measure = Accumulator()
        self.assertEqual(measure._cumsum, 0)
        self.assertEqual(measure._count, 0)
        self.assertEqual(measure.total, 0)
        self.assertEqual(measure.count, 0)
        self.assertEqual(measure.mean, 0)

    def test_update(self):
        measure = Accumulator()
        measure.update(1.5)
        self.assertEqual(measure.total, 1.5)
        self.assertEqual(measure.count, 1)
        self.assertEqual(measure.mean, 1.5)
        measure.update(3.0, 2)
        self.assertEqual(measure.total, 4.5)
        self.assertEqual(measure.count, 3)
        self.assertEqual(measure.mean, 1.5)


class TestGDSAccuracy(unittest.TestCase):
    def test_init(self):
        measure = Accuracy()
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = Accuracy()
        preds = np.array([1, 1])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.5)
        preds = np.array([1, 0, 1])
        truth = np.array([1, 0, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 3 / 5)


class TestGDSRecall(unittest.TestCase):
    def test_init(self):
        measure = BinaryRecall()
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = BinaryRecall()
        preds = np.array([1, 1])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 1.0)
        preds = np.array([1, 0, 1])
        truth = np.array([1, 1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 2 / 3)


class TestGDSPrecision(unittest.TestCase):
    def test_init(self):
        measure = BinaryPrecision()
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = BinaryPrecision()
        preds = np.array([1, 1])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.5)
        preds = np.array([1, 0, 1])
        truth = np.array([1, 1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.5)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
