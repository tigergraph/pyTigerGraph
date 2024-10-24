import unittest

import numpy as np
from pyTigerGraph.gds.metrics import (Accumulator,
                                      Accuracy,
                                      BinaryPrecision,
                                      BinaryRecall,
                                      Recall,
                                      Precision,
                                      MSE,
                                      RMSE,
                                      MAE,
                                      HitsAtK,
                                      RecallAtK,
                                      ConfusionMatrix)


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


class TestGDSBinaryRecall(unittest.TestCase):
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


class TestGDSBinaryPrecision(unittest.TestCase):
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


class TestGDSRecall(unittest.TestCase):
    def test_init(self):
        measure = Recall(num_classes=2)
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = Recall(num_classes=2)
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
        measure = Precision(num_classes=2)
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = Precision(num_classes=2)
        preds = np.array([1, 1])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.5)
        preds = np.array([1, 0, 1])
        truth = np.array([1, 1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.5)


class TestGDSMSE(unittest.TestCase):
    def test_init(self):
        measure = MSE()
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = MSE()
        preds = np.array([1, 0.5])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.125)
        preds = np.array([1, 0, 0.5])
        truth = np.array([1, 0.5, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.15)


class TestGDSRMSE(unittest.TestCase):
    def test_init(self):
        measure = RMSE()
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = RMSE()
        preds = np.array([1, 0.5])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.125**.5)
        preds = np.array([1, 0, 0.5])
        truth = np.array([1, 0.5, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.15**.5)


class TestGDSMAE(unittest.TestCase):
    def test_init(self):
        measure = MAE()
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = MAE()
        preds = np.array([1, 0.5])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.25)
        preds = np.array([1, 0, 0.5])
        truth = np.array([1, 0.5, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.3)


class TestGDSHitsAtK(unittest.TestCase):
    def test_init(self):
        measure = HitsAtK(k=1)
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = HitsAtK(k=1)
        preds = np.array([0.2, 0.5])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0)
        preds = np.array([0.7, 0.1, 0.5])
        truth = np.array([1, 0.5, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0.5)


class TestGDSRecallAtK(unittest.TestCase):
    def test_init(self):
        measure = RecallAtK(k=1)
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = RecallAtK(k=1)
        preds = np.array([0.2, 0.5])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 0)
        preds = np.array([0.7, 0.1, 0.5])
        truth = np.array([1, 1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value, 1/3)


class TestGDSConfusionMatrix(unittest.TestCase):
    def test_init(self):
        measure = ConfusionMatrix(num_classes=2)
        self.assertIsNone(measure.value)

    def test_update(self):
        measure = ConfusionMatrix(num_classes=2)
        preds = np.array([1, 1])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value.values[1, 1], 1)
        preds = np.array([1, 1])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value.values[1, 1], 2)

    def test_update_multiclass(self):
        measure = ConfusionMatrix(num_classes=4)
        preds = np.array([1, 1, 3, 2])
        truth = np.array([1, 0, 3, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value.values[3, 3], 1)
        preds = np.array([1, 1])
        truth = np.array([1, 0])
        measure.update(preds, truth)
        self.assertEqual(measure.value.values[1, 1], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
