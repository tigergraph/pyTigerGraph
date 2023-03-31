"""pyTigerGraph GDS Metrics.
:stem: latexmath

Utility for gathering metrics for GNN predictions.
"""

from numpy import ndarray
import pandas as pd
import numpy as np
import warnings
from typing import Union

__all__ = ["Accumulator", "Accuracy", "BinaryPrecision", "BinaryRecall", "Precision", "Recall"]


class Accumulator:
    """NO DOC: Base Metric Accumulator.

    Usage:

    * Call the update function to add a value.
    * Get running average by accessing the mean property, running sum by the total property, and
    number of values by the count property.
    """

    def __init__(self) -> None:
        '''Initialize the accumulator.'''
        self._cumsum = 0.0
        self._count = 0

    def update(self, value: float, count: int = 1) -> None:
        """Add a value to the running sum.

        Args:
            value (float): 
                The value to be added.
            count (int, optional): 
                The input value is by default treated as a single value.
                If it is a sum of multiple values, the number of values can be specified by this
                length argument, so that the running average can be calculated correctly. Defaults to 1.
        """
        self._cumsum += float(value)
        self._count += int(count)

    @property
    def mean(self) -> float:
        """Get running average."""
        if self._count > 0:
            return self._cumsum / self._count
        else:
            return 0.0

    @property
    def total(self) -> float:
        """Get running sum."""
        return self._cumsum

    @property
    def count(self) -> int:
        """Get running count"""
        return self._count


class Accuracy(Accumulator):
    """Accuracy Metric.

    Accuracy = sum(predictions == labels) / len(labels)

    Usage:

    * Call the update function to add predictions and labels.
    * Get accuracy score at any point by accessing the value property.
    """

    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"
        self._cumsum += float((preds == labels).sum())
        self._count += len(labels)

    @property
    def value(self) -> float:
        '''Get accuracy score.
            Returns:
                Accuracy score (float).
        '''
        if self._count > 0:
            return self.mean
        else:
            return None


class BinaryRecall(Accumulator):
    """DEPRECATED: Binary Recall Metric.

    Recall = stem:[\frac{\sum(predictions * labels)}{\sum(labels)}]

    This metric is for binary classifications, i.e., both predictions and labels are arrays of 0's and 1's.

    Usage:

    * Call the update function to add predictions and labels.
    * Get recall score at any point by accessing the value property.
    """
    def __init__(self) -> None:
        super().__init__()
        warnings.warn(
                "The `BinaryRecall` metric is deprecated; use `Recall` metric instead.",
                DeprecationWarning)

    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"
        self._cumsum += float((preds * labels).sum())
        self._count += int(labels.sum())

    @property
    def value(self) -> float:
        '''Get recall score.
            Returns:
                Recall score (float).
        '''
        if self._count > 0:
            return self.mean
        else:
            return None

class ConfusionMatrix(Accumulator):
    """Confusion Matrix Metric.
    Updates a confusion matrix as new updates occur.

    Args:
        num_classes (int):
            Number of classes in your classification task.
    """
    def __init__(self, num_classes: int) -> None:
        super().__init__()
        self.num_classes = num_classes

    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"


        confusion_mat = np.zeros((self.num_classes, self.num_classes))
        for pair in zip(labels.tolist(), preds.tolist()):
            confusion_mat[int(pair[0]), int(pair[1])] += 1

        self._cumsum += confusion_mat
        self._count += len(labels)

    @property
    def value(self) -> np.array:
        '''Get the confusion matrix.
            Returns:
                Consfusion matrix in dataframe form.
        '''
        if self._count > 0:
            return pd.DataFrame(self._cumsum, columns=["predicted_"+ str(i) for i in range(self.num_classes)], index=["label_"+str(i) for i in range(self.num_classes)])
        else:
            return None

class Recall(ConfusionMatrix):
    """Recall Metric.

    Recall = stem:[\frac{true positives}{\sum(true positives + false negatives)}
    This metric is for classification, i.e., both predictions and labels are arrays of multiple whole numbers.

    Usage:

    * Call the update function to add predictions and labels.
    * Get recall score at any point by accessing the value property.
    """

    @property
    def value(self) -> Union[dict, float]:
        '''Get recall score for each class.
            Returns:
                Recall score for each class or the average recall if `num_classes` == 2.
        '''
        if self._count > 0:
            cm = self._cumsum
            recalls = {}

            for c in range(self.num_classes):
                tp = cm[c,c]
                fn = sum(cm[c, :]) - tp
                recalls[c] = tp/(tp+fn)
            if self.num_classes == 2:
                return recalls[1]
            else:
                return recalls
        else:
            return None

class BinaryPrecision(Accumulator):
    """DEPRECATED: Binary Precision Metric.

    Precision = stem:[\frac{\sum(predictions * labels)}{\sum(predictions)}]

    This metric is for binary classifications, i.e., both predictions and labels are arrays of 0's and 1's.

    Usage:

    * Call the update function to add predictions and labels.
    * Get precision score at any point by accessing the value property.
    """

    def __init__(self) -> None:
        super().__init__()
        warnings.warn(
                "The `BinaryPrecision` metric is deprecated; use `Precision` metric instead.",
                DeprecationWarning)

    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"
        self._cumsum += float((preds * labels).sum())
        self._count += int(preds.sum())

    @property
    def value(self) -> float:
        '''Get precision score.
            Returns:
                Precision score (float).
        '''
        if self._count > 0:
            return self.mean
        else:
            return None

class Precision(ConfusionMatrix):
    """Precision Metric.

    Recall = stem:[\frac{true positives}{\sum(true positives + false positives)}
    This metric is for classification, i.e., both predictions and labels are arrays of multiple whole numbers.

    Usage:

    * Call the update function to add predictions and labels.
    * Get recall score at any point by accessing the value property.
    """

    @property
    def value(self) -> Union[dict, float]:
        '''Get precision score for each class.
            Returns:
                Precision score for each class or the average precision if `num_classes` == 2.
        '''
        if self._count > 0:
            cm = self._cumsum
            precs = {}

            for c in range(self.num_classes):
                tp = cm[c,c]
                fp = sum(cm[:, c]) - tp
                precs[c] = tp/(tp+fp)
            if self.num_classes == 2:
                return precs[1]
            else:
                return precs
        else:
            return None

class MSE(Accumulator):
    """MSE Metrc.
    
    MSE = #TODO FILL IN FORMLA

    This metric is for regression tasks, i.e. predicting a n-dimensional vector of float values.

    Usage:

    * Call the update function to add predictions and labels.
    * Get MSE value at any point by accessing the value property.
    """
    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"
        self._cumsum += float(((preds - labels)**2).sum())
        self._count += int(len(preds))

    @property
    def value(self) -> float:
        '''Get MSE score.
            Returns:
                MSE value (float).
        '''
        if self._count > 0:
            return self.mean
        else:
            return None

class RMSE(MSE):
    """RMSE Metric.

    RMSE = #TODO FILL IN FORMULA

    This metric is for regression tasks, i.e. predicting a n-dimensional vector of float values.

    Usage:

    * Call the update function to add predictions and labels.
    * Get RMSE score at any point by accessing the value property.
    """
    def __init__(self):
        super().__init__()

    @property
    def value(self) -> float:
        '''Get RMSE value.
            Returns:
                RMSE value (float).
        '''
        if self._count > 0:
            return (self.mean)**.5
        else:
            return None

class MAE(Accumulator):
    """MAE Metrc.

    MAE = #TODO FILL IN FORMLA

    This metric is for regression tasks, i.e. predicting a n-dimensional vector of float values.

    Usage:

    * Call the update function to add predictions and labels.
    * Get MAE value at any point by accessing the value property.
    """
    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"
        self._cumsum += float(abs((preds - labels)).sum())
        self._count += int(len(preds))

    @property
    def value(self) -> float:
        '''Get MAE score.
            Returns:
                MAE value (float).
        '''
        if self._count > 0:
            return self.mean
        else:
            return None

class HitsAtK(Accumulator):
    """Hits@K Metric.
    Hits@K = #TODO Fill in Formula

    This metric is used in link prediction tasks, i.e. determining if two vertices have an edge between them.
    Also known as Precsion@K.

    Usage:

    * Call the update function to add predictions and labels.
    * Get Hits@K value at any point by accessing the value property.

    Args:
        k (int):
            Number of classes in your classification task.
    """
    def __init__(self, k:int) -> None:
        super().__init__()
        self.k = k

    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"
        top_indices = preds.argsort()[::-1][:self.k]
        self._cumsum += float(labels[top_indices].sum())
        self._count += int(self.k)

    @property
    def value(self) -> float:
        '''Get Hits@K score.
            Returns:
                Hits@K value (float).
        '''
        if self._count > 0:
            return self.mean
        else:
            return None

class RecallAtK(Accumulator):
    """Recall@K Metric.
    Recall@K = #TODO Fill in Formula

    This metric is used in link prediction tasks, i.e. determining if two vertices have an edge between them

    Usage:

    * Call the update function to add predictions and labels.
    * Get Recall@K value at any point by accessing the value property.

    Args:
        k (int):
            Number of classes in your classification task.
    """
    def __init__(self, k:int) -> None:
        super().__init__()
        self.k = k

    def update(self, preds: ndarray, labels: ndarray) -> None:
        """Add predictions and labels to be compared.

        Args:
            preds (ndarray): 
                Array of predicted labels.
            labels (ndarray): 
                Array of true labels.
        """
        assert len(preds) == len(
            labels
        ), "The lists of predictions and labels must have same length"
        top_indices = preds.argsort()[::-1][:self.k]
        self._cumsum += float(labels[top_indices].sum())
        self._count += int(labels.sum())

    @property
    def value(self) -> float:
        '''Get Recall@K score.
            Returns:
                Recall@K value (float).
        '''
        if self._count > 0:
            return self.mean
        else:
            return None

class BaseMetrics():
    def __init__(self):
        self.reset_metrics()
    
    def reset_metrics(self):
        self.loss = Accumulator()

    def update_metrics(self, loss, out, batch, target_type=None):
        self.loss.update(loss)

    def get_metrics(self):
        return {"loss": self.loss.mean}


class ClassificationMetrics(BaseMetrics):
    def __init__(self, num_classes: int=2):
        self.num_classes = num_classes
        super(ClassificationMetrics, self).__init__()
        self.reset_metrics()

    def reset_metrics(self):
        super().reset_metrics()
        self.accuracy = Accuracy()
        self.confusion_matrix = ConfusionMatrix(self.num_classes)
        self.precision = Precision(self.num_classes)
        self.recall = Recall(self.num_classes)

    def update_metrics(self, loss, out, batch, target_type=None):
        super().update_metrics(loss, out, batch)
        pred = out.argmax(dim=1)
        if target_type:
            self.accuracy.update(pred[batch[target_type].is_seed], batch[target_type].y[batch[target_type].is_seed])
            self.confusion_matrix.update(pred[batch[target_type].is_seed], batch[target_type].y[batch[target_type].is_seed])
            self.precision.update(pred[batch[target_type].is_seed], batch[target_type].y[batch[target_type].is_seed])
            self.recall.update(pred[batch[target_type].is_seed], batch[target_type].y[batch[target_type].is_seed])
        else:
            self.accuracy.update(pred[batch.is_seed], batch.y[batch.is_seed])
            self.confusion_matrix.update(pred[batch.is_seed], batch.y[batch.is_seed])
            self.precision.update(pred[batch.is_seed], batch.y[batch.is_seed])
            self.recall.update(pred[batch.is_seed], batch.y[batch.is_seed])

    def get_metrics(self):
        super_met = super().get_metrics()
        metrics = {"accuracy": self.accuracy.value, "precision": self.precision.value, "recall": self.recall.value, "confusion_matrix": self.confusion_matrix.value}
        metrics.update(super_met)
        return metrics

class RegressionMetrics(BaseMetrics):
    def __init__(self):
        super().__init__()
        self.reset_metrics()

    def reset_metrics(self):
        super().reset_metrics()
        self.mse = MSE()
        self.rmse = RMSE()
        self.mae = MAE()

    def update_metrics(self, loss, out, batch, target_type=None):
        super().update_metrics(loss, out, batch)
        self.mse.update(out[batch.is_seed], batch.y[batch.is_seed])
        self.rmse.update(out[batch.is_seed], batch.y[batch.is_seed])
        self.mae.update(out[batch.is_seed], batch.y[batch.is_seed])

    def get_metrics(self):
        super_met = super().get_metrics()
        metrics = {"mse": self.mse.value,
                   "rmse": self.rmse.value,
                   "mae": self.mae.value}
        metrics.update(super_met)
        return metrics


class LinkPredictionMetrics(BaseMetrics):
    def __init__(self, k):
        self.k = k
        super(LinkPredictionMetrics, self).__init__()
        self.reset_metrics()

    def reset_metrics(self):
        super().reset_metrics()
        self.recall_at_k = RecallAtK(self.k)
        self.hits_at_k = HitsAtK(self.k)

    def update_metrics(self, loss, out, batch, target_type=None):
        super().update_metrics(loss, out, batch)
        self.recall_at_k.update(out, batch.y)
        self.hits_at_k.update(out, batch.y)

    def get_metrics(self):
        super_met = super().get_metrics()
        metrics = {"recall_at_k": self.recall_at_k.value,
                   "hits_at_k": self.hits_at_k.value,
                   "k": self.k}
        metrics.update(super_met)
        return metrics

