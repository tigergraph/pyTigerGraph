"""pyTigerGraph GDS Metrics.
:stem: latexmath

Utility for gathering metrics for GNN predictions.
"""

from numpy import ndarray

__all__ = ["Accumulator", "Accuracy", "BinaryPrecision", "BinaryRecall"]


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
    """Recall Metric.

    Recall = stem:[\frac{\sum(predictions * labels)}{\sum(labels)}]

    This metric is for binary classifications, i.e., both predictions and labels are arrays of 0's and 1's.

    Usage:

    * Call the update function to add predictions and labels.
    * Get recall score at any point by accessing the value property.
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


class BinaryPrecision(Accumulator):
    """Precision Metric.

    Precision = stem:[\frac{\sum(predictions * labels)}{\sum(predictions)}]

    This metric is for binary classifications, i.e., both predictions and labels are arrays of 0's and 1's.

    Usage:

    * Call the update function to add predictions and labels.
    * Get precision score at any point by accessing the value property.
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
        self._count += int(preds.sum())

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

    @property
    def value(self) -> float:
        '''Get RMSE value.
            Returns:
                RMSE value (float).
        '''
        if self._count > 0:
            return self.mean**.5
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
        self._count += int(preds.sum())

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
    def __init__(self):
        super().__init__()
        self.reset_metrics()

    def reset_metrics(self):
        super().reset_metrics()
        self.accuracy = Accuracy()
        self.precision = None #TODO implement prcesion
        self.recall = None #TODO implement recall

    def update_metrics(self, loss, out, batch, target_type=None):
        super().update_metrics(loss, out, batch)
        pred = out.argmax(dim=1)
        if target_type:
            self.accuracy.update(pred[batch[target_type].is_seed], batch[target_type].y[batch[target_type].is_seed])
        else:
            self.accuracy.update(pred[batch.is_seed], batch.y[batch.is_seed])

    def get_metrics(self):
        super_met = super().get_metrics()
        metrics = {"accuracy": self.accuracy.value}
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
    def __init__(self):
        super().__init__()
        self.reset_metrics()

    def reset_metrics(self):
        super().reset_metrics()
        self.precision_at_1 = BinaryPrecision()
        self.recall_at_1 = BinaryRecall()

    def update_metrics(self, loss, out, batch, target_type=None):
        super().update_metrics(loss, out, batch)
        self.precision_at_1.update(out, batch.y)
        self.recall_at_1.update(out, batch.y)

    def get_metrics(self):
        super_met = super().get_metrics()
        metrics = {"precision_at_1": self.precision_at_1.value,
                   "recall_at_1": self.recall_at_1.value}
        metrics.update(super_met)
        return metrics

