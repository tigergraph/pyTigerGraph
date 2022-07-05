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
        self._cumsum: float = 0.0
        self._count: int = 0

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
        self._count += labels.sum()

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
        self._count += preds.sum()

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
