import pandas as pd


def encode_labels(series: pd.Series) -> pd.Series:
    """A sped up version of scikit-learn's LabelEncoder that only works on pandas Series"""
    return series.astype("category").cat.codes
