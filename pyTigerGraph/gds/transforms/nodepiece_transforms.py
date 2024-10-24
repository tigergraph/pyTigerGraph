"""NodePiece Transforms"""


class BaseNodePieceTransform():
    """NO DOC."""

    def __call__(self, data):
        return data

    def __repr__(self):
        return f'{self.__class__.__name__}()'


class NodePieceMLPTransform(BaseNodePieceTransform):
    """NodePieceMLPTransform.
    The NodePieceMLPTransform converts a batch of data from the NodePieceLoader into a format that can be used in a MLP implemented in PyTorch.
    """
    # Assumes numerical types for features and labels. No support for complex datatypes as features.

    def __init__(self, label: str, features: list = [], target_type: str = None):
        """Instantiate a NodePieceMLPTransform.
        Args:
            label (str): 
                The attribute name that corresponds with the label being predicted. Supports numerical datatypes (INT, FLOAT, DOUBLE).
            features (list of str, optional):
                List of attributes to use as features into the model. Supports numerical datatypes (INT, FLOAT, DOUBLE).
            target_type (str, optional):
                The type of vertex to perform predictions on.
        """
        try:
            import torch
        except:
            raise Exception(
                "PyTorch is required to use this transform. Please install PyTorch")
        self.features = features
        self.target_type = target_type
        self.label = label

    def __call__(self, data):
        """Perform the transform.
        Args:
            data (pd.DataFrame):
                Batch of data produced by the NodePiece Loader.
        """
        import torch
        batch = {}
        if self.target_type:
            data = data[self.target_type]
        batch["y"] = torch.tensor(data[self.label].astype(int))
        batch["relational_context"] = torch.tensor(
            data["relational_context"], dtype=torch.long)
        batch["anchors"] = torch.tensor(data["anchors"], dtype=torch.long)
        batch["distance"] = torch.tensor(
            data["anchor_distances"], dtype=torch.long)
        if len(self.features) > 0:
            batch["features"] = torch.stack(
                [torch.tensor(data[feat]) for feat in self.features]).T
        return batch
