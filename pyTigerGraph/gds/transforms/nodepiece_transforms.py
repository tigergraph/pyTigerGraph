class BaseNodePieceTransform():
    def __call__(self, data):
        return data

    def __repr__(self):
        return f'{self.__class__.__name__}()'

class NodePieceMLPTransform(BaseNodePieceTransform):
    # Assumes numerical types for features and labels. No support for complex datatypes as features.
    def __init__(self, features: list, label: str, target_type: str = None):
        try:
            import torch
        except:
            raise Exception("PyTorch is required to use this transform. Please install PyTorch")
        self.features = features
        self.target_type = target_type
        self.label = label

    def __call__(self, data):
        import torch
        batch = {}
        if self.target_type:
            data = data[self.target_type]
        batch["y"] = torch.tensor(data[self.label].astype(int))
        batch["relational_context"] = torch.tensor(data["relational_context"], dtype=torch.long)
        batch["anchors"] = torch.tensor(data["anchors"], dtype=torch.long)
        batch["distance"] = torch.tensor(data["anchor_distances"], dtype=torch.long)
        batch["features"] = torch.stack([torch.tensor(data[feat]) for feat in self.features]).T
        return batch

        