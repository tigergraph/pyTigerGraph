class BasePyGTransform(torch_geometric.transforms.BaseTransform):
    def __init__(self):
        try:
            import torch_geometric
        except:
            raise("PyTorch Geometric must be installed to use PyG Transforms")
        super().__init__()
    def __call__(self, data):
        pass