try:
    import torch
except:
    raise Exception("PyTorch required to use built-in models. Please install PyTorch")

class BaseModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.model = None

    def reset_parameters(self):
        self.model.reset_parameters()

    def forward(self, batch):
        raise NotImplementedError("Forward pass not implemented for BaseModel")

    def compute_loss(self):
        raise NotImplementedError("Loss computation not implemented in BaseModel")

    def compute_metrics(self):
        raise NotImplementedError("Metrics computation not implemented in BaseModel")