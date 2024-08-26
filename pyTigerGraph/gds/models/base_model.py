try:
    import torch
except:
    raise Exception(
        "PyTorch required to use built-in models. Please install PyTorch")

from ..trainer import Trainer


class BaseModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.model = None

    def reset_parameters(self):
        self.model.reset_parameters()

    def forward(self, batch, target_type=None, **kwargs):
        raise NotImplementedError("Forward pass not implemented for BaseModel")

    def compute_loss(self, logits, batch, loss_fn=None, **kwargs):
        raise NotImplementedError(
            "Loss computation not implemented in BaseModel")

    def fit(self, training_dataloader, eval_dataloader, number_epochs, target_type=None, trainer_kwargs={}):
        trainer_kwargs.update({"model": self,
                               "training_dataloader": training_dataloader,
                               "eval_dataloader": eval_dataloader,
                               "target_type": target_type})
        self.trainer = Trainer(**trainer_kwargs)
        self.trainer.train(number_epochs)

    def predict(self, batch):
        if self.trainer:
            return self.trainer.predict(batch)
        else:
            raise Exception(
                "Model has not been fit yet. Call model.fit() before model.predict()")
