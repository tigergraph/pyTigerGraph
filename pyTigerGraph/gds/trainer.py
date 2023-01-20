import metrics
from dataloaders import BaseLoader
from typing import Union, List
from models import BaseModel

class Trainer():
    def __init__(self, 
                 model: BaseModel,
                 training_dataloader: BaseLoader,
                 eval_dataloader: BaseLoader,
                 metrics: Union[List[metrics.Accumulator], metrics.Accumulator]=None, 
                 loss_fn=None, 
                 optimizer=None):
        try:
            import torch
        except:
            raise Exception("PyTorch is required to use the trainer. Please install PyTorch.")
        self.model = model
        self.train_loader = training_dataloader,
        self.eval_loader = eval_dataloader,
        if isinstance(metrics, list):
            self.metrics = metrics
        elif isinstance(metrics, metrics.Accumulator):
            self.metrics = [metrics]
        else:
            raise Exception("Metrics must inherit from the base Accumulator class")
        self.loss_fn = loss_fn,
        self.optimizer = optimizer
        self.is_hetero = training_dataloader.is_hetero
        if self.train_loader.v_out_labels:
            pass


    def train(self, num_epochs=None, num_steps=None):
        for epoch in range(10):
            # Train
            self.model.train()
            epoch_train_loss = metrics.Accumulator()
            epoch_train_acc = metrics.Accuracy()
            # Iterate through the loader to get a stream of subgraphs instead of the whole graph
            for batch in self.train_loader:
                batchsize = batch["Movie"].x.shape[0]
                batch.to(device)
                # Forward pass
                out = self.model(batch.x_dict, batch.edge_index_dict)
                # Calculate loss
                mask = batch["Movie"].is_seed
                loss = self.loss_fn(out["Movie"][mask], batch["Movie"].y[mask])
                # Backward pass
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                epoch_train_loss.update(loss.item() * batchsize, batchsize)
                # Predict on training data
                with torch.no_grad():
                    pred = out["Movie"].argmax(dim=1)
                    epoch_train_acc.update(pred[mask], batch["Movie"].y[mask])
                # Log training status after each batch
                logs["loss"] = epoch_train_loss.mean
                logs["acc"] = epoch_train_acc.value
                train_log.add_scalar("Loss", logs["loss"], global_steps)
                train_log.add_scalar("Accuracy", logs["acc"], global_steps)
                train_log.flush()
                global_steps += 1
            # Evaluate
            self.model.eval()
            epoch_val_loss = Accumulator()
            epoch_val_acc = Accuracy()
            for batch in valid_loader:
                batchsize = batch["Movie"].x.shape[0]
                batch.to(device)
                with torch.no_grad():
                    # Forward pass
                    out = model(batch.x_dict, batch.edge_index_dict)
                    # Calculate loss
                    mask = batch["Movie"].is_seed
                    valid_loss = self.loss_fn(out["Movie"][mask], batch["Movie"].y[mask])
                    epoch_val_loss.update(valid_loss.item() * batchsize, batchsize)
                    # Prediction
                    pred = out["Movie"].argmax(dim=1)
                    epoch_val_acc.update(pred[mask], batch["Movie"].y[mask])
            # Log testing result after each epoch
            logs["val_loss"] = epoch_val_loss.mean
            logs["val_acc"] = epoch_val_acc.value
            print(
                "Epoch {}, Valid Loss {:.4f}, Valid Accuracy {:.4f}".format(
                    epoch, logs["val_loss"], logs["val_acc"]
                )
            )
            valid_log.add_scalar("Loss", logs["val_loss"], global_steps)
            valid_log.add_scalar("Accuracy", logs["val_acc"], global_steps)
            valid_log.flush()