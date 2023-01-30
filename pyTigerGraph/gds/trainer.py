from .dataloaders import BaseLoader
from typing import Union, List, Callable


class BaseCallback():
    def __init__(self):
        pass

    def on_init_end(self):
        pass

    def on_train_step_end(self):
        pass

    def on_epoch_end(self):
        pass

    def on_eval_step_end(self):
        pass

    def on_eval_end(self):
        pass


class PrinterCallback(BaseCallback):
    def __init__(self):
        pass

    def on_train_step_end(self, trainer):
        print(trainer.train_step_metrics)

    def on_eval_end(self, trainer):
        print(trainer.eval_global_metrics)
        

class DefaultCallback(BaseCallback):
    def __init__(self, output_dir="./logs"):
        self.output_dir = output_dir
        self.best_loss = float("inf")

    def on_train_step_end(self, trainer):
        pass
    def on_eval_end(self, trainer):
        pass


class Trainer():
    def __init__(self, 
                 model,
                 training_dataloader: BaseLoader,
                 eval_dataloader: BaseLoader,
                 callbacks: List[BaseCallback] = [PrinterCallback],
                 metrics = None,
                 learning_rate = 0.001, 
                 weight_decay = 0,
                 target_type = None,
                 loss_fn = None, 
                 optimizer = None):
        try:
            import torch
        except:
            raise Exception("PyTorch is required to use the trainer. Please install PyTorch.")
        self.model = model
        self.train_loader = training_dataloader
        self.eval_loader = eval_dataloader
        self.loss_fn = loss_fn
        self.callbacks = callbacks
        if metrics:
            self.metrics = metrics
        elif self.model.metrics:
            self.metrics = self.model.metrics
        else:
            print("No metrics class defined, only calculating loss")
        if optimizer:
            self.optimizer = optimizer
        else:
            self.optimizer = torch.optim.Adam(
                model.parameters(), lr=learning_rate, weight_decay=weight_decay
            )
        self.is_hetero = training_dataloader.is_hetero
        if self.train_loader.v_out_labels:
            if self.is_hetero:
                self.target_type = list(self.train_loader.v_out_labels.keys())[0]
            else:
                self.target_type = self.train_loader.v_out_labels
        else:
            self.target_type = target_type

        for callback in self.callbacks: # instantiate callbacks if not already done so
            if isinstance(callback, type):
                callback = callback()

        for callbacks in self.callbacks:
            callbacks.on_init_end(trainer=self)


    def train(self, num_epochs=None, max_num_steps=None, valid_freq=None):
        if num_epochs:
            max_num_steps = self.train_loader.num_batches * num_epochs
        if not(valid_freq):
            valid_freq = self.train_loader.num_batches
        cur_step = 0
        while cur_step < max_num_steps:
            for batch in self.train_loader:
                if cur_step >= max_num_steps:
                    break
                out = self.model(batch)
                loss = self.model.compute_loss(out,
                                               batch,
                                               self.target_type,
                                               loss_fn = self.loss_fn)
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                if self.metrics:
                    self.metrics.update_metrics(loss, out, batch)
                    self.train_step_metrics = self.metrics.get_metrics()
                    self.train_step_metrics["global_step"] = cur_step
                    self.train_step_metrics["epoch"] = cur_step/self.train_loader.num_batches
                    self.metrics.reset_metrics()
                else:
                    self.train_step_metrics = {}
                cur_step += 1
                for callback in self.callbacks:
                    callback.on_train_step_end(self)
                if self.eval_loader:
                    if cur_step % valid_freq == 0:
                        self.eval()

            for callback in self.callbacks:
                callback.on_epoch_end(self)             

    def eval(self):
        self.model.eval()
        for batch in self.eval_loader:
            out = self.model(batch)
            loss = self.model.compute_loss(out,
                                        batch,
                                        self.target_type,
                                        loss_fn = self.loss_fn)
            if self.metrics:
                self.metrics.update_metrics(loss, out, batch)
            for callback in self.callbacks:
                callback.on_eval_step_end(self)
        if self.metrics:
            self.eval_global_metrics = self.metrics.get_metrics()
        else:
            self.eval_global_metrics = {}
        for callback in self.callbacks:
            callback.on_eval_end(self)
        self.metrics.reset_metrics()
        self.model.train()