from .dataloaders import BaseLoader
from .metrics import BaseMetrics
from typing import Union, List, Callable
import logging
import time
import os
import warnings

class BaseCallback():
    """Base class for training callbacks.

    The `BaseCallback` class is an abstract class that all other trainer
    callbacks inherit from. It contains a series of functions that are executed
    during that point in time of the trainer's execution, such as the beginning
    or end of an epoch. Inherit from this class if a custom callback implementation is desired.
    """
    def __init__(self):
        """NO DOC"""
        pass

    def on_init_end(self, trainer):
        """Run operations after the initialization of the trainer.
        
        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_epoch_start(self, trainer):
        """Run operations at the start of a training epoch.
        
        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_train_step_start(self, trainer):
        """Run operations at the start of a training step.

        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_train_step_end(self, trainer):
        """Run operations at the end of a training step.

        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_epoch_end(self, trainer):
        """Run operations at the end of an epoch.
        
        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_eval_start(self, trainer):
        """Run operations at the start of the evaulation process.
        
        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_eval_step_start(self, trainer):
        """Run operations at the start of an evaluation batch.

        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_eval_step_end(self, trainer):
        """Run operations at the end of an evaluation batch.

        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass

    def on_eval_end(self, trainer):
        """Run operations at the end of the evaluation process.
        
        Args:
            trainer (pyTigerGraph Trainer):
                Takes in the trainer in order to perform operations on it.
        """
        pass


class PrinterCallback(BaseCallback):
    """Callback for printing metrics during training.
    
    To use, import the class and pass it to the Trainer's callback argument.

    [.wrap,python]
    ----
    from pyTigerGraph.gds.trainer import Trainer, PrinterCallback

    trainer = Trainer(model, training_dataloader, eval_dataloader, callbacks=[PrinterCallback])
    """
    def __init__(self):
        """NO DOC"""
        pass

    def on_train_step_end(self, trainer):
        """NO DOC"""
        print(trainer.get_train_step_metrics())

    def on_eval_end(self, trainer):
        """NO DOC"""
        print(trainer.get_eval_metrics())

class MetricsCallback(BaseCallback):
    def on_train_step_end(self, trainer):
        trainer.reset_train_step_metrics()
        for metric in trainer.metrics:
            metric.update_metrics(trainer.loss, trainer.out, trainer.batch, target_type=trainer.target_type)
            trainer.update_train_step_metrics(metric.get_metrics())
            metric.reset_metrics()
        trainer.update_train_step_metrics({"global_step": trainer.cur_step})
        trainer.update_train_step_metrics({"epoch": int(trainer.cur_step/trainer.train_loader.num_batches)})
    
    def on_eval_start(self, trainer):
        for metric in trainer.metrics:
            metric.reset_metrics()
    
    def on_eval_step_end(self, trainer):
        for metric in trainer.metrics:
            metric.update_metrics(trainer.loss, trainer.out, trainer.batch, target_type=trainer.target_type)
    
    def on_eval_end(self, trainer):
        for metric in trainer.metrics:
            trainer.update_eval_metrics(metric.get_metrics())

class DefaultCallback(BaseCallback):
    def __init__(self, output_dir="./logs", use_tqdm=True):
        if use_tqdm:
            try:
                from tqdm import tqdm
                self.tqdm = tqdm
                self.epoch_bar = None
                self.batch_bar = None
                self.valid_bar = None
            except:
                self.tqdm = None
                warnings.warn("tqdm not installed. Please install tqdm if progress bar support is desired.")
        else:
            self.tqdm = False
        self.output_dir = output_dir
        self.best_loss = float("inf")
        os.makedirs(self.output_dir, exist_ok=True)
        curDT = time.time()
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                            filename=output_dir+'/train_results_'+str(curDT)+'.log',
                            filemode='w',
                            encoding='utf-8',
                            level=logging.INFO)

    def on_epoch_start(self, trainer):
        if self.tqdm:
            if not(self.epoch_bar):
                if trainer.num_epochs:
                    self.epoch_bar = self.tqdm(desc="Epochs", total=trainer.num_epochs)
                else:
                    self.epoch_bar = self.tqdm(desc="Training Steps", total=trainer.max_num_steps)
            if not(self.batch_bar):
                self.batch_bar = self.tqdm(desc="Training Batches", total=trainer.train_loader.num_batches)

    def on_train_step_end(self, trainer):
        logger = logging.getLogger(__name__)
        logger.info("train_step:"+str(trainer.get_train_step_metrics()))
        if self.tqdm:
            if self.batch_bar:
                self.batch_bar.update(1)

    def on_eval_start(self, trainer):
        trainer.reset_eval_metrics()
        if self.tqdm:
            if not(self.valid_bar):
                self.valid_bar = self.tqdm(desc="Eval Batches", total=trainer.eval_loader.num_batches)

    def on_eval_step_end(self, trainer):
        if self.tqdm:
            if self.valid_bar:
                self.valid_bar.update(1)

    def on_eval_end(self, trainer):
        logger = logging.getLogger(__name__)
        logger.info("evaluation:"+str(trainer.get_eval_metrics()))
        trainer.model.train()
        if self.tqdm:
            if self.valid_bar:
                self.valid_bar.close()
                self.valid_bar = None

    def on_epoch_end(self, trainer):
        if self.tqdm:
            if self.epoch_bar:
                self.epoch_bar.update(1)
            if self.batch_bar:
                self.batch_bar.close()
                self.batch_bar = None
        trainer.eval()


class Trainer():
    def __init__(self, 
                 model,
                 training_dataloader: BaseLoader,
                 eval_dataloader: BaseLoader,
                 callbacks: List[BaseCallback] = [DefaultCallback],
                 metrics = None,
                 target_type = None,
                 loss_fn = None, 
                 optimizer = None,
                 optimizer_kwargs = {}):
        try:
            import torch
        except:
            raise Exception("PyTorch is required to use the trainer. Please install PyTorch.")
        self.model = model
        self.train_loader = training_dataloader
        self.eval_loader = eval_dataloader
        self.loss_fn = loss_fn
        self.callbacks = []
        self.metrics = []
        if metrics:
            if isinstance(metrics, list):
                self.metrics += metrics
            else:
                self.metrics.append(metrics)
        elif self.model.metrics:
            self.metrics.append(self.model.metrics)
        else:
            self.metrics.append(BaseMetrics())
        self.reset_eval_metrics()
        self.reset_train_step_metrics()
        optimizer_kwargs["params"] = self.model.parameters()
        if optimizer:
            self.optimizer = optimizer(**optimizer_kwargs)
        else:
            self.optimizer = torch.optim.Adam(**optimizer_kwargs)
        self.is_hetero = training_dataloader.is_hetero
        if self.train_loader.v_out_labels:
            if self.is_hetero:
                self.target_type = list(self.train_loader.v_out_labels.keys())[0]
            else:
                self.target_type = None #self.train_loader.v_out_labels
        else:
            self.target_type = target_type

        callbacks = [MetricsCallback] + callbacks
        for callback in callbacks: # instantiate callbacks if not already done so
            if isinstance(callback, type):
                callback = callback()
                self.callbacks.append(callback)
            else:
                self.callbacks.append(callback)
        for callback in self.callbacks:
            callback.on_init_end(trainer=self)

    def update_train_step_metrics(self, metrics):
        self.train_step_metrics.update(metrics)

    def get_train_step_metrics(self):
        if self.train_step_metrics:
            return self.train_step_metrics
        else:
            return {}

    def reset_train_step_metrics(self):
        self.train_step_metrics = {}

    def update_eval_metrics(self, metrics):
        self.eval_metrics = metrics

    def get_eval_metrics(self):
        if self.eval_metrics:
            return self.eval_metrics
        else:
            return {}

    def reset_eval_metrics(self):
        self.eval_metrics = {}

    def train(self, num_epochs=None, max_num_steps=None):
        if num_epochs:
            self.max_num_steps = self.train_loader.num_batches * num_epochs
        else:
            self.max_num_steps = max_num_steps
        self.num_epochs = num_epochs
        self.cur_step = 0
        while self.cur_step < self.max_num_steps:
            for callback in self.callbacks:
                callback.on_epoch_start(trainer=self)
            for batch in self.train_loader:
                if self.cur_step >= self.max_num_steps:
                    break
                for callback in self.callbacks:
                    callback.on_train_step_start(trainer=self)
                self.out = self.model(batch, tgt_type=self.target_type)
                self.batch = batch
                self.loss = self.model.compute_loss(self.out,
                                               batch,
                                               target_type = self.target_type,
                                               loss_fn = self.loss_fn)
                self.optimizer.zero_grad()
                self.loss.backward()
                self.optimizer.step()
                self.cur_step += 1
                for callback in self.callbacks:
                    callback.on_train_step_end(trainer=self)

            for callback in self.callbacks:
                callback.on_epoch_end(trainer=self)             

    def eval(self, loader=None):
        if loader:
            eval_loader = loader
        else:
            eval_loader = self.eval_loader
        self.model.eval()
        for callback in self.callbacks:
            callback.on_eval_start(trainer=self)
        for batch in eval_loader:
            for callback in self.callbacks:
                callback.on_eval_step_start(trainer=self)
            self.out = self.model(batch, tgt_type=self.target_type)
            self.batch = batch
            self.loss = self.model.compute_loss(self.out,
                                        batch,
                                        target_type = self.target_type,
                                        loss_fn = self.loss_fn)
            for callback in self.callbacks:
                callback.on_eval_step_end(trainer=self)
        for callback in self.callbacks:
            callback.on_eval_end(trainer=self)

    def predict(self, batch):
        self.eval(loader=[batch])
        return self.out, self.get_eval_metrics()