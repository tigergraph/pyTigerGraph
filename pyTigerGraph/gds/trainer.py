"""Model Trainer and Callbacks
:description: Train Graph ML models with pyTigerGraph

Train Graph Machine Learning models (such as GraphSAGE and NodePiece) in a concise way.
pyTigerGraph offers built-in models that can be used with the Trainer, consuming
pyTigerGraph dataloaders.

Callbacks are classes that perform arbitrary operations at various stages of the
training process. Inherit from the `BaseCallback` class to create compatible operations.
"""

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
    ----
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
    """NO DOC"""

    def on_train_step_end(self, trainer):
        """NO DOC"""
        trainer.reset_train_step_metrics()
        for metric in trainer.metrics:
            metric.update_metrics(trainer.loss, trainer.out,
                                  trainer.batch, target_type=trainer.target_type)
            trainer.update_train_step_metrics(metric.get_metrics())
            metric.reset_metrics()
        trainer.update_train_step_metrics({"global_step": trainer.cur_step})
        trainer.update_train_step_metrics(
            {"epoch": int(trainer.cur_step/trainer.train_loader.num_batches)})

    def on_eval_start(self, trainer):
        """NO DOC"""
        for metric in trainer.metrics:
            metric.reset_metrics()

    def on_eval_step_end(self, trainer):
        """NO DOC"""
        for metric in trainer.metrics:
            metric.update_metrics(trainer.loss, trainer.out,
                                  trainer.batch, target_type=trainer.target_type)

    def on_eval_end(self, trainer):
        """NO DOC"""
        for metric in trainer.metrics:
            trainer.update_eval_metrics(metric.get_metrics())


class DefaultCallback(BaseCallback):
    """Default Callback

    The `DefaultCallback` class logs metrics and updates progress bars during the training process.
    The Trainer `callbacks` parameter is populated with this callback.
    If you define other callbacks with that parameter, you will have to pass `DefaultCallback` again in your list of callbacks.
    """

    def __init__(self, output_dir="./logs", use_tqdm=True):
        """Instantiate the Default Callback.

        Args:
            output_dir (str, optional):
                Path to output directory to log metrics to. Defaults to `./logs`
            use_tqdm (bool, optional):
                Whether to use tqdm for progress bars. Defaults to True. 
                Install the `tqdm` package if the progress bar is desired.
        """
        if use_tqdm:
            try:
                from tqdm import tqdm
                self.tqdm = tqdm
                self.epoch_bar = None
                self.batch_bar = None
                self.valid_bar = None
            except:
                self.tqdm = None
                warnings.warn(
                    "tqdm not installed. Please install tqdm if progress bar support is desired.")
        else:
            self.tqdm = False
        self.output_dir = output_dir
        self.best_loss = float("inf")
        os.makedirs(self.output_dir, exist_ok=True)
        curDT = time.time()
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                            filename=output_dir +
                            '/train_results_'+str(curDT)+'.log',
                            filemode='w',
                            encoding='utf-8',
                            level=logging.INFO)

    def on_epoch_start(self, trainer):
        """NO DOC"""
        if self.tqdm:
            if not (self.epoch_bar):
                if trainer.num_epochs:
                    self.epoch_bar = self.tqdm(
                        desc="Epochs", total=trainer.num_epochs)
                else:
                    self.epoch_bar = self.tqdm(
                        desc="Training Steps", total=trainer.max_num_steps)
            if not (self.batch_bar):
                self.batch_bar = self.tqdm(
                    desc="Training Batches", total=trainer.train_loader.num_batches)

    def on_train_step_end(self, trainer):
        """NO DOC"""
        logger = logging.getLogger(__name__)
        logger.info("train_step:"+str(trainer.get_train_step_metrics()))
        if self.tqdm:
            if self.batch_bar:
                self.batch_bar.update(1)

    def on_eval_start(self, trainer):
        """NO DOC"""
        trainer.reset_eval_metrics()
        if self.tqdm:
            if not (self.valid_bar):
                self.valid_bar = self.tqdm(
                    desc="Eval Batches", total=trainer.eval_loader.num_batches)

    def on_eval_step_end(self, trainer):
        """NO DOC"""
        if self.tqdm:
            if self.valid_bar:
                self.valid_bar.update(1)

    def on_eval_end(self, trainer):
        """NO DOC"""
        logger = logging.getLogger(__name__)
        logger.info("evaluation:"+str(trainer.get_eval_metrics()))
        trainer.model.train()
        if self.tqdm:
            if self.valid_bar:
                self.valid_bar.close()
                self.valid_bar = None

    def on_epoch_end(self, trainer):
        """NO DOC"""
        if self.tqdm:
            if self.epoch_bar:
                self.epoch_bar.update(1)
            if self.batch_bar:
                self.batch_bar.close()
                self.batch_bar = None
        trainer.eval()


class Trainer():
    """Trainer

    Train graph machine learning models that comply with the `BaseModel` object in pyTigerGraph.
    Performs training and evaluation loops and automatically collects metrics for the given task.

    PyTorch is required to use the Trainer.
    """

    def __init__(self,
                 model,
                 training_dataloader: BaseLoader,
                 eval_dataloader: BaseLoader,
                 callbacks: List[BaseCallback] = [DefaultCallback],
                 metrics=None,
                 target_type=None,
                 loss_fn=None,
                 optimizer=None,
                 optimizer_kwargs={}):
        """Instantiate a Trainer.

        Create a Trainer object to train graph machine learning models. 

        Args:
            model (pyTigerGraph.gds.models.base_model.BaseModel):
                A graph machine learning model that inherits from the BaseModel class.
            training_dataloader (pyTigerGraph.gds.dataloaders.BaseLoader):
                A pyTigerGraph dataloader to iterate through training batches.
            eval_dataloader (pyTigerGraph.gds.dataloaders.BaseLoader):
                A pyTigerGraph dataloader to iterate through evaluation batches.
            callbacks (List[pyTigerGraph.gds.trainer.BaseCallback], optional):
                A list of `BaseCallback` objects. Defaults to `[DefaultCallback]`
            metrics (List[pyTigerGraph.gds.metrics.BaseMetrics] or pyTigerGraph.gds.metrics.BaseMetrics, optional):
                A list or object of type `BaseMetrics`. If not specified, will use the metrics corresponding to the built-in model.
            target_type (string or tuple, optional):
                If using heterogenous graphs, specify the schema element to compute loss and metrics on.
                If using vertices, specify it with a string. 
                If using an edge type, use the form `("src_vertex_type", "edge_type", "dest_vertex_type")`
            loss_fn (torch.nn._Loss, optional):
                A function that computes the loss of the model. If not specified, the default loss function of the model type will be used.
            optimizer (torch.optim.Optimizer, optional):
                Specify the optimizer to be used during the training process. Defaults to Adam.
            optimizer_kwargs (dict, optional):
                Dictionary of optimizer arguments, such as learning rate. Defaults to optimizer's default values.
        """
        try:
            import torch
        except:
            raise Exception(
                "PyTorch is required to use the trainer. Please install PyTorch.")
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
        try:
            if self.train_loader.v_out_labels:
                if self.is_hetero:
                    self.target_type = list(
                        self.train_loader.v_out_labels.keys())[0]
                else:
                    self.target_type = None  # self.train_loader.v_out_labels
            else:
                self.target_type = target_type
        except:
            self.target_type = None

        callbacks = [MetricsCallback] + callbacks
        for callback in callbacks:  # instantiate callbacks if not already done so
            if isinstance(callback, type):
                callback = callback()
                self.callbacks.append(callback)
            else:
                self.callbacks.append(callback)
        for callback in self.callbacks:
            callback.on_init_end(trainer=self)

    def update_train_step_metrics(self, metrics):
        """Update the metrics for a training step.

        Args:
            metrics (dict):
                Dictionary of calculated metrics.
        """
        self.train_step_metrics.update(metrics)

    def get_train_step_metrics(self):
        """Get the metrics for a training step.

        Returns:
            Dictionary of training metrics results.
        """
        if self.train_step_metrics:
            return self.train_step_metrics
        else:
            return {}

    def reset_train_step_metrics(self):
        """Reset training step metrics.
        """
        self.train_step_metrics = {}

    def update_eval_metrics(self, metrics):
        """Update the metrics of an evaluation loop.

        Args:
            metrics (dict):
                Dictionary of calculated metrics.
        """
        self.eval_metrics.update(metrics)

    def get_eval_metrics(self):
        """Get the metrics for an evaluation loop.

        Returns:
            Dictionary of evaluation loop metrics results.
        """
        if self.eval_metrics:
            return self.eval_metrics
        else:
            return {}

    def reset_eval_metrics(self):
        """Reset evaluation loop metrics.
        """
        self.eval_metrics = {}

    def train(self, num_epochs=None, max_num_steps=None):
        """Train a model.

        Args:
            num_epochs (int, optional):
                Number of epochs to train for. Defaults to 1 full iteration through the `training_dataloader`.
            max_num_steps (int, optional):
                Number of training steps to perform. `num_epochs` takes priority over this parameter.
                Defaults to the length of the `training_dataloader`
        """
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
                self.out = self.model(batch, target_type=self.target_type)
                self.batch = batch
                self.loss = self.model.compute_loss(self.out,
                                                    batch,
                                                    target_type=self.target_type,
                                                    loss_fn=self.loss_fn)
                self.optimizer.zero_grad()
                self.loss.backward()
                self.optimizer.step()
                self.cur_step += 1
                for callback in self.callbacks:
                    callback.on_train_step_end(trainer=self)

            for callback in self.callbacks:
                callback.on_epoch_end(trainer=self)

    def eval(self, loader=None):
        """Evaluate a model.

        Args:
            loader (pyTigerGraph.gds.dataloaders.BaseLoader, optional):
                A dataloader to iterate through. 
                If not defined, defaults to the `eval_dataloader` specified in the Trainer instantiation.
        """
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
            self.out = self.model(batch, target_type=self.target_type)
            self.batch = batch
            self.loss = self.model.compute_loss(self.out,
                                                batch,
                                                target_type=self.target_type,
                                                loss_fn=self.loss_fn)
            for callback in self.callbacks:
                callback.on_eval_step_end(trainer=self)
        for callback in self.callbacks:
            callback.on_eval_end(trainer=self)

    def predict(self, batch):
        """Predict a batch.

        Args:
            batch (any):
                Data object that is compatible with the model being trained.
                Make predictions on the batch passed in.

        Returns:
            Returns a tuple of `(model output, evaluation metrics)`
        """
        self.eval(loader=[batch])
        return self.out, self.get_eval_metrics()
