from .dataloaders import BaseLoader
from .metrics import BaseMetrics
from typing import Union, List, Callable
import logging
import time
import os

class BaseCallback():
    def __init__(self):
        pass

    def on_init_end(self, trainer):
        pass

    def on_epoch_start(self, trainer):
        pass

    def on_train_step_start(self, trainer):
        pass

    def on_train_step_end(self, trainer):
        pass

    def on_epoch_end(self, trainer):
        pass

    def on_eval_start(self, trainer):
        pass

    def on_eval_step_start(self, trainer):
        pass

    def on_eval_step_end(self, trainer):
        pass

    def on_eval_end(self, trainer):
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
        os.makedirs(self.output_dir, exist_ok=True)
        curDT = time.time()
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                            filename=output_dir+'/train_results_'+str(curDT)+'.log',
                            filemode='w',
                            encoding='utf-8',
                            level=logging.INFO)

    def on_train_step_end(self, trainer):
        logger = logging.getLogger(__name__)
        logger.info("train_step:"+str(trainer.train_step_metrics))

    def on_eval_end(self, trainer):
        logger = logging.getLogger(__name__)
        logger.info("evaluation:"+str(trainer.eval_global_metrics))

    def on_epoch_end(self, trainer):
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

        for callback in callbacks: # instantiate callbacks if not already done so
            if isinstance(callback, type):
                callback = callback()
                self.callbacks.append(callback)
            else:
                self.callbacks.append(callback)

        for callback in self.callbacks:
            callback.on_init_end(trainer=self)


    def train(self, num_epochs=None, max_num_steps=None):
        if num_epochs:
            max_num_steps = self.train_loader.num_batches * num_epochs
        cur_step = 0
        while cur_step < max_num_steps:
            for callback in self.callbacks:
                callback.on_epoch_start(trainer=self)
            for batch in self.train_loader:
                if cur_step >= max_num_steps:
                    break
                for callback in self.callbacks:
                    callback.on_train_step_start(trainer=self)
                out = self.model(batch, tgt_type=self.target_type)
                loss = self.model.compute_loss(out,
                                               batch,
                                               target_type = self.target_type,
                                               loss_fn = self.loss_fn)
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                self.train_step_metrics = {}
                for metric in self.metrics:
                    metric.update_metrics(loss, out, batch, target_type=self.target_type)
                    self.train_step_metrics.update(metric.get_metrics())
                    self.train_step_metrics["global_step"] = cur_step
                    self.train_step_metrics["epoch"] = int(cur_step/self.train_loader.num_batches)
                    metric.reset_metrics()
                cur_step += 1
                for callback in self.callbacks:
                    callback.on_train_step_end(trainer=self)

            for callback in self.callbacks:
                callback.on_epoch_end(trainer=self)             

    def eval(self):
        self.model.eval()
        for callback in self.callbacks:
            callback.on_eval_start(trainer=self)
        for batch in self.eval_loader:
            for callback in self.callbacks:
                callback.on_eval_step_start(trainer=self)
            out = self.model(batch, tgt_type=self.target_type)
            loss = self.model.compute_loss(out,
                                        batch,
                                        target_type = self.target_type,
                                        loss_fn = self.loss_fn)
            for metric in self.metrics:
                metric.update_metrics(loss, out, batch, target_type=self.target_type)
            for callback in self.callbacks:
                callback.on_eval_step_end(trainer=self)
        self.eval_global_metrics = {}
        for metric in self.metrics:
            self.eval_global_metrics.update(metric.get_metrics())
        for callback in self.callbacks:
            callback.on_eval_end(trainer=self)
        if self.metrics:
            for metric in self.metrics:
                metric.reset_metrics()
        self.model.train()