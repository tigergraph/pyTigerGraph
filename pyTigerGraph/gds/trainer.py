from .dataloaders import BaseLoader
from typing import Union, List

class TrainerState():
    def __init__(self):
        self.eval_loss = None
        self.eval_logits = None
        self.eval_labels = None
        
        self.train_loss = None
        self.train_logits = None
        self.train_labels = None

    def update_train_state(self, loss, logits, labels):
        self.train_loss = loss
        self.train_logits = logits
        self.train_labels = labels

    def update_eval_state(self, loss, logits, labels):
        self.eval_loss = loss
        self.eval_logits = logits
        self.eval_labels = labels

class Trainer():
    def __init__(self, 
                 model,
                 training_dataloader: BaseLoader,
                 eval_dataloader: BaseLoader,
                 metrics = None, 
                 learning_rate = 0.001, 
                 weight_decay = 0,
                 target_type = None,
                 loss_fn=None, 
                 optimizer=None):
        try:
            import torch
        except:
            raise Exception("PyTorch is required to use the trainer. Please install PyTorch.")
        self.state = TrainerState()
        self.model = model
        self.train_loader = training_dataloader
        self.eval_loader = eval_dataloader
        self.loss_fn = loss_fn
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


    def train(self, num_epochs=None, num_steps=None, valid_freq=None):
        if num_epochs:
            num_steps = self.train_loader.num_batches * num_epochs
        if not(valid_freq):
            valid_freq = self.train_loader.num_batches
        cur_step = 0
        while cur_step < num_steps:
            for batch in self.train_loader:
                out = self.model(batch)
                loss = self.model.compute_loss(out,
                                               batch,
                                               self.target_type,
                                               loss_fn = self.loss_fn)
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                cur_step += 1
            if cur_step % valid_freq == 0:
                self.model.eval()
                for batch in self.eval_loader:
                    out = self.model(batch)
                    loss = self.model.compute_loss(out,
                                                batch,
                                                self.target_type,
                                                loss_fn = self.loss_fn)
                    self.state.update_eval_state(loss, out, )
                self.model.train()