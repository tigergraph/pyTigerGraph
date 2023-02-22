from .trainer import Trainer
import warnings

try:
    from . import models
except:
    warnings.warn("Using built-in models requires Pytorch Geometric to be installed.")