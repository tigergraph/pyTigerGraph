# pyTigerGraph

pyTigerGraph is a Python package for connecting to TigerGraph databases. Check out the documentation [here](https://docs.tigergraph.com/pytigergraph/current/intro/)

## Quickstart

### Installing pyTigerGraph
This section walks you through installing pyTigerGraph on your machine.

#### Prerequisites
* OS version
* Python version
* Package managers

#### Install _pyTigerGraph_

To download _pyTigerGraph_, run the following command in the command line or use the appropriate tool of your development environment (anaconda, PyCharm, etc.).:

```sh
pip3 install pyTigerGraph
```

#### Install _pyTigerGraph[gds]_

To utilize the Graph Data Science functionality, download the GDS package that fits your needs.
Any of the options below allows you to use GDS functions, but certain flavors allow you to output results in formats sup
The options are:

* To install all required packages for full GDS functionality, including PyTorch, PyTorch Geometric, and DGL, run the following command:
+
```sh
pip3 install pyTigerGraph[gds]
```

* To use the GDS functions with the option of producing output in the format supported by the *PyTorch Geometric framework*, run the following command:
```sh
pip3 install pyTigerGraph[gds-pyg]
```

* To use the GDS functions with the option of producing output in the format supported by the *Deep Graph Library* (DGL) framework, run the following command:
```sh
pip3 install pyTigerGraph[gds-dgl]
```
* To use the GDS functions without needing to produce output in the format supported by PyTorch Geometric or DGL.
This makes the data loaders output *Pandas dataframes*:
```sh
pip3 install pyTigerGraph[gds-lite]
```

Once the package is installed, you can import it like any other Python package:

```py
import pyTigerGraph as tg
```
### Getting Started with Core Functions

[![pyTigerGraph 101](https://img.youtube.com/vi/2BcC3C-qfX4/hqdefault.jpg)](https://www.youtube.com/watch?v=2BcC3C-qfX4)

The video above is a good starting place for learning the core functions of pyTigerGraph. [This Google Colab notebook](https://colab.research.google.com/drive/1JhYcnGVWT51KswcXZzyPzKqCoPP5htcC) is the companion notebook to the video.