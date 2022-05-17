# pyTigerGraph

pyTigerGraph is a Python package for connecting to TigerGraph databases. Check out the documentation [here](https://docs.tigergraph.com/pytigergraph/current/intro/)

## Quickstart

### Installing pyTigerGraph
This section walks you through installing pyTigerGraph on your machine.

#### Prerequisites
* Python 3+
* If you wish to use the GDS functionality, install `torch` ahead of time.

#### Install _pyTigerGraph_

To download _pyTigerGraph_, run the following command in the command line or use the appropriate tool of your development environment (anaconda, PyCharm, etc.).:

```sh
pip3 install pyTigerGraph
```

#### Install _pyTigerGraph[gds]_

To utilize the Graph Data Science Functionality, there are a few options:
* To use the GDS functions with **PyTorch Geometric**, install `torch` and `PyTorch Geometric` according to their instructions:

    1) [Install Torch](https://pytorch.org/get-started/locally/)

    2) [Install PyTorch Geometric](https://pytorch-geometric.readthedocs.io/en/latest/notes/installation.html)

    3) Install pyTigerGraph with:
        ```sh
        pip3 install 'pyTigerGraph[gds]'
        ```

* To use the GDS functions with **DGL**, install `torch` and `dgl` according to their instructions:

    1) [Install Torch](https://pytorch.org/get-started/locally/)

    2) [Install DGL](https://www.dgl.ai/pages/start.html)

    3) Install pyTigerGraph with:
        ```sh
        pip3 install 'pyTigerGraph[gds]'
        ```

* To use the GDS functions without needing to produce output in the format supported by PyTorch Geometric or DGL.
This makes the data loaders output *Pandas dataframes*:
```sh
pip3 install 'pyTigerGraph[gds]'
```

Once the package is installed, you can import it like any other Python package:

```py
import pyTigerGraph as tg
```
### Getting Started with Core Functions

[![pyTigerGraph 101](https://img.youtube.com/vi/2BcC3C-qfX4/hqdefault.jpg)](https://www.youtube.com/watch?v=2BcC3C-qfX4)

The video above is a good starting place for learning the core functions of pyTigerGraph. [This Google Colab notebook](https://colab.research.google.com/drive/1JhYcnGVWT51KswcXZzyPzKqCoPP5htcC) is the companion notebook to the video.
