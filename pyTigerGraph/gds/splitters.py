"""Data Splitting Functions
This class contains functions for data splitting.
"""

import os.path
from typing import TYPE_CHECKING

from .utilities import install_query_file

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection


class BaseRandomSplitter:
    """Base Random Splitter"""

    def __init__(
        self,
        conn: "TigerGraphConnection",
        query_path,
        timeout: int = 600000,
        **split_ratios
    ) -> None:
        self._validate_args(split_ratios)
        self.split_ratios = split_ratios
        self._graph = conn
        self.query_name = install_query_file(self._graph, query_path)
        self.timeout = timeout
        # TODO: Check if attributes exist in database. If not, raise error or create

    def _validate_args(self, split_ratios) -> None:
        if len(split_ratios) == 0:
            raise ValueError("Need at least one partition ratio in input.")
        if len(split_ratios) > 3:
            raise ValueError("Can take at most 3 partition ratios in input.")
        for v in split_ratios.values():
            if v < 0 or v > 1:
                raise ValueError("All partition ratios have to be between 0 and 1.")
        if sum(split_ratios.values()) > 1:
            raise ValueError("Sum of all partition ratios have to be <=1")

    def run(self, **split_ratios) -> None:
        """Perform the split.

        The split ratios set in initialization can be overridden here. For example,
        `splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6); splitter.run(attr_name=0.3)`
        will use the ratio 0.3 instead of 0.6.

        """
        if split_ratios:
            self._validate_args(split_ratios)
        else:
            split_ratios = self.split_ratios
        payload = {}
        for i, key in enumerate(split_ratios):
            payload["attr{}".format(i + 1)] = key
            payload["ratio{}".format(i + 1)] = split_ratios[key]
        resp = self._graph.runInstalledQuery(
            self.query_name, params=payload, timeout=self.timeout, usePost=True
        )
        return resp


class RandomVertexSplitter(BaseRandomSplitter):
    """Split vertices into at most 3 parts randomly.

    The split results are stored in the provided vertex attributes. Each boolean attribute
    indicates which part a vertex belongs to.

    Usage:
        1)  A random 60% of vertices will have their attribute "attr_name" set to True, and
        others False. `attr_name` can be any attribute that exists in the database (same below).
        Example:
        [source,python]
        conn = TigerGraphConnection(...)
        splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6)
        splitter.run()
       
        2) A random 60% of vertices will have their attribute "attr_name" set to True, and a
        random 20% of vertices will have their attribute "attr_name2" set to True. The two
        parts are disjoint. Example:
        [source,python]
        conn = TigerGraphConnection(...)
        splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6, attr_name2=0.2)
        splitter.run()

        3)  A random 60% of vertices will have their attribute "attr_name" set to True, a
        random 20% of vertices will have their attribute "attr_name2" set to True, and
        another random 20% of vertices will have their attribute "attr_name3" set to True.
        The three parts are disjoint. Example:
        [source,python]
        conn = TigerGraphConnection(...)
        splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6, attr_name2=0.2, attr_name3=0.2)
        splitter.run()
        
    Args:
        conn (TigerGraphConnection):
            Connection to TigerGraph database.
        timeout (int, optional):
            Timeout value for the operation. Defaults to 600000.
    """

    def __init__(
        self, conn: "TigerGraphConnection", timeout: int = 600000, **split_ratios
    ) -> None:
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "splitters",
            "random_vertex_split.gsql",
        )
        super().__init__(conn, query_path, timeout, **split_ratios)

    def run(self, **split_ratios) -> None:
        """Perform the split.

        The split ratios set in initialization can be overridden here. For example,
        `splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6); splitter.run(attr_name=0.3)`
        will use the ratio 0.3 instead of 0.6.

        """
        print("Splitting vertices...")
        resp = super().run(**split_ratios)
        print(resp[0]["Status"])


class RandomEdgeSplitter(BaseRandomSplitter):
    """Split edges into at most 3 parts randomly.

    The split results are stored in the provided edge attributes. Each boolean attribute
    indicates which part an edge belongs to.

    Usage:
        1) A random 60% of edges will have their attribute "attr_name" set to True, and 
        others False. `attr_name` can be any attribute that exists in the database (same below).
        Example:
        [source,python]
        conn = TigerGraphConnection(...)
        splitter = conn.gds.edgeSplitter(timeout, attr_name=0.6)
        splitter.run()

        2) A random 60% of edges will have their attribute "attr_name" set to True, and a 
        random 20% of edges will have their attribute "attr_name2" set to True. The two 
        parts are disjoint. Example:
        [source,python]
        conn = TigerGraphConnection(...)
        splitter = conn.gds.edgeSplitter(timeout, attr_name=0.6, attr_name2=0.2)
        splitter.run()

        3) A random 60% of edges will have their attribute "attr_name" set to True, a 
        random 20% of edges will have their attribute "attr_name2" set to True, and 
        another random 20% of edges will have their attribute "attr_name3" set to True. 
        The three parts are disjoint. Example:
        [source,python]
        conn = TigerGraphConnection(...)
        splitter = conn.gds.edgeSplitter(timeout, attr_name=0.6, attr_name2=0.2, attr_name3=0.2)
        splitter.run()

    Args:
        conn (TigerGraphConnection):
            Connection to TigerGraph database.
        timeout (int, optional):
            Timeout value for the operation. Defaults to 600000.
    """

    def __init__(
        self, conn: "TigerGraphConnection", timeout: int = 600000, **split_ratios
    ) -> None:
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "splitters",
            "random_edge_split.gsql",
        )
        super().__init__(conn, query_path, timeout, **split_ratios)

    def run(self, **split_ratios) -> None:
        """Perform the split.

        The split ratios set in initialization can be overridden here. For example,
        `splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6); splitter.run(attr_name=0.3)`
        will use the ratio 0.3 instead of 0.6.

        """
        print("Splitting edges...")
        resp = super().run(**split_ratios)
        print(resp[0]["Status"])