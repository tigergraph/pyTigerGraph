"""Graph Data Science Functions
The Graph Data Science Functions are a collection of functions that are used to perform various graph data science and graph machine learning tasks.
In order to use these functions, confirm that the appropriate prerequisites are installed. Check the docs for more details.
"""
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

from .dataloaders import EdgeLoader, GraphLoader, NeighborLoader, VertexLoader
from .featurizer import Featurizer
from .splitters import RandomEdgeSplitter, RandomVertexSplitter


class GDS:
    def __init__(self, conn: "TigerGraphConnection") -> None:
        """NO DOC: Initiate a GDS object.
            Args:
                conn (TigerGraphConnection):
                    Accept a TigerGraphConnection to run queries with
            Returns:
                None
        """
        self.conn = conn

    def neighborLoader(
        self,
        v_in_feats: Union[list, dict] = None,
        v_out_labels: Union[list, dict] = None,
        v_extra_feats: Union[list, dict] = None,
        e_in_feats: Union[list, dict] = None,
        e_out_labels: Union[list, dict] = None,
        e_extra_feats: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        num_neighbors: int = 10,
        num_hops: int = 2,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        timeout: int = 300000,
    ) -> NeighborLoader:
        """Get a graph loader that performs neighbor sampling as introduced in the
        Inductive Representation Learning on Large Graphs (https://arxiv.org/abs/1706.02216)
        paper.

        Specifically, the loader first chooses `batch_size` number of vertices as seeds,
        then picks `num_neighbors` number of neighbors of each seed at random,
        then `num_neighbors` neighbors of each neighbor, and repeat for `num_hops`.
        This generates one subgraph. As you loop through this data loader, every
        vertex will at some point be chosen as a seed and you will get the subgraph
        expanded from the seed. If you want to limit seeds to certain vertices, the boolean
        attribute provided to `filter_by` will be used to indicate which vertices can be
        included as seeds.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader. See
        https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all data at once (`num_batches=1`),
          there will be only one batch (of all the data) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader itself.

        See the following documentation for more details about the Neighbor Loader arguments:
        xref:dataloaders.adoc#_neighbor_loader[Neighbor Loader Documentation]
        """
        return NeighborLoader(
            self.conn,
            v_in_feats,
            v_out_labels,
            v_extra_feats,
            e_in_feats,
            e_out_labels,
            e_extra_feats,
            batch_size,
            num_batches,
            num_neighbors,
            num_hops,
            shuffle,
            filter_by,
            output_format,
            add_self_loop,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )

    def edgeLoader(
        self,
        attributes: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "dataframe",
        loader_id: str = None,
        buffer_size: int = 4,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        timeout: int = 300000,
    ) -> EdgeLoader:
        """Get a graph loader that pulls batches of edges from database.
        Edge attributes are not supported.

        Specifically, it divides edges into `num_batches` and returns each batch separately.
        The boolean attribute provided to `filter_by` indicates which edges are included.
        If you need random batches, set `shuffle` to True.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader. See
        https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all edges at once (`num_batches=1`),
          there will be only one batch (of all the edges) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader again.

        See the following documentation for more details about the Edge Loader arguments:
        xref:dataloaders.adoc#_edge_loader[Edge Loader Documentation]
        """
        return EdgeLoader(
            self.conn,
            attributes,
            batch_size,
            num_batches,
            shuffle,
            filter_by,
            output_format,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )

    def vertexLoader(
            self,
            attributes: Union[list, dict] = None,
            batch_size: int = None,
            num_batches: int = 1,
            shuffle: bool = False,
            filter_by: str = None,
            output_format: str = "dataframe",
            loader_id: str = None,
            buffer_size: int = 4,
            kafka_address: str = None,
            kafka_max_msg_size: int = 104857600,
            kafka_num_partitions: int = 1,
            kafka_replica_factor: int = 1,
            kafka_retention_ms: int = 60000,
            kafka_auto_del_topic: bool = True,
            kafka_address_consumer: str = None,
            kafka_address_producer: str = None,
            timeout: int = 300000,
    ) -> VertexLoader:
        """Get a data loader that pulls batches of vertices from database.

        Specifically, it divides vertices into `num_batches` and returns each batch separately.
        The boolean attribute provided to `filter_by` indicates which vertices are included.
        If you need random batches, set `shuffle` to True.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader.
        See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all vertices at once (`num_batches=1`),
          there will be only one batch (of all the vertices) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader again.

        See the following documentation for more details about the Vertex Loader arguments:
        xref:dataloaders.adoc#_vertex_loader[Vertex Loader Documentation]
        """
        return VertexLoader(
            self.conn,
            attributes,
            batch_size,
            num_batches,
            shuffle,
            filter_by,
            output_format,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )

    def graphLoader(
        self,
        v_in_feats: Union[list, dict] = None,
        v_out_labels: Union[list, dict] = None,
        v_extra_feats: Union[list, dict] = None,
        e_in_feats: Union[list, dict] = None,
        e_out_labels: Union[list, dict] = None,
        e_extra_feats: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        timeout: int = 300000,
    ) -> GraphLoader:
        """Get a data loader that pulls batches of vertices and edges from database.

        Different from NeighborLoader which produces connected subgraphs, this loader
        generates (random) batches of edges and vertices attached to those edges.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader. See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all data at once (`num_batches=1`),
          there will be only one batch (of all the data) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader itself.

        See the following documentation for more details about the Graph Loader arguments:
        xref:dataloaders.adoc#_graph_loader[Graph Loader Documentation]
        """
        return GraphLoader(
            self.conn,
            v_in_feats,
            v_out_labels,
            v_extra_feats,
            e_in_feats,
            e_out_labels,
            e_extra_feats,
            batch_size,
            num_batches,
            shuffle,
            filter_by,
            output_format,
            add_self_loop,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )

    def featurizer(self) -> Featurizer:
        """Get a featurizer.
            Returns:
                Featurizer
        """
        return Featurizer(self.conn)

    def vertexSplitter(self, timeout: int = 600000, **split_ratios):
        """Get a vertex splitter that splits vertices into at most 3 parts randomly.

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
            timeout (int, optional):
                Timeout value for the operation. Defaults to 600000.
        """
        return RandomVertexSplitter(self.conn, timeout, **split_ratios)

    def edgeSplitter(self, timeout: int = 600000, **split_ratios):
        """Get an edge splitter that splits edges into at most 3 parts randomly. 

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
            timeout (int, optional): 
                Timeout value for the operation. Defaults to 600000.
        """     
        return RandomEdgeSplitter(self.conn, timeout, **split_ratios)
