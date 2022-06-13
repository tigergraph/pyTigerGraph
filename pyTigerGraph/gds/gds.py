"""Factory Functions
Factory Functions are a special collection of functions that return an instance of a class.

All factory functions are methods of the `GDS` class. 
You can call a factory function after instantiating a TigerGraph Connection. 
For example:

[,python]
----
conn = TigerGraphConnection(
    host="http://127.0.0.1", 
    graphname="Cora",
    username="tigergraph",
    password="tigergraph",
    useCert=False
)
edge_loader = conn.gds.edgeLoader(
    num_batches=1,
    attributes=["time", "is_train"])
----

The object returned has access to instance methods of the class. 
You can find the reference for those classes on the following pages:

* link:https://docs.tigergraph.com/pytigergraph/current/gds/dataloaders[Data loaders]
* link:https://docs.tigergraph.com/pytigergraph/current/gds/featurizer[Featurizer]
* link:https://docs.tigergraph.com/pytigergraph/current/gds/metrics[Metrics]
* link:https://docs.tigergraph.com/pytigergraph/current/gds/splitters[Splitters]
"""
from typing import TYPE_CHECKING, Union, List

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

from .dataloaders import (EdgeLoader, EdgeNeighborLoader, GraphLoader,
                          NeighborLoader, VertexLoader)
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
        """Returns a `NeighborLoader` instance.
        A `NeighborLoader` instance performs neighbor sampling from vertices in the graph in batches in the following manner:

        . It chooses a specified number (`batch_size`) of vertices as seeds. 
        The number of batches is the total number of vertices divided by the batch size. 
        * If you specify the number of batches (`num_batches`) instead, `batch_size` is calculated by dividing the total number of vertices by the number of batches.
        If specify both parameters, `batch_size` takes priority. 
        . It picks a specified number (`num_neighbors`) of neighbors of each seed at random.
        . It picks the same number of neighbors for each neighbor, and repeats this process until it finished performing a specified number of hops (`num_hops`).
        
        This generates one subgraph. 
        As you loop through this data loader, every vertex will at some point be chosen as a seed and you will get the subgraph
        expanded from the seeds. 
        If you want to limit seeds to certain vertices, the boolean
        attribute provided to `filter_by` will be used to indicate which vertices can be
        included as seeds.
        If you want to load from certain types of vertices and edges, 
        use the `dict` input for `v_in_feats`, `v_out_labels`, `v_extra_feats`,
        `e_in_feats`, `e_out_labels`, `e_extra_feats` where keys of the dict are vertex 
        or edge types to be selected and values are lists of attributes to collect from the
        vertex or edge types. 

        NOTE: When you initialize the loader on a graph for the first time,
        the initialization might take a minute as it installs the corresponding
        query to the database. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same graph again.

        See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_neighborloader.ipynb[the ML Workbench tutorial notebook]
        for examples.

        Args:
            v_in_feats (list or dict, optional):
                Vertex attributes to be used as input features. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type.
                Only numeric and boolean attributes are allowed. The type of an attribute 
                is automatically determined from the database schema. Defaults to None.
            v_out_labels (list or dict, optional):
                Vertex attributes to be used as labels for prediction. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type.
                Only numeric and boolean attributes are allowed. Defaults to None.
            v_extra_feats (list or dict, optional):
                Other attributes to get such as indicators of train/test data. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type. 
                All types of attributes are allowed. Defaults to None.
            e_in_feats (list or dict, optional):
                Edge attributes to be used as input features. 
                If it is a list, then the attributes
                in the list from all edge types will be selected. An error will be thrown if
                certain attribute doesn't exist in all edge types. If it is a dict, keys of the 
                dict are edge types to be selected, and values are lists of attributes to be 
                selected for each edge type.
                Only numeric and boolean attributes are allowed. The type of an attribute
                is automatically determined from the database schema. Defaults to None.
            e_out_labels (list or dict, optional):
                Edge attributes to be used as labels for prediction. 
                If it is a list, then the attributes in the list from all edge types will be 
                selected. An error will be thrown if certain attribute doesn't exist in all 
                edge types. If it is a dict, keys of the dict are edge types to be selected, 
                and values are lists of attributes to be selected for each edge type.
                Only numeric and boolean attributes are allowed. Defaults to None.
            e_extra_feats (list or dict, optional):
                Other edge attributes to get such as indicators of train/test data. 
                If it is a list, then the attributes in the list from all edge types will be 
                selected. An error will be thrown if certain attribute doesn't exist in all 
                edge types. If it is a dict, keys of the dict are edge types to be selected, 
                and values are lists of attributes to be selected for each edge type.
                All types of attributes are allowed. Defaults to None.
            batch_size (int, optional):
                Number of vertices as seeds in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the vertices into as seeds.
                If both `batch_size` and `num_batches` are provided, `batch_size` takes higher
                priority. Defaults to 1.
            num_neighbors (int, optional):
                Number of neighbors to sample for each vertex.
                Defaults to 10.
            num_hops (int, optional):
                Number of hops to traverse when sampling neighbors.
                Defaults to 2.
            shuffle (bool, optional):
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which vertices
                can be included as seeds. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "PyG", "DGL" and "dataframe" are supported. Defaults to "PyG".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
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
        """Returns an `EdgeLoader` instance. 
        An `EdgeLoader` instance loads all edges in the graph in batches.

        It divides all edges into `num_batches` and returns each batch separately.
        You can also specify the size of each batch, and the number of batches is calculated accordingly. 
        If you provide both parameters, `batch_size` take priority. 
        The boolean attribute provided to `filter_by` indicates which edges are included.
        If you want to load from certain types of edges, 
        use the `dict` input for `attributes` where keys of the dict are edge types to be 
        selected and values are lists of attributes to collect from the edge types. 
        If you need random batches, set `shuffle` to True.

        NOTE: When you initialize the loader on a graph for the first time,
        the initialization might take a minute as it installs the corresponding
        query to the database. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same graph again.

        There are two ways to use the data loader.

        * It can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all edges at once (`num_batches=1`),
          there will be only one batch (of all the edges) in the iterator.
        * You can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator. If there are
          multiple batches of data to load, it returns the loader itself.

        Args:
            attributes (list or dict, optional):
                Edge attributes to be included. If it is a list, then the attributes
                in the list from all edge types will be selected. An error will be thrown if
                certain attribute doesn't exist in all edge types. If it is a dict, keys of the 
                dict are edge types to be selected, and values are lists of attributes to be 
                selected for each edge type. Defaults to None.
            batch_size (int, optional):
                Number of edges in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the edges.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the edges before loading data.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which edges are included. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "dataframe" is supported. Defaults to "dataframe".
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.

        See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_edgeloader.ipynb[the ML Workbench edge loader tutorial notebook]
        for examples.
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
        """Returns a `VertexLoader` instance.
        A `VertexLoader` can load all vertices of a graph in batches.

        It divides vertices into `num_batches` and returns each batch separately.
        The boolean attribute provided to `filter_by` indicates which vertices are included.
        If you want to load from certain types of vertices, 
        use the `dict` input for `attributes` where keys of the dict are vertex 
        types to be selected and values are lists of attributes to collect from the
        vertex types. 
        If you need random batches, set `shuffle` to True.

        NOTE: When you initialize the loader on a graph for the first time,
        the initialization might take a minute as it installs the corresponding
        query to the database. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same graph again.

        There are two ways to use the data loader:

        * It can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all vertices at once (`num_batches=1`),
          there will be only one batch (of all the vertices) in the iterator.
        * You can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader again.

        Args:
            attributes (list or dict, optional):
                Vertex attributes to be included. If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type. Defaults to None.
            batch_size (int, optional):
                Number of vertices in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the vertices.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which vertices
                can be included. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "dataframe" is supported. Defaults to "dataframe".
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this loader.
                Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.

        See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_vertexloader.ipynb[the ML Workbench tutorial notebook]
        for examples.
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
        """Returns a `GraphLoader`instance.
        A `GraphLoader` instance loads all edges from the graph in batches, along with the vertices that are connected with each edge.

        Different from NeighborLoader which produces connected subgraphs, this loader
        generates (random) batches of edges and vertices attached to those edges.

        If you want to load from certain types of vertices and edges, 
        use the `dict` input for `v_in_feats`, `v_out_labels`, `v_extra_feats`,
        `e_in_feats`, `e_out_labels`, `e_extra_feats` where keys of the dict are vertex 
        or edge types to be selected and values are lists of attributes to collect from the
        vertex or edge types. 

        NOTE: When you initialize the loader on a graph for the first time,
        the initialization might take a minute as it installs the corresponding
        query to the database. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same graph again.

        There are two ways to use the data loader:

        * It can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all data at once (`num_batches=1`),
          there will be only one batch (of all the data) in the iterator.
        * You can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader itself.

        Args:
            v_in_feats (list or dict, optional):
                Vertex attributes to be used as input features. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type.
                Only numeric and boolean attributes are allowed. The type of an attribute
                is automatically determined from the database schema. Defaults to None.
            v_out_labels (list or dict, optional):
                Vertex attributes to be used as labels for prediction. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type.
                Only numeric and boolean attributes are allowed. Defaults to None.
            v_extra_feats (list or dict, optional):
                Other attributes to get such as indicators of train/test data.
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type. 
                All types of attributes are allowed. Defaults to None.
            e_in_feats (list or dict, optional):
                Edge attributes to be used as input features. 
                If it is a list, then the attributes
                in the list from all edge types will be selected. An error will be thrown if
                certain attribute doesn't exist in all edge types. If it is a dict, keys of the 
                dict are edge types to be selected, and values are lists of attributes to be 
                selected for each edge type.
                Only numeric and boolean attributes are allowed. The type of an attribute
                is automatically determined from the database schema. Defaults to None.
            e_out_labels (list or dict, optional):
                Edge attributes to be used as labels for prediction. 
                If it is a list, then the attributes in the list from all edge types will be 
                selected. An error will be thrown if certain attribute doesn't exist in all 
                edge types. If it is a dict, keys of the dict are edge types to be selected, 
                and values are lists of attributes to be selected for each edge type.
                Only numeric and boolean attributes are allowed. Defaults to None.
            e_extra_feats (list or dict, optional):
                Other edge attributes to get such as indicators of train/test data. 
                If it is a list, then the attributes in the list from all edge types will be 
                selected. An error will be thrown if certain attribute doesn't exist in all 
                edge types. If it is a dict, keys of the dict are edge types to be selected, 
                and values are lists of attributes to be selected for each edge type.
                All types of attributes are allowed. Defaults to None.
            batch_size (int, optional):
                Number of edges in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the edges.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the data before loading.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which edges can be included.
                Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader.
                Only "PyG", "DGL" and "dataframe" are supported. Defaults to "dataframe".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.

        See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_graphloader.ipynb[the ML Workbench tutorial notebook for graph loaders]
         for examples.
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

    def edgeNeighborLoader(
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
    ) -> EdgeNeighborLoader:
        """Returns an `EdgeNeighborLoader` instance.
        An `EdgeNeighborLoader` instance performs neighbor sampling from all edges in the graph in batches in the following manner:

        . It chooses a specified number (`batch_size`) of edges as seeds. 
        The number of batches is the total number of edges divided by the batch size. 
        * If you specify the number of batches (`num_batches`) instead, `batch_size` is calculated by dividing the total number of vertices by the number of batches.
        If specify both parameters, `batch_size` takes priority. 
        . Starting from the vertices attached to the seed edges, it picks a specified number (`num_neighbors`) of neighbors of each vertex at random.
        . It picks the same number of neighbors for each neighbor, and repeats this process until it finished performing a specified number of hops (`num_hops`).
        
        This generates one subgraph. 
        As you loop through this data loader, every edge will at some point be chosen as a seed and you will get the subgraph
        expanded from the seeds. 
        If you want to limit seeds to certain edges, the boolean
        attribute provided to `filter_by` will be used to indicate which edges can be
        included as seeds.
        If you want to load from certain types of vertices and edges, 
        use the `dict` input for `v_in_feats`, `v_out_labels`, `v_extra_feats`,
        `e_in_feats`, `e_out_labels`, `e_extra_feats` where keys of the dict are vertex 
        or edge types to be selected and values are lists of attributes to collect from the
        vertex or edge types. 

        NOTE: When you initialize the loader on a graph for the first time,
        the initialization might take a minute as it installs the corresponding
        query to the database. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same graph again.

        See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_neighborloader.ipynb[the ML Workbench tutorial notebook]
        for examples.

        Args:
            v_in_feats (list or dict, optional):
                Vertex attributes to be used as input features. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type. 
                Only numeric and boolean attributes are allowed. The type of an attribute 
                is automatically determined from the database schema. Defaults to None.
            v_out_labels (list or dict, optional):
                Vertex attributes to be used as labels for prediction. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type.
                Only numeric and boolean attributes are allowed. Defaults to None.
            v_extra_feats (list or dict, optional):
                Other attributes to get such as indicators of train/test data. 
                If it is a list, then the attributes
                in the list from all vertex types will be selected. An error will be thrown if
                certain attribute doesn't exist in all vertex types. If it is a dict, keys of the 
                dict are vertex types to be selected, and values are lists of attributes to be 
                selected for each vertex type. 
                All types of attributes are allowed. Defaults to None.
            e_in_feats (list or dict, optional):
                Edge attributes to be used as input features. 
                If it is a list, then the attributes
                in the list from all edge types will be selected. An error will be thrown if
                certain attribute doesn't exist in all edge types. If it is a dict, keys of the 
                dict are edge types to be selected, and values are lists of attributes to be 
                selected for each edge type.
                Only numeric and boolean attributes are allowed. The type of an attribute
                is automatically determined from the database schema. Defaults to None.
            e_out_labels (list or dict, optional):
                Edge attributes to be used as labels for prediction. 
                If it is a list, then the attributes in the list from all edge types will be 
                selected. An error will be thrown if certain attribute doesn't exist in all 
                edge types. If it is a dict, keys of the dict are edge types to be selected, 
                and values are lists of attributes to be selected for each edge type.
                Only numeric and boolean attributes are allowed. Defaults to None.
            e_extra_feats (list or dict, optional):
                Other edge attributes to get such as indicators of train/test data. 
                If it is a list, then the attributes in the list from all edge types will be 
                selected. An error will be thrown if certain attribute doesn't exist in all 
                edge types. If it is a dict, keys of the dict are edge types to be selected, 
                and values are lists of attributes to be selected for each edge type.
                All types of attributes are allowed. Defaults to None.
            batch_size (int, optional):
                Number of vertices as seeds in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the vertices into as seeds.
                If both `batch_size` and `num_batches` are provided, `batch_size` takes higher
                priority. Defaults to 1.
            num_neighbors (int, optional):
                Number of neighbors to sample for each vertex.
                Defaults to 10.
            num_hops (int, optional):
                Number of hops to traverse when sampling neighbors.
                Defaults to 2.
            shuffle (bool, optional):
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which edges
                can be included as seeds. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "PyG", "DGL" and "dataframe" are supported. Defaults to "PyG".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
        """
        return EdgeNeighborLoader(
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

    def featurizer(self) -> Featurizer:
        """Get a featurizer.
            Returns:
                Featurizer
        """
        return Featurizer(self.conn)

    def vertexSplitter(self, v_types: List[str] = None, timeout: int = 600000, **split_ratios):
        """Get a vertex splitter that splits vertices into at most 3 parts randomly.

        The split results are stored in the provided vertex attributes. Each boolean attribute
        indicates which part a vertex belongs to.

        Usage:

            * A random 60% of vertices will have their attribute `attr_name` set to True, and
            others False. `attr_name` can be any attribute that exists in the database (same below).
            Example:
            [source,python]
            ----
            conn = TigerGraphConnection(...)
            splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6)
            splitter.run()
            ----

            * A random 60% of vertices will have their attribute "attr_name" set to True, and a
            random 20% of vertices will have their attribute "attr_name2" set to True. The two
            parts are disjoint. Example:
            [source,python]
            ----
            conn = TigerGraphConnection(...)
            splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6, attr_name2=0.2)
            splitter.run()
            ----

            * A random 60% of vertices will have their attribute "attr_name" set to True, a
            random 20% of vertices will have their attribute "attr_name2" set to True, and
            another random 20% of vertices will have their attribute "attr_name3" set to True.
            The three parts are disjoint. Example:
            [source,python]
            ----
            conn = TigerGraphConnection(...)
            splitter = RandomVertexSplitter(conn, timeout, attr_name=0.6, attr_name2=0.2, attr_name3=0.2)
            splitter.run()
            ----

        Args:
            v_types (List[str], optional):
                Types of vertex the split will work on. Defaults to None (all types).
            timeout (int, optional):
                Timeout value for the operation. Defaults to 600000.
        """
        return RandomVertexSplitter(self.conn, v_types, timeout, **split_ratios)

    def edgeSplitter(self, e_types: List[str] = None, timeout: int = 600000, **split_ratios):
        """Get an edge splitter that splits edges into at most 3 parts randomly. 

        The split results are stored in the provided edge attributes. Each boolean attribute
        indicates which part an edge belongs to.

        Usage:
            
            * A random 60% of edges will have their attribute "attr_name" set to True, and 
            others False. `attr_name` can be any attribute that exists in the database (same below).
            Example:
            [source,python]
            conn = TigerGraphConnection(...)
            splitter = conn.gds.edgeSplitter(timeout, attr_name=0.6)
            splitter.run()

            * A random 60% of edges will have their attribute "attr_name" set to True, and a 
            random 20% of edges will have their attribute "attr_name2" set to True. The two 
            parts are disjoint. Example:
            [source,python]
            conn = TigerGraphConnection(...)
            splitter = conn.gds.edgeSplitter(timeout, attr_name=0.6, attr_name2=0.2)
            splitter.run()

            * A random 60% of edges will have their attribute "attr_name" set to True, a 
            random 20% of edges will have their attribute "attr_name2" set to True, and 
            another random 20% of edges will have their attribute "attr_name3" set to True. 
            The three parts are disjoint. Example:
            [source,python]
            conn = TigerGraphConnection(...)
            splitter = conn.gds.edgeSplitter(timeout, attr_name=0.6, attr_name2=0.2, attr_name3=0.2)
            splitter.run()

        Args:
            e_types (List[str], optional):
                Types of edges the split will work on. Defaults to None (all types).
            timeout (int, optional): 
                Timeout value for the operation. Defaults to 600000.
        """     
        return RandomEdgeSplitter(self.conn, e_types, timeout, **split_ratios)
