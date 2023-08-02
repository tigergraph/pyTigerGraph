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
from typing import TYPE_CHECKING, Union, List, Callable

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

from .dataloaders import (EdgeLoader, EdgeNeighborLoader, GraphLoader,
                          NeighborLoader, VertexLoader, NodePieceLoader,
                          HGTLoader)
from .featurizer import Featurizer
from .splitters import RandomEdgeSplitter, RandomVertexSplitter
# from ..pyTigerGraph import pyTigerGraphGSQL
from pyTigerGraph.pyTigerGraphGSQL import pyTigerGraphGSQL


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
        self.kafkaConfig = None

    def configureKafka(self,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = None,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        kafka_security_protocol: str = "PLAINTEXT",
        kafka_sasl_mechanism: str = None,
        kafka_sasl_plain_username: str = None,
        kafka_sasl_plain_password: str = None,
        kafka_sasl_kerberos_service_name: str = None,
        kafka_sasl_kerberos_keytab: str = None,
        kafka_sasl_kerberos_principal: str = None,
        kafka_sasl_kerberos_domain_name: str = None,
        kafka_ssl_check_hostname: bool = None,
        kafka_producer_ca_location: str = None,
        kafka_producer_certificate_location: str = None,
        kafka_producer_key_location: str = None,
        kafka_producer_key_password: str = None,
        kafka_consumer_ca_location: str = None,
        kafka_consumer_certificate_location: str = None,
        kafka_consumer_key_location: str = None,
        kafka_consumer_key_password: str = None,
        kafka_skip_produce: bool = False,
        kafka_auto_offset_reset: str = "earliest",
        kafka_del_topic_per_epoch: bool = False,
        kafka_add_topic_per_epoch: bool = False,
        kafka_group_id: str = None,
        kafka_topic: str = None
    ) -> None:
        """Configure the Kafka connection.
        Args:
            kafka_address (str, optional):
                Address of the Kafka broker. Defaults to None.
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
                Whether to delete the Kafka topics created by this loader when
                it is destroyed. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the Kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the Kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_security_protocol (str, optional):
                Security prototol for Kafka. Defaults to None.
            kafka_sasl_mechanism (str, optional):
                Authentication mechanism for Kafka. Defaults to None.
            kafka_sasl_plain_username (str, optional):
                SASL username for Kafka. Defaults to None.
            kafka_sasl_plain_password (str, optional):
                SASL password for Kafka. Defaults to None.
            kafka_sasl_kerberos_service_name (str, optional):
                Kerberos principal name that Kafka runs as. Defaults to None.
            kafka_sasl_kerberos_keytab (str, optional):
                Path to Kerberos keytab file. Defaults to None.
            kafka_sasl_kerberos_principal (str, optional):
                This client's Kerberos principal name. Defaults to None.
            kafka_sasl_kerberos_domain_name (str, optional):
                Kerberos domain name to use in GSSAPI mechanism handshake. Defaults to None.
            kafka_ssl_check_hostname (bool, optional): 
                Whether SSL handshake should verify that the certificate matches 
                the brokers hostname. Defaults to None,
            kafka_producer_ca_location (str, optional):
                Path to CA certificate on TigerGraph DB server for verifying the broker. 
            kafka_producer_certificate_location (str, optional):
                Path to client's certificate (PEM) on TigerGraph DB server used for authentication.
            kafka_producer_key_location (str, optional):
                Path to client's private key (PEM) on TigerGraph DB server used for authentication.
            kafka_producer_key_password (str, optional):
                Private key passphrase for use with `kafka_producer_key_location`.
            kafka_consumer_ca_location (str, optional):
                Path to CA certificate on client machine for verifying the broker. 
            kafka_consumer_certificate_location (str, optional):
                Path to client's certificate (PEM) used for authentication.
            kafka_consumer_key_location (str, optional):
                Path to client's private key (PEM) used for authentication.
            kafka_consumer_key_password (str, optional):
                Private key passphrase for use with `kafka_consumer_key_password`.
            kafka_skip_produce (bool, optional):
                Whether or not to skip calling the producer. Defaults to False.
            kafka_auto_offset_reset (str, optional):
                Where to start for a new consumer. "earliest" will move to the oldest available message, 
                "latest" will move to the most recent. Any other value will raise the exception.
                Defaults to "earliest".
            kafka_del_topic_per_epoch (bool, optional): 
                Whether to delete the topic after each epoch. It is effective only when
                `kafka_add_topic_per_epoch` is True. Defaults to False.
            kafka_add_topic_per_epoch (bool, optional):  
                Whether to add a topic for each epoch. Defaults to False.
            kafka_group_id (str, optional):
                Consumer group ID if joining a consumer group for dynamic partition assignment and offset commits. Defaults to None.
            kafka_topic (str, optional):
                Name of the Kafka topic to stream data with.
        """
        self.kafkaConfig = {
            "kafka_address": kafka_address,
            "kafka_max_msg_size": kafka_max_msg_size,
            "kafka_num_partitions": kafka_num_partitions,
            "kafka_replica_factor": kafka_replica_factor,
            "kafka_retention_ms": kafka_retention_ms,
            "kafka_auto_del_topic": kafka_auto_del_topic,
            "kafka_address_consumer": kafka_address_consumer,
            "kafka_address_producer": kafka_address_producer,
            "kafka_security_protocol": kafka_security_protocol,
            "kafka_sasl_mechanism": kafka_sasl_mechanism,
            "kafka_sasl_plain_username": kafka_sasl_plain_username,
            "kafka_sasl_plain_password": kafka_sasl_plain_password,
            "kafka_sasl_kerberos_service_name": kafka_sasl_kerberos_service_name,
            "kafka_sasl_kerberos_keytab": kafka_sasl_kerberos_keytab,
            "kafka_sasl_kerberos_principal": kafka_sasl_kerberos_principal,
            "kafka_sasl_kerberos_domain_name": kafka_sasl_kerberos_domain_name,
            "kafka_ssl_check_hostname": kafka_ssl_check_hostname,
            "kafka_producer_ca_location": kafka_producer_ca_location,
            "kafka_producer_certificate_location": kafka_producer_certificate_location,
            "kafka_producer_key_location": kafka_producer_key_location,
            "kafka_producer_key_password": kafka_producer_key_password,
            "kafka_consumer_ca_location": kafka_consumer_ca_location,
            "kafka_consumer_certificate_location": kafka_consumer_certificate_location,
            "kafka_consumer_key_location": kafka_consumer_key_location,
            "kafka_consumer_key_password": kafka_consumer_key_password,
            "kafka_skip_produce": kafka_skip_produce,
            "kafka_auto_offset_reset": kafka_auto_offset_reset,
            "kafka_del_topic_per_epoch": kafka_del_topic_per_epoch,
            "kafka_add_topic_per_epoch": kafka_add_topic_per_epoch,
            "kafka_group_id": kafka_group_id,
            "kafka_topic": kafka_topic
        }

    def neighborLoader(
        self,
        v_in_feats: Union[list, dict] = None,
        v_out_labels: Union[list, dict] = None,
        v_extra_feats: Union[list, dict] = None,
        v_seed_types: Union[str, list] = None,
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
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        callback_fn: Callable = None,
        reinstall_query: bool = False,
        distributed_query: bool = False,
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

        See https://github.com/tigergraph/graph-ml-notebooks/blob/main/GNNs/PyG/gcn_node_classification.ipynb[the ML Workbench tutorial notebook]
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
            v_seed_types (str or list, optional):
                Directly specify the vertex types to use as seeds. If not specified, defaults to
                the vertex types used in filter_by. If not specified there, uses all vertex types.
                Defaults to None.
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
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
            filter_by (str, dict, list, optional):
                Denotes the name of a boolean attribute used to indicate which vertices
                can be included as seeds. If a dictionary is provided, must be in the form of: 
                {"vertex_type": "attribute"}. If a list, must contain multiple filters and an 
                unique loader will be returned for each list element. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "PyG", "DGL", "spektral", and "dataframe" are supported. Defaults to "PyG".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to "|".
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name if Kafka topic is not given. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            reinstall_query (bool, optional):
                Whether to reinstall the queries associated with this loader at instantiation. One can also call the member function
                `reinstall_query()` on a loader instance to reinstall the queries at any time. 
                Defaults to False.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.
        """
        if isinstance(filter_by, list):
            loaders = []
            for filter_item in filter_by:
                params = {
                    "graph": self.conn,
                    "v_in_feats": v_in_feats,
                    "v_out_labels": v_out_labels,
                    "v_extra_feats": v_extra_feats,
                    "v_seed_types": v_seed_types,
                    "e_in_feats": e_in_feats,
                    "e_out_labels": e_out_labels,
                    "e_extra_feats": e_extra_feats,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "num_neighbors": num_neighbors,
                    "num_hops": num_hops,
                    "shuffle": shuffle,
                    "filter_by": filter_item,
                    "output_format": output_format,
                    "add_self_loop": add_self_loop,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                }
                if self.kafkaConfig:
                    params.update(self.kafkaConfig)
                loaders.append(NeighborLoader(**params))
            if reinstall_query:
                loaders[0].reinstall_query()
            return loaders
        else:
            params = {
                    "graph": self.conn,
                    "v_in_feats": v_in_feats,
                    "v_out_labels": v_out_labels,
                    "v_extra_feats": v_extra_feats,
                    "v_seed_types": v_seed_types,
                    "e_in_feats": e_in_feats,
                    "e_out_labels": e_out_labels,
                    "e_extra_feats": e_extra_feats,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "num_neighbors": num_neighbors,
                    "num_hops": num_hops,
                    "shuffle": shuffle,
                    "filter_by": filter_by,
                    "output_format": output_format,
                    "add_self_loop": add_self_loop,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                }
            if self.kafkaConfig:
                params.update(self.kafkaConfig)
            loader = NeighborLoader(**params)
            if reinstall_query:
                loader.reinstall_query()
            return loader

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
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        callback_fn: Callable = None,
        reinstall_query: bool = False,
        distributed_query: bool = False
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
                selected for each edge type. Numeric, boolean and string attributes are allowed.
                Defaults to None.
            batch_size (int, optional):
                Number of edges in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the edges.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the edges before loading data.
                Defaults to False.
            filter_by (str, dict, list, optional):
                Denotes the name of a boolean attribute used to indicate which vertices
                can be included as seeds. If a dictionary is provided, must be in the form of: 
                {"vertex_type": "attribute"}. If a list, must contain multiple filters and an 
                unique loader will be returned for each list element. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "dataframe" is supported. Defaults to "dataframe".
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name if Kafka topic is not given. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to "|".
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            reinstall_query (bool, optional):
                Whether to reinstall the queries associated with this loader at instantiation. One can also call the member function
                `reinstall_query()` on a loader instance to reinstall the queries at any time. 
                Defaults to False.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.

        See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_edgeloader.ipynb[the ML Workbench edge loader tutorial notebook]
        for examples.
        """
        if isinstance(filter_by, list):
            loaders = []
            for filter_item in filter_by:
                params = {
                    "graph": self.conn,
                    "attributes": attributes,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "shuffle": shuffle,
                    "filter_by": filter_item,
                    "output_format": output_format,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                }
                if self.kafkaConfig:
                    params.update(self.kafkaConfig)
                loaders.append(EdgeLoader(**params))
            if reinstall_query:
                loaders[0].reinstall_query()
            return loaders
        else:
            params = {
                "graph": self.conn,
                "attributes": attributes,
                "batch_size": batch_size,
                "num_batches": num_batches,
                "shuffle": shuffle,
                "filter_by": filter_by,
                "output_format": output_format,
                "loader_id": loader_id,
                "buffer_size": buffer_size,
                "reverse_edge": reverse_edge,
                "delimiter": delimiter,
                "timeout": timeout,
                "callback_fn": callback_fn,
                "distributed_query": distributed_query
            }
            if self.kafkaConfig:
                params.update(self.kafkaConfig)
            loader = EdgeLoader(**params)
            if reinstall_query:
                loader.reinstall_query()
            return loader

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
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        callback_fn: Callable = None,
        reinstall_query: bool = False,
        distributed_query: bool = False
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
                selected for each vertex type. Numeric, boolean and string attributes are allowed.
                Defaults to None.
            batch_size (int, optional):
                Number of vertices in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the vertices.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, dict, list, optional):
                Denotes the name of a boolean attribute used to indicate which vertices
                can be included as seeds. If a dictionary is provided, must be in the form of: 
                {"vertex_type": "attribute"}. If a list, must contain multiple filters and an 
                unique loader will be returned for each list element. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "dataframe" is supported. Defaults to "dataframe".
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to "|".
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            reinstall_query (bool, optional):
                Whether to reinstall the queries associated with this loader at instantiation. One can also call the member function
                `reinstall_query()` on a loader instance to reinstall the queries at any time. 
                Defaults to False.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.

        See https://github.com/tigergraph/graph-ml-notebooks/blob/main/applications/fraud_detection/fraud_detection.ipynb[the ML Workbench tutorial notebook]
        for examples.
        """
        if isinstance(filter_by, list):
            loaders = []
            for filter_item in filter_by:
                params = {
                    "graph": self.conn,
                    "attributes": attributes,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "shuffle": shuffle,
                    "filter_by": filter_item,
                    "output_format": output_format,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                } 
                if self.kafkaConfig:
                    params.update(self.kafkaConfig)
                loaders.append(VertexLoader(**params))
            if reinstall_query:
                loaders[0].reinstall_query()
            return loaders
        else:
            params = {
                "graph": self.conn,
                "attributes": attributes,
                "batch_size": batch_size,
                "num_batches": num_batches,
                "shuffle": shuffle,
                "filter_by": filter_by,
                "output_format": output_format,
                "loader_id": loader_id,
                "buffer_size": buffer_size,
                "reverse_edge": reverse_edge,
                "delimiter": delimiter,
                "timeout": timeout,
                "callback_fn": callback_fn,
                "distributed_query": distributed_query
            }
            if self.kafkaConfig:
                params.update(self.kafkaConfig)
            loader = VertexLoader(**params)
            if reinstall_query:
                loader.reinstall_query()
            return loader

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
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        callback_fn: Callable = None,
        reinstall_query: bool = False,
        distributed_query: bool = False
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
            batch_size (int, optional):
                Number of edges in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the edges.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the data before loading.
                Defaults to False.
            filter_by (str, dict, list, optional):
                Denotes the name of a boolean attribute used to indicate which vertices
                can be included as seeds. If a dictionary is provided, must be in the form of: 
                {"vertex_type": "attribute"}. If a list, must contain multiple filters and an 
                unique loader will be returned for each list element. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader.
                Only "PyG", "DGL", "spektral", and "dataframe" are supported. Defaults to "PyG".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name if Kafka topic is not given. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to "|".
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            reinstall_query (bool, optional):
                Whether to reinstall the queries associated with this loader at instantiation. One can also call the member function
                `reinstall_query()` on a loader instance to reinstall the queries at any time. 
                Defaults to False.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.

        See https://github.com/tigergraph/graph-ml-notebooks/blob/main/GNNs/PyG/gcn_node_classification.ipynb[the ML Workbench tutorial notebook for graph loaders]
         for examples.
        """
        if isinstance(filter_by, list):
            loaders = []
            for filter_item in filter_by:
                params = {
                    "graph": self.conn,
                    "v_in_feats": v_in_feats,
                    "v_out_labels": v_out_labels,
                    "v_extra_feats": v_extra_feats,
                    "e_in_feats": e_in_feats,
                    "e_out_labels": e_out_labels,
                    "e_extra_feats": e_extra_feats,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "shuffle": shuffle,
                    "filter_by": filter_item,
                    "output_format": output_format,
                    "add_self_loop": add_self_loop,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                }
                if self.kafkaConfig:
                    params.update(self.kafkaConfig)
                loaders.append(GraphLoader(**params))
            if reinstall_query:
                loaders[0].reinstall_query()
            return loaders
        else:
            params = {
                "graph": self.conn,
                "v_in_feats": v_in_feats,
                "v_out_labels": v_out_labels,
                "v_extra_feats": v_extra_feats,
                "e_in_feats": e_in_feats,
                "e_out_labels": e_out_labels,
                "e_extra_feats": e_extra_feats,
                "batch_size": batch_size,
                "num_batches": num_batches,
                "shuffle": shuffle,
                "filter_by": filter_by,
                "output_format": output_format,
                "add_self_loop": add_self_loop,
                "loader_id": loader_id,
                "buffer_size": buffer_size,
                "reverse_edge": reverse_edge,
                "delimiter": delimiter,
                "timeout": timeout,
                "callback_fn": callback_fn,
                "distributed_query": distributed_query
            }

            if self.kafkaConfig:
                params.update(self.kafkaConfig)
            loader = GraphLoader(**params)
            if reinstall_query:
                loader.reinstall_query()
            return loader

    def edgeNeighborLoader(
        self,
        v_in_feats: Union[list, dict] = None,
        v_out_labels: Union[list, dict] = None,
        v_extra_feats: Union[list, dict] = None,
        e_in_feats: Union[list, dict] = None,
        e_out_labels: Union[list, dict] = None,
        e_extra_feats: Union[list, dict] = None,
        e_seed_types: Union[str, list] = None,
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
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        callback_fn: Callable = None,
        reinstall_query: bool = False,
        distributed_query: bool = False
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

        See https://github.com/tigergraph/graph-ml-notebooks/blob/main/GNNs/PyG/gcn_link_prediction.ipynb[the ML Workbench tutorial notebook]
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
            e_seed_types (str or list, optional):
                Directly specify the edge types to use as seeds. If not specified, defaults to
                the edge types used in filter_by. If not specified there, uses all edge types.
                Defaults to None.
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
            filter_by (str, dict, list, optional):
                Denotes the name of a boolean attribute used to indicate which vertices
                can be included as seeds. If a dictionary is provided, must be in the form of: 
                {"vertex_type": "attribute"}. If a list, must contain multiple filters and an 
                unique loader will be returned for each list element. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "PyG", "DGL", "Spektral", and "dataframe" are supported. Defaults to "PyG".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name if Kafka topic is not given. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to "|".
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            reinstall_query (bool, optional):
                Whether to reinstall the queries associated with this loader at instantiation. One can also call the member function
                `reinstall_query()` on a loader instance to reinstall the queries at any time. 
                Defaults to False.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.
        """
        if isinstance(filter_by, list):
            loaders = []
            for filter_item in filter_by:
                params = {
                    "graph": self.conn,
                    "v_in_feats": v_in_feats,
                    "v_out_labels": v_out_labels,
                    "v_extra_feats": v_extra_feats,
                    "e_in_feats": e_in_feats,
                    "e_out_labels": e_out_labels,
                    "e_extra_feats": e_extra_feats,
                    "e_seed_types": e_seed_types,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "num_neighbors": num_neighbors,
                    "num_hops": num_hops,
                    "shuffle": shuffle,
                    "filter_by": filter_item,
                    "output_format": output_format,
                    "add_self_loop": add_self_loop,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                }
                if self.kafkaConfig:
                    params.update(self.kafkaConfig)
                loaders.append(EdgeNeighborLoader(**params))
            if reinstall_query:
                loaders[0].reinstall_query()
            return loaders
        else:
            params = {
                "graph": self.conn,
                "v_in_feats": v_in_feats,
                "v_out_labels": v_out_labels,
                "v_extra_feats": v_extra_feats,
                "e_in_feats": e_in_feats,
                "e_out_labels": e_out_labels,
                "e_extra_feats": e_extra_feats,
                "e_seed_types": e_seed_types,
                "batch_size": batch_size,
                "num_batches": num_batches,
                "num_neighbors": num_neighbors,
                "num_hops": num_hops,
                "shuffle": shuffle,
                "filter_by": filter_by,
                "output_format": output_format,
                "add_self_loop": add_self_loop,
                "loader_id": loader_id,
                "buffer_size": buffer_size,
                "reverse_edge": reverse_edge,
                "delimiter": delimiter,
                "timeout": timeout,
                "callback_fn": callback_fn,
                "distributed_query": distributed_query
            }
            if self.kafkaConfig:
                params.update(self.kafkaConfig)
            loader = EdgeNeighborLoader(**params)
            if reinstall_query:
                loader.reinstall_query()
            return loader

    def nodepieceLoader(
        self, 
        v_feats: Union[list, dict] = None,
        target_vertex_types: Union[str, list] = None,
        compute_anchors: bool = False,
        use_cache: bool = False,
        clear_cache: bool = False,
        anchor_method: str = "random",
        anchor_cache_attr: str = "anchors",
        max_distance: int = 5,
        max_anchors: int = 10,
        max_relational_context: int = 10,
        anchor_percentage: float = 0.01,
        anchor_attribute: str = "is_anchor",
        e_types: list = None,
        global_schema_change: bool = False,
        tokenMap: Union[dict, str] = None,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        loader_id: str = None,
        buffer_size: int = 4,
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        callback_fn: Callable = None,
        reinstall_query: bool = False,
        distributed_query: bool = False
    ) -> NodePieceLoader:
        """Returns a `NodePieceLoader` instance.
        A `NodePieceLoader` instance loads all edges from the graph in batches, along with the vertices that are connected with each edge.

        The NodePiece algorithm borrows the idea of "tokenization" from Natural Language Processing. The dataloader offers the functionality
        to "tokenize" the graph in the form of randomly selecting "anchor vertices". If you are running NodePiece for the first time,
        anchors have to be created.

        NOTE: The first time you initialize the loader on a graph, it must first install the corresponding query to the database. 
        However, the query installation only needs to be done once, so you will not need to wait when you initialize the loader on the same graph again.

        There are two ways to use the data loader:

        * It can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all data at once (`num_batches=1`),
          there will be only one batch (of all the data) in the iterator.
        * You can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader itself.

        Args:
            v_feats (list or dict, optional):
                If a heterogenous graph, dictionary of the form {"VERTEX_TYPE": ["vertex_attribute1", ...]}.
                If a homogeneous graph, list of the form ["vertex_attribute1", ...].
                If None, all vertex types will be used, but no vertex attributes will be loaded. 
                If not None, only vertex types specified will be used.
            target_vertex_types (str or list, optional):
                A list or string of vertex types that are going to be used for training the model.
                If None, the vertex types specified in v_feats will be used.
            compute_anchors (bool, optional):
                False by default. If set to True, the dataloader will compute anchors and store them in the attribute
                defined by `anchor_attribute`. 
            use_cache (bool, optional):
                False by default. If True, will cache the result of the anchor search process onto the attribute
                defined by `anchor_cache_attr`. Must define `anchor_cache_attr` if True.
            clear_cache (bool, optional):
                False by default. If True, the cache of the anchor search process will be cleared for the attribute
                defined by `anchor_cache_attr`.
            anchor_method (str, optional):
                "random" by default. Currently, "random" anchor selection strategy is the only strategy supported.
            anchor_cache_attr (str, optional):
                Defines the attribute name to store the cached anchor search results in. By default, the attribute is "anchors".
            max_distance (int, optional):
                The max number of hops away in the graph to search for anchors. Defaults to 5.
            max_anchors (int, optional):
                The max number of anchors used to generate representation of target vertex. Defaults to 10.
            max_relational_context (int, optional):
                The max number of edge types to collect to generate representation of target vertex. Defaults to 10.
            anchor_percentage (float, optional):
                The percentage of vertices to use as anchors. Defaults to 0.01 (1%).
            anchor_attribute (str, optional):
                Attribute to store if a vertex is an anchor. Defaults to "is_anchor".
            e_types (list, optional):
                List of edge types to use in traversing the graph. Defaults to all edge types.
            global_schema_change (bool, optional):
                By default False. Must be True if altering the schema of global namespace graphs.
            tokenMap (dict or str, optional):
                Optional, for use when wanting to transfer the token -> index map from one NodePiece dataloader instance to another.
                Takes in a dictonary of token -> index, or a filepath to a pickle file containing the map. This map can be produced using the
                `saveTokens()` method of the NodePiece loader.
            batch_size (int, optional):
                The batch size to iterate through. Defaults to None.
            num_batches (int, optional):
                The number of batches to produce. Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, dict, list, optional):
                Denotes the name of a boolean attribute used to indicate which vertices
                can be included as seeds. If a dictionary is provided, must be in the form of: 
                {"vertex_type": "attribute"}. If a list, must contain multiple filters and an 
                unique loader will be returned for each list element. Defaults to None.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name if Kafka topic is not given. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to "|".
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            reinstall_query (bool, optional):
                Whether to reinstall the queries associated with this loader at instantiation. One can also call the member function
                `reinstall_query()` on a loader instance to reinstall the queries at any time. 
                Defaults to False.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.

        See https://github.com/tigergraph/graph-ml-notebooks/tree/main/applications/nodepiece/nodepiece.ipynb[the ML Workbench tutorial notebook for nodepiece loaders]
         for examples.
        """
        if isinstance(filter_by, list):
            loaders = []
            for filter_item in filter_by:
                params = {
                    "graph": self.conn,
                    "v_feats": v_feats,
                    "use_cache": use_cache,
                    "clear_cache": clear_cache,
                    "target_vertex_types": target_vertex_types,
                    "compute_anchors": compute_anchors,
                    "anchor_method": anchor_method,
                    "anchor_cache_attr": anchor_cache_attr,
                    "max_distance": max_distance,
                    "max_anchors": max_anchors,
                    "max_relational_context": max_relational_context, 
                    "anchor_percentage": anchor_percentage,
                    "anchor_attribute": anchor_attribute,
                    "e_types": e_types,
                    "tokenMap": tokenMap,
                    "global_schema_change": global_schema_change,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "shuffle": shuffle,
                    "filter_by": filter_item,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                }
                if self.kafkaConfig:
                    params.update(self.kafkaConfig)
                if len(loaders) > 0:
                    params.update({"compute_anchors": False})
                    params.update({"tokenMap": loaders[0].idToIdx})
                loaders.append(NodePieceLoader(**params))
            if reinstall_query:
                loaders[0].reinstall_query()
            return loaders
        else:
            params = {
                "graph": self.conn,
                "v_feats": v_feats,
                "use_cache": use_cache,
                "clear_cache": clear_cache,
                "target_vertex_types": target_vertex_types,
                "compute_anchors": compute_anchors,
                "anchor_method": anchor_method,
                "anchor_cache_attr": anchor_cache_attr,
                "max_distance": max_distance,
                "max_anchors": max_anchors,
                "max_relational_context": max_relational_context, 
                "anchor_percentage": anchor_percentage,
                "anchor_attribute": anchor_attribute,
                "e_types": e_types,
                "tokenMap": tokenMap,
                "global_schema_change": global_schema_change,
                "batch_size": batch_size,
                "num_batches": num_batches,
                "shuffle": shuffle,
                "filter_by": filter_by,
                "loader_id": loader_id,
                "buffer_size": buffer_size,
                "reverse_edge": reverse_edge,
                "delimiter": delimiter,
                "timeout": timeout,
                "callback_fn": callback_fn,
                "distributed_query": distributed_query
            }
            if self.kafkaConfig:
                params.update(self.kafkaConfig)
            loader = NodePieceLoader(**params)
            if reinstall_query:
                loader.reinstall_query()
            return loader

    def hgtLoader(
        self,
        num_neighbors: dict,
        v_in_feats: Union[list, dict] = None,
        v_out_labels: Union[list, dict] = None,
        v_extra_feats: Union[list, dict] = None,
        v_seed_types: Union[str, list] = None,
        e_in_feats: Union[list, dict] = None,
        e_out_labels: Union[list, dict] = None,
        e_extra_feats: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        num_hops: int = 2,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        callback_fn: Callable = None,
        reinstall_query: bool = False,
        distributed_query: bool = False
    ) -> HGTLoader:
        """Returns a `HGTLoader` instance.
        A `HGTLoader` instance performs stratified neighbor sampling from vertices in the graph in batches in the following manner:

        . It chooses a specified number (`batch_size`) of vertices as seeds. 
        The number of batches is the total number of vertices divided by the batch size. 
        * If you specify the number of batches (`num_batches`) instead, `batch_size` is calculated by dividing the total number of vertices by the number of batches.
        If specify both parameters, `batch_size` takes priority. 
        . It picks a specified number of neighbors of each type (as specified by the dict `num_neighbors`) of each seed at random.
        . It picks the specified number of neighbors of every type for each neighbor, and repeats this process until it finished performing a specified number of hops (`num_hops`).
        
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

        Args:
            num_neighbors (dict):
                Number of neighbors of each type to sample. Keys are vertex types and values
                are the number of neighbors to sample for each type.
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
            v_seed_types (str or list, optional):
                Directly specify the vertex types to use as seeds. If not specified, defaults to
                the vertex types used in filter_by. If not specified there, uses all vertex types.
                Defaults to None.
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
                Numeric, boolean and string attributes are allowed. Defaults to None.
            batch_size (int, optional):
                Number of vertices as seeds in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the vertices into as seeds.
                If both `batch_size` and `num_batches` are provided, `batch_size` takes higher
                priority. Defaults to 1.
            num_hops (int, optional):
                Number of hops to traverse when sampling neighbors.
                Defaults to 2.
            shuffle (bool, optional):
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, dict, list, optional):
                Denotes the name of a boolean attribute used to indicate which vertices
                can be included as seeds. If a dictionary is provided, must be in the form of: 
                {"vertex_type": "attribute"}. If a list, must contain multiple filters and an 
                unique loader will be returned for each list element. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "PyG", "DGL", "spektral", and "dataframe" are supported. Defaults to "PyG".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name if Kafka topic is not given. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to "|".
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            reinstall_query (bool, optional):
                Whether to reinstall the queries associated with this loader at instantiation. One can also call the member function
                `reinstall_query()` on a loader instance to reinstall the queries at any time. 
                Defaults to False.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.
        """
        if isinstance(filter_by, list):
            loaders = []
            for filter_item in filter_by:
                params = {
                    "graph": self.conn,
                    "v_in_feats": v_in_feats,
                    "v_out_labels": v_out_labels,
                    "v_extra_feats": v_extra_feats,
                    "v_seed_types": v_seed_types,
                    "e_in_feats": e_in_feats,
                    "e_out_labels": e_out_labels,
                    "e_extra_feats": e_extra_feats,
                    "batch_size": batch_size,
                    "num_batches": num_batches,
                    "num_neighbors": num_neighbors,
                    "num_hops": num_hops,
                    "shuffle": shuffle,
                    "filter_by": filter_item,
                    "output_format": output_format,
                    "add_self_loop": add_self_loop,
                    "loader_id": loader_id,
                    "buffer_size": buffer_size,
                    "reverse_edge": reverse_edge,
                    "delimiter": delimiter,
                    "timeout": timeout,
                    "callback_fn": callback_fn,
                    "distributed_query": distributed_query
                }
                if self.kafkaConfig:
                    params.update(self.kafkaConfig)
                loaders.append(HGTLoader(**params))
            if reinstall_query:
                loaders[0].reinstall_query()
            return loaders
        else:
            params = {
                "graph": self.conn,
                "v_in_feats": v_in_feats,
                "v_out_labels": v_out_labels,
                "v_extra_feats": v_extra_feats,
                "v_seed_types": v_seed_types,
                "e_in_feats": e_in_feats,
                "e_out_labels": e_out_labels,
                "e_extra_feats": e_extra_feats,
                "batch_size": batch_size,
                "num_batches": num_batches,
                "num_neighbors": num_neighbors,
                "num_hops": num_hops,
                "shuffle": shuffle,
                "filter_by": filter_by,
                "output_format": output_format,
                "add_self_loop": add_self_loop,
                "loader_id": loader_id,
                "buffer_size": buffer_size,
                "reverse_edge": reverse_edge,
                "delimiter": delimiter,
                "timeout": timeout,
                "callback_fn": callback_fn,
                "distributed_query": distributed_query
            }
            if self.kafkaConfig:
                params.update(self.kafkaConfig)
            loader = HGTLoader(**params)
            if reinstall_query:
                loader.reinstall_query()
            return loader
    
    def featurizer(
        self,
        repo: str = None, 
        algo_version: str = None) -> Featurizer:
        """Get a featurizer. The Featurizer enables installation and execution of algorithms in the Graph Data Science (GDS) libarary. 
        The Featurizer pulls the most up-to-date version of the algorithm available in our public GitHub repository that is
        compatible with your database version.
        Note: In environments not connected to the public internet, you can download the repository manually and use the featurizer
        like this:
        ```
        import pyTigerGraph as tg
        from pyTigerGraph.gds.featurizer import Featurizer

        conn = tg.TigerGraphConnection(host="HOSTNAME_HERE", username="USERNAME_HERE", password="PASSWORD_HERE", graphname="GRAPHNAME_HERE")
        conn.getToken(conn.createSecret())
        feat = conn.gds.featurizer(repo="PATH/TO/MANUALLY_DOWNLOADED_REPOSITORY")

        res = feat.runAlgorithm("tg_pagerank", params={"v_type": "Paper", "e_type": "CITES"})

        print(res)
        ```
        Returns:
            Featurizer
        """
        return Featurizer(self.conn, repo, algo_version)

    def vertexSplitter(self, v_types: List[str] = None, timeout: int = 600000, **split_ratios):
        """Get a vertex splitter that splits vertices into at most 3 parts randomly.

        The split results are stored in the provided vertex attributes. Each boolean attribute
        indicates which part a vertex belongs to.

        Make sure to create the appropriate attributes in the graph before using these functions.

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

        Make sure to create the appropriate attributes in the graph before using these functions.

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
