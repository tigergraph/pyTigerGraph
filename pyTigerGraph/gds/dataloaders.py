"""Data Loaders
:description: Data loader classes in the pyTigerGraph GDS module. 

Data loaders are classes in the pyTigerGraph Graph Data Science (GDS) module. 
You can define an instance of each data loader class through a link:https://docs.tigergraph.com/pytigergraph/current/gds/factory-functions[factory function].

Requires `querywriters` user permissions for full functionality. 
"""

import io
import logging
import math
import os
from collections import defaultdict
from queue import Empty, Queue
from threading import Event, Thread
from time import sleep
from typing import (TYPE_CHECKING, Any, Iterator, NoReturn, Tuple,
                    Union)

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection
    from kafka import KafkaAdminClient, KafkaConsumer
    import torch
    import dgl
    import torch_geometric as pyg
    from typing import Literal

import numpy as np
import pandas as pd

from ..pyTigerGraphException import TigerGraphException
from .utilities import install_query_file, random_string

__all__ = ["VertexLoader", "EdgeLoader", "NeighborLoader", "GraphLoader", "EdgeNeighborLoader"]
__pdoc__ = {}

_udf_funcs = {
    "UINT": "int_to_string",
    "INT": "int_to_string",
    "BOOL": "bool_to_string",
    "FLOAT": "float_to_string",
    "DOUBLE": "float_to_string",
    "LIST:UINT": "int_to_string",
    "LIST:INT": "int_to_string",
    "LIST:BOOL": "bool_to_string",
    "LIST:FLOAT": "float_to_string",
    "LIST:DOUBLE": "float_to_string",
}


class BaseLoader:
    """NO DOC: Base Dataloader Class."""
    def __init__(
        self,
        graph: "TigerGraphConnection",
        loader_id: str = None,
        num_batches: int = 1,
        buffer_size: int = 4,
        output_format: str = "dataframe",
        reverse_edge: bool = False,
        kafka_address: str = "",
        Kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_consumer_address: str = None,
        kafka_producer_address: str = None,
        kafka_security_protocol: str = "PLAINTEXT",
        kafka_sasl_mechanism: str = None,
        kafka_sasl_plain_username: str = None,
        kafka_sasl_plain_password: str = None,
        kafka_producer_ca_location: str = None,
        kafka_consumer_ca_location: str = None,
        timeout: int = 300000,
    ) -> None:
        """Base Class for data loaders.

        The job of a data loader is to stream data from the TigerGraph database to the client.
        Kafka is used as the data streaming pipeline. Hence, for data streaming to work,
        a running Kafka cluster is required. However, you can also get data in a single HTTP
        response without streaming if you don't provide the Kafka cluster.

        NOTE: When you initialize the loader on a graph for the first time,
        the initialization might take a minute as it installs the corresponding
        query to the database. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same graph again.

        Args:
            graph (TigerGraphConnection):
                Connection to the TigerGraph database.
            loader_iD (str):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string
                will be generated for it. Defaults to None.
            num_batches (int):
                Number of batches to divide the desired data into. Defaults to 1.
            buffer_size (int):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            output_format (str):
                Format of the output data of the loader. Defaults to dataframe.
            reverse_edge (bool, optional):
                Whether to traverse along reverse edge types. Defaults to False.
            kafka_address (str):
                Address of the Kafka broker. Defaults to localhost:9092.
            max_kafka_msg_size (int, optional):
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
            kafka_consumer_address (str, optional):
                Address of the Kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_producer_address (str, optional):
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
            kafka_producer_ca_location (str, optional):
                Path to CA certificate on TigerGraph DB server for verifying the broker's key. 
            kafka_consumer_ca_location (str, optional):
                Path to CA certificate on client machine for verifying the broker's key. 
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
        """
        # Thread to send requests, download and load data
        self._requester = None
        self._downloader = None
        self._reader = None
        # Queues to store tasks and data
        self._request_task_q = None
        self._download_task_q = None
        self._read_task_q = None
        self._data_q = None
        self._kafka_topic = None
        # Exit signal to terminate threads
        self._exit_event = None
        # In-memory data cache. Only used if num_batches=1
        self._data = None
        # Kafka topic configs
        self.kafka_partitions = kafka_num_partitions
        self.kafka_replica = kafka_replica_factor
        self.kafka_retention_ms = kafka_retention_ms
        self.delete_kafka_topic = kafka_auto_del_topic
        # Get graph info
        self.reverse_edge = reverse_edge
        self._graph = graph
        self._v_schema, self._e_schema = self._get_schema()
        # Initialize basic params
        if not loader_id:
            self.loader_id = random_string(6)
        else:
            self.loader_id = loader_id
        self.num_batches = num_batches
        self.output_format = output_format
        self.buffer_size = buffer_size
        self.timeout = timeout
        self._iterations = 0
        self._iterator = False
        # Kafka consumer and admin
        self.max_kafka_msg_size = Kafka_max_msg_size
        self.kafka_address_consumer = (
            kafka_consumer_address if kafka_consumer_address else kafka_address
        )
        self.kafka_address_producer = (
            kafka_producer_address if kafka_producer_address else kafka_address
        )
        if self.kafka_address_consumer:
            try:
                from kafka import KafkaAdminClient, KafkaConsumer
            except ImportError:
                raise ImportError(
                    "kafka-python is not installed. Please install it to use kafka streaming."
                )
            try:
                self._kafka_consumer = KafkaConsumer(
                    bootstrap_servers=self.kafka_address_consumer,
                    client_id=self.loader_id,
                    max_partition_fetch_bytes=Kafka_max_msg_size,
                    fetch_max_bytes=Kafka_max_msg_size,
                    auto_offset_reset="earliest",
                    security_protocol=kafka_security_protocol,
                    sasl_mechanism=kafka_sasl_mechanism,
                    sasl_plain_username=kafka_sasl_plain_username,
                    sasl_plain_password=kafka_sasl_plain_password,
                    ssl_cafile=kafka_consumer_ca_location if kafka_consumer_ca_location else None
                )
                self._kafka_admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_address_consumer,
                    client_id=self.loader_id,
                    security_protocol=kafka_security_protocol,
                    sasl_mechanism=kafka_sasl_mechanism,
                    sasl_plain_username=kafka_sasl_plain_username,
                    sasl_plain_password=kafka_sasl_plain_password,
                    ssl_cafile=kafka_consumer_ca_location if kafka_consumer_ca_location else None
                )
            except:
                raise ConnectionError(
                    "Cannot reach Kafka broker. Please check Kafka settings."
                )
        # Initialize parameters for the query
        self._payload = {}
        if self.kafka_address_producer:
            self._payload["kafka_address"] = self.kafka_address_producer
            if kafka_security_protocol == "PLAINTEXT":
                pass
            elif kafka_security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
                self._payload["security_protocol"] = kafka_security_protocol
                if kafka_sasl_mechanism == "PLAIN":
                    self._payload["sasl_mechanism"] = kafka_sasl_mechanism
                    if kafka_sasl_plain_username and kafka_sasl_plain_password:
                        self._payload["sasl_username"] = kafka_sasl_plain_username
                        self._payload["sasl_password"] = kafka_sasl_plain_password
                    else:
                        raise ValueError("Please provide kafka_sasl_plain_username and kafka_sasl_plain_password for Kafka.")
                    if kafka_producer_ca_location:
                        self._payload["ssl_ca_location"] = kafka_producer_ca_location
                    else:
                        self._payload["ssl_ca_location"] = ""
                else:
                    raise NotImplementedError("Only PLAIN mechanism is supported for SASL.")
            else:
                raise NotImplementedError("Only SASL_PLAINTEXT and SASL_SSL are supported for Kafka authentication.")
            # kafka_topic will be filled in later.
        # Implement `_install_query()` that installs your query
        # self._install_query()

    def __del__(self) -> NoReturn:
        self._reset()

    def _get_schema(self) -> Tuple[dict, dict]:
        v_schema = {}
        e_schema = {}
        schema = self._graph.getSchema(force=True)
        # Get vertex schema
        for vtype in schema["VertexTypes"]:
            v = vtype["Name"]
            v_schema[v] = {}
            for attr in vtype["Attributes"]:
                if attr["AttributeType"]["Name"] == "LIST":
                    v_schema[v][attr["AttributeName"]] = "LIST:" + attr["AttributeType"][
                        "ValueTypeName"
                    ]
                else:
                    v_schema[v][attr["AttributeName"]] = attr["AttributeType"]["Name"]
            if vtype["PrimaryId"].get("PrimaryIdAsAttribute"):
                v_schema[v][vtype["PrimaryId"]["AttributeName"]] = vtype["PrimaryId"][
                    "AttributeType"
                ]["Name"]
        # Get edge schema
        for etype in schema["EdgeTypes"]:
            e = etype["Name"]
            e_schema[e] = {}
            e_schema[e]["FromVertexTypeName"] = etype["FromVertexTypeName"]
            e_schema[e]["ToVertexTypeName"] = etype["ToVertexTypeName"]
            e_schema[e]["IsDirected"] = etype["IsDirected"]
            for attr in etype["Attributes"]:
                if attr["AttributeType"]["Name"] == "LIST":
                    e_schema[e][attr["AttributeName"]] = "LIST:" + attr["AttributeType"][
                        "ValueTypeName"
                    ]
                else:
                    e_schema[e][attr["AttributeName"]] = attr["AttributeType"]["Name"]
            if self.reverse_edge and ("REVERSE_EDGE" in etype["Config"]):
                re = etype["Config"]["REVERSE_EDGE"]
                e_schema[re] = {}
                e_schema[re].update(e_schema[e])
                e_schema[re]["FromVertexTypeName"] = etype["ToVertexTypeName"]
                e_schema[re]["ToVertexTypeName"] = etype["FromVertexTypeName"]
        return v_schema, e_schema

    def _validate_vertex_attributes(
        self, attributes: Union[None, list, dict], is_hetero: bool = False
    ) -> Union[list, dict]:
        return self._validate_attributes(attributes, "vertex", is_hetero)

    def _validate_edge_attributes(
        self, attributes: Union[None, list, dict], is_hetero: bool = False
    ) -> Union[list, dict]:
        return self._validate_attributes(attributes, "edge", is_hetero)

    def _validate_attributes(
        self, 
        attributes: Union[None, list, dict], 
        schema_type: 'Literal["vertex", "edge"]', 
        is_hetero: bool = False
    ) -> Union[list, dict]:
        if schema_type == "vertex":
            schema = self._v_schema
        elif schema_type == "edge":
            schema = self._e_schema
        else:
            raise ValueError("Schema type can only be vertex or edge.")
        if not attributes:
            if is_hetero:
                return {}
            else:
                return []
        if isinstance(attributes, str):
            raise ValueError(
                "The old string way of specifying attributes is deprecated to better support heterogeneous graphs. Please use the new format."
            )
        if isinstance(attributes, list):
            if is_hetero:
                raise ValueError("Input to attributes should be dict or None if you want heterogeneous graph output.")
            for i in range(len(attributes)):
                attributes[i] = attributes[i].strip()
            attr_set = set(attributes)
            for vtype in schema:
                allowlist = set(schema[vtype].keys())
                if attr_set - allowlist:
                    raise ValueError(
                        "Attributes {} are not available for {} type {}.".format(
                            attr_set - allowlist, schema_type, vtype
                        )
                    )
        elif isinstance(attributes, dict):
            if not is_hetero:
                raise ValueError("Input to attributes should be list or None if you want homogeneous graph output.")
            for vtype in attributes:
                if vtype not in schema:
                    raise ValueError(
                        "{} type {} is not available in the database.".format(schema_type, vtype)
                    )
                for i in range(len(attributes[vtype])):
                    attributes[vtype][i] = attributes[vtype][i].strip()
                attr_set = set(attributes[vtype])
                allowlist = set(schema[vtype].keys())
                if attr_set - allowlist:
                    raise ValueError(
                        "Attributes {} are not available for {} type {}.".format(
                            attr_set - allowlist, schema_type, vtype
                        )
                    )
        else:
            raise ValueError("Input to attributes should be None, list, or dict.")
        return attributes

    def _install_query(self) -> NoReturn:
        # Install the right GSQL query for the loader.
        self.query_name = ""
        raise NotImplementedError

    @staticmethod
    def _request_kafka(
        exit_event: Event,
        tgraph: "TigerGraphConnection",
        query_name: str,
        kafka_consumer: "KafkaConsumer",
        kafka_admin: "KafkaAdminClient",
        kafka_topic: str,
        kafka_partitions: int = 1,
        kafka_replica: int = 1,
        kafka_topic_size: int = 100000000,
        kafka_retention_ms: int = 60000,
        timeout: int = 600000,
        payload: dict = {},
        headers: dict = {},
    ) -> NoReturn:
        # Create topic if not exist
        try:
            from kafka.admin import NewTopic
        except ImportError:
            raise ImportError(
                "kafka-python is not installed. Please install it to use kafka streaming."
            )
        if kafka_topic not in kafka_consumer.topics():
            new_topic = NewTopic(
                kafka_topic,
                kafka_partitions,
                kafka_replica,
                topic_configs={
                    "retention.ms": str(kafka_retention_ms),
                    "max.message.bytes": str(kafka_topic_size),
                },
            )
            resp = kafka_admin.create_topics([new_topic])
            if resp.to_object()["topic_errors"][0]["error_code"] != 0:
                raise ConnectionError(
                    "Failed to create Kafka topic {} at {}.".format(
                        kafka_topic, kafka_consumer.config["bootstrap_servers"]
                    )
                )
        # Subscribe to the topic
        kafka_consumer.subscribe([kafka_topic])
        _ = kafka_consumer.topics() # Call this to refresh metadata. Or the new subscription seems to be delayed.
        # Run query async
        # TODO: change to runInstalledQuery when it supports async mode
        _headers = {"GSQL-ASYNC": "true", "GSQL-TIMEOUT": str(timeout)}
        _headers.update(headers)
        _payload = {}
        _payload.update(payload)
        resp = tgraph._post(
            tgraph.restppUrl + "/query/" + tgraph.graphname + "/" + query_name,
            data=_payload,
            headers=_headers,
            resKey=None
        )
        # Check status
        try:
            _stat_payload = {
                "graph_name": tgraph.graphname,
                "requestid": resp["request_id"],
            }
        except KeyError:
            if resp["results"][0]["kafkaError"] != '':
                raise TigerGraphException(
                    "Error writing to Kafka: {}".format(resp["results"][0]["kafkaError"])
                )
            return

        while not exit_event.is_set():
            status = tgraph._get(
                tgraph.restppUrl + "/query_status", params=_stat_payload
            )
            if status[0]["status"] == "running":
                sleep(1)
                continue
            elif status[0]["status"] == "success":
                res = tgraph._get(
                    tgraph.restppUrl + "/query_result", params=_stat_payload
                )
                if res[0]["kafkaError"]:
                    raise TigerGraphException(
                        "Error writing to Kafka: {}".format(res[0]["kafkaError"])
                    )
                else:
                    break
            else:
                raise TigerGraphException(
                    "Error generating data. Query {}.".format(
                        status["results"][0]["status"]
                    )
                )

    @staticmethod
    def _request_rest(
        tgraph: "TigerGraphConnection",
        query_name: str,
        read_task_q: Queue,
        timeout: int = 600000,
        payload: dict = {},
        resp_type: 'Literal["both", "vertex", "edge"]' = "both",
    ) -> NoReturn:
        # Run query
        resp = tgraph.runInstalledQuery(
            query_name, params=payload, timeout=timeout, usePost=True
        )
        # Put raw data into reading queue
        for i in resp:
            if resp_type == "both":
                data = (i["vertex_batch"], i["edge_batch"])
            elif resp_type == "vertex":
                data = i["vertex_batch"]
            elif resp_type == "edge":
                data = i["edge_batch"]
            read_task_q.put(data)
        read_task_q.put(None)

    @staticmethod
    def _download_from_kafka(
        exit_event: Event,
        read_task_q: Queue,
        num_batches: int,
        out_tuple: bool,
        kafka_consumer: "KafkaConsumer",
    ) -> NoReturn:
        delivered_batch = 0
        buffer = {}
        while not exit_event.is_set():
            if delivered_batch == num_batches:
                break
            resp = kafka_consumer.poll(1000)
            if not resp:
                continue
            for msgs in resp.values():
                for message in msgs:
                    key = message.key.decode("utf-8")
                    if out_tuple:
                        if key.startswith("vertex"):
                            companion_key = key.replace("vertex", "edge")
                            if companion_key in buffer:
                                read_task_q.put((message.value.decode("utf-8"), 
                                                 buffer[companion_key]))
                                del buffer[companion_key]
                                delivered_batch += 1
                            else:
                                buffer[key] = message.value.decode("utf-8")
                        elif key.startswith("edge"):
                            companion_key = key.replace("edge", "vertex")
                            if companion_key in buffer:
                                read_task_q.put((buffer[companion_key], 
                                                 message.value.decode("utf-8")))
                                del buffer[companion_key]
                                delivered_batch += 1
                            else:
                                buffer[key] = message.value.decode("utf-8")
                        else:
                            raise ValueError(
                                "Unrecognized key {} for messages in kafka".format(key)
                            )
                    else:
                        read_task_q.put(message.value.decode("utf-8"))
                        delivered_batch += 1
        read_task_q.put(None)

    @staticmethod
    def _read_data(
        exit_event: Event,
        in_q: Queue,
        out_q: Queue,
        in_format: str = "vertex",
        out_format: str = "dataframe",
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        add_self_loop: bool = False,
        reindex: bool = True,
        is_hetero: bool = False
    ) -> NoReturn:
        while not exit_event.is_set():
            raw = in_q.get()
            if raw is None:
                in_q.task_done()
                out_q.put(None)
                break
            data = BaseLoader._parse_data(
                raw = raw,
                in_format = in_format,
                out_format = out_format,
                v_in_feats = v_in_feats,
                v_out_labels = v_out_labels,
                v_extra_feats = v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = e_in_feats,
                e_out_labels = e_out_labels,
                e_extra_feats = e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = add_self_loop,
                reindex = reindex,
                primary_id = {},
                is_hetero = is_hetero
            )
            out_q.put(data)
            in_q.task_done()

    @staticmethod
    def _parse_data(
        raw: Union[str, Tuple[str, str]],
        in_format: 'Literal["vertex", "edge", "graph"]' = "vertex",
        out_format: str = "dataframe",
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        add_self_loop: bool = False,
        reindex: bool = True,
        primary_id: dict = {},
        is_hetero: bool = False
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame], "dgl.DGLGraph", "pyg.data.Data",
               dict, Tuple[dict, dict], "pyg.data.HeteroData"]:
        """Parse raw data into dataframes, DGL graphs, or PyG graphs.
        """    
        def attr_to_tensor(
            attributes: list, attr_types: dict, df: pd.DataFrame
        ) -> "torch.Tensor":
            """Turn multiple columes of a dataframe into a tensor.
            """        
            x = []
            for col in attributes:
                dtype = attr_types[col].lower()
                if dtype.startswith("str"):
                    raise TypeError(
                        "String type not allowed for input and output features."
                    )
                if dtype.startswith("list"):
                    dtype2 = dtype.split(":")[1]
                    x.append(df[col].str.split(expand=True).to_numpy().astype(dtype2))
                elif dtype.startswith("set") or dtype.startswith("map") or dtype.startswith("date"):
                    raise NotImplementedError(
                        "{} type not supported for input and output features yet.".format(dtype))
                elif dtype == "bool":
                    x.append(df[[col]].astype("int8").to_numpy().astype(dtype))
                else:
                    x.append(df[[col]].to_numpy().astype(dtype))
            return torch.tensor(np.hstack(x)).squeeze(dim=1)

        def add_attributes(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                           graph, is_hetero: bool, mode: str, feat_name: str, 
                           target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add multiple attributes as a single feature to edges or vertices.
            """                    
            if is_hetero:
                if not vetype:
                    raise ValueError("Vertex or edge type required for heterogeneous graphs")
                # Focus on a specific type
                if mode == "pyg":
                    if target == "edge":
                        data = graph[attr_types["FromVertexTypeName"], 
                                     vetype,
                                     attr_types["ToVertexTypeName"]]
                    elif target == "vertex":
                        data = graph[vetype]
                elif mode == "dgl":
                    raise NotImplementedError
            else:
                if mode == "pyg":
                    data = graph
                elif mode == "dgl":
                    if target == "edge":
                        data = graph.edata
                    elif target == "vertex":
                        data = graph.ndata

            data[feat_name] = attr_to_tensor(attr_names, attr_types, attr_df)
        
        def add_sep_attr(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                         graph, is_hetero: bool, mode: str,
                         target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add each attribute as a single feature to edges or vertices.
            """
            if is_hetero:
                if not vetype:
                    raise ValueError("Vertex or edge type required for heterogeneous graphs")
                # Focus on a specific type
                if mode == "pyg":
                    if target == "edge":
                        data = graph[attr_types["FromVertexTypeName"], 
                                     vetype,
                                     attr_types["ToVertexTypeName"]]
                    elif target == "vertex":
                        data = graph[vetype]
                elif mode == "dgl":
                    raise NotImplementedError
            else:
                if mode == "pyg":
                    data = graph
                elif mode == "dgl":
                    if target == "edge":
                        data = graph.edata
                    elif target == "vertex":
                        data = graph.ndata

            for col in attr_names:
                dtype = attr_types[col].lower()
                if dtype.startswith("str"):
                    if mode == "dgl":
                        graph.extra_data[col] = attr_df[col].to_list()
                    elif mode == "pyg":
                        data[col] = attr_df[col].to_list()
                elif dtype.startswith("list"):
                    dtype2 = dtype.split(":")[1]
                    data[col] = torch.tensor(
                        attr_df[col]
                        .str.split(expand=True)
                        .to_numpy()
                        .astype(dtype2)
                    )
                elif dtype.startswith("set") or dtype.startswith("map") or dtype.startswith("date"):
                    raise NotImplementedError(
                        "{} type not supported for extra features yet.".format(dtype))
                elif dtype == "bool":
                    data[col] = torch.tensor(
                        attr_df[col].astype("int8").astype(dtype)
                    )
                else:
                    data[col] = torch.tensor(
                        attr_df[col].astype(dtype)
                    )
        # Read in vertex and edge CSVs as dataframes              
        vertices, edges = None, None
        if in_format == "vertex":
            # String of vertices in format vid,v_in_feats,v_out_labels,v_extra_feats
            if not is_hetero:
                v_attributes = ["vid"] + v_in_feats + v_out_labels + v_extra_feats
                data = pd.read_csv(io.StringIO(raw), header=None, names=v_attributes)
            else:
                v_file = (line.split(',') for line in raw.split('\n') if line)
                v_file_dict = defaultdict(list)
                for line in v_file:
                    v_file_dict[line[0]].append(line[1:])
                vertices = {}
                for vtype in v_file_dict:
                    v_attributes = ["vid"] + \
                                   v_in_feats.get(vtype, []) + \
                                   v_out_labels.get(vtype, []) + \
                                   v_extra_feats.get(vtype, [])
                    vertices[vtype] = pd.DataFrame(v_file_dict[vtype], columns=v_attributes)
                data = vertices
        elif in_format == "edge":
            # String of edges in format source_vid,target_vid
            if not is_hetero:
                e_attributes = ["source", "target"] + e_in_feats + e_out_labels + e_extra_feats
                data = pd.read_csv(io.StringIO(raw), header=None, names=e_attributes)
            else:
                e_file = (line.split(',') for line in raw.split('\n') if line)
                e_file_dict = defaultdict(list)
                for line in e_file:
                    e_file_dict[line[0]].append(line[1:])
                edges = {}
                for etype in e_file_dict:
                    e_attributes = ["source", "target"] + \
                                   e_in_feats.get(etype, []) + \
                                   e_out_labels.get(etype, [])  + \
                                   e_extra_feats.get(etype, [])
                    edges[etype] = pd.DataFrame(e_file_dict[etype], columns=e_attributes)
                del e_file_dict, e_file
                data = edges
        elif in_format == "graph":
            # A pair of in-memory CSVs (vertex, edge)
            v_file, e_file = raw
            if not is_hetero:
                v_attributes = ["vid"] + v_in_feats + v_out_labels + v_extra_feats
                e_attributes = ["source", "target"] + e_in_feats + e_out_labels + e_extra_feats
                vertices = pd.read_csv(io.StringIO(v_file), header=None, names=v_attributes, dtype="object")
                if primary_id:
                    id_map = pd.DataFrame({"vid": primary_id.keys(), "primary_id": primary_id.values()}, 
                                          dtype="object")
                    vertices = vertices.merge(id_map, on="vid")
                    v_extra_feats.append("primary_id")
                edges = pd.read_csv(io.StringIO(e_file), header=None, names=e_attributes, dtype="object")
                data = (vertices, edges)
            else:
                v_file = (line.split(',') for line in v_file.split('\n') if line)
                v_file_dict = defaultdict(list)
                for line in v_file:
                    v_file_dict[line[0]].append(line[1:])
                vertices = {}
                for vtype in v_file_dict:
                    v_attributes = ["vid"] + \
                                   v_in_feats.get(vtype, []) + \
                                   v_out_labels.get(vtype, []) + \
                                   v_extra_feats.get(vtype, [])
                    vertices[vtype] = pd.DataFrame(v_file_dict[vtype], columns=v_attributes, dtype="object")
                if primary_id:
                    id_map = pd.DataFrame({"vid": primary_id.keys(), "primary_id": primary_id.values()},
                                          dtype="object")
                    for vtype in vertices:
                        vertices[vtype] = vertices[vtype].merge(id_map, on="vid")
                        v_extra_feats[vtype].append("primary_id")
                del v_file_dict, v_file
                e_file = (line.split(',') for line in e_file.split('\n') if line)
                e_file_dict = defaultdict(list)
                for line in e_file:
                    e_file_dict[line[0]].append(line[1:])
                edges = {}
                for etype in e_file_dict:
                    e_attributes = ["source", "target"] + \
                                   e_in_feats.get(etype, []) + \
                                   e_out_labels.get(etype, [])  + \
                                   e_extra_feats.get(etype, [])
                    edges[etype] = pd.DataFrame(e_file_dict[etype], columns=e_attributes, dtype="object")
                del e_file_dict, e_file
                data = (vertices, edges)
        else:
            raise NotImplementedError
        # Convert dataframes into PyG or DGL graphs
        if out_format.lower() == "pyg" or out_format.lower() == "dgl":
            try:
                import torch
            except ImportError:
                raise ImportError(
                    "PyTorch is not installed. Please install it to use PyG or DGL output."
                )
            if vertices is None or edges is None:
                raise ValueError(
                    "PyG or DGL format can only be used with (sub)graph loaders."
                )
            if out_format.lower() == "dgl":
                try:
                    import dgl
                    mode = "dgl"
                except ImportError:
                    raise ImportError(
                        "DGL is not installed. Please install DGL to use DGL format."
                    )
            elif out_format.lower() == "pyg":
                try:
                    from torch_geometric.data import Data as pygData
                    from torch_geometric.data import \
                        HeteroData as pygHeteroData
                    from torch_geometric.utils import add_self_loops
                    mode = "pyg"
                except ImportError:
                    raise ImportError(
                        "PyG is not installed. Please install PyG to use PyG format."
                    )
            else:
                raise NotImplementedError
            # Reformat as a graph.
            # Need to have a pair of tables for edges and vertices.
            if not is_hetero:
                # Deal with edgelist first
                if reindex:
                    vertices["tmp_id"] = range(len(vertices))
                    id_map = vertices[["vid", "tmp_id"]]
                    edges = edges.merge(id_map, left_on="source", right_on="vid")
                    edges.drop(columns=["source", "vid"], inplace=True)
                    edges = edges.merge(id_map, left_on="target", right_on="vid")
                    edges.drop(columns=["target", "vid"], inplace=True)
                    edgelist = edges[["tmp_id_x", "tmp_id_y"]]
                else:
                    edgelist = edges[["source", "target"]]
                edgelist = torch.tensor(edgelist.to_numpy().T, dtype=torch.long)
                if mode == "dgl":
                    data = dgl.graph(data=(edgelist[0], edgelist[1]))
                    if add_self_loop:
                        data = dgl.add_self_loop(data)
                    data.extra_data = {}
                elif mode == "pyg":
                    data = pygData()
                    if add_self_loop:
                        edgelist = add_self_loops(edgelist)[0]
                    data["edge_index"] = edgelist
                del edgelist
                # Deal with edge attributes
                if e_in_feats:
                    add_attributes(e_in_feats, e_attr_types, edges, 
                                   data, is_hetero, mode, "edge_feat", "edge")
                if e_out_labels:
                    add_attributes(e_out_labels, e_attr_types, edges, 
                                   data, is_hetero, mode, "edge_label", "edge")
                if e_extra_feats:
                    add_sep_attr(e_extra_feats, e_attr_types, edges,
                                 data, is_hetero, mode, "edge")            
                del edges
                # Deal with vertex attributes next
                if v_in_feats:
                    add_attributes(v_in_feats, v_attr_types, vertices, 
                                   data, is_hetero, mode, "x", "vertex")
                if v_out_labels:
                    add_attributes(v_out_labels, v_attr_types, vertices, 
                                   data, is_hetero, mode, "y", "vertex")
                if v_extra_feats:
                    add_sep_attr(v_extra_feats, v_attr_types, vertices,
                                 data, is_hetero, mode, "vertex")
                del vertices
            else:
                # Heterogeneous graph
                # Deal with edgelist first
                edgelist = {}
                if reindex:
                    id_map = {}
                    for vtype in vertices:
                        vertices[vtype]["tmp_id"] = range(len(vertices[vtype]))
                        id_map[vtype] = vertices[vtype][["vid", "tmp_id"]]
                    for etype in edges:
                        source_type = e_attr_types[etype]["FromVertexTypeName"]
                        target_type = e_attr_types[etype]["ToVertexTypeName"]
                        if e_attr_types[etype]["IsDirected"] or source_type==target_type:
                            edges[etype] = edges[etype].merge(id_map[source_type], left_on="source", right_on="vid")
                            edges[etype].drop(columns=["source", "vid"], inplace=True)
                            edges[etype] = edges[etype].merge(id_map[target_type], left_on="target", right_on="vid")
                            edges[etype].drop(columns=["target", "vid"], inplace=True)
                            edgelist[etype] = edges[etype][["tmp_id_x", "tmp_id_y"]]
                        else:
                            subdf1 = edges[etype].merge(id_map[source_type], left_on="source", right_on="vid")
                            subdf1.drop(columns=["source", "vid"], inplace=True)
                            subdf1 = subdf1.merge(id_map[target_type], left_on="target", right_on="vid")
                            subdf1.drop(columns=["target", "vid"], inplace=True)
                            if len(subdf1) < len(edges[etype]):
                                subdf2 = edges[etype].merge(id_map[source_type], left_on="target", right_on="vid")
                                subdf2.drop(columns=["target", "vid"], inplace=True)
                                subdf2 = subdf2.merge(id_map[target_type], left_on="source", right_on="vid")
                                subdf2.drop(columns=["source", "vid"], inplace=True)
                                subdf1 = pd.concat((subdf1, subdf2), ignore_index=True)
                            edges[etype] = subdf1
                            edgelist[etype] = edges[etype][["tmp_id_x", "tmp_id_y"]]
                else:
                    for etype in edges:
                        edgelist[etype] = edges[etype][["source", "target"]]
                for etype in edges:
                    edgelist[etype] = torch.tensor(edgelist[etype].to_numpy().T, dtype=torch.long)
                if mode == "dgl":
                    data.extra_data = {}
                    raise NotImplementedError
                elif mode == "pyg":
                    data = pygHeteroData()
                    for etype in edgelist:
                        if add_self_loop:
                            edgelist[etype] = add_self_loops(edgelist[etype])[0]
                        data[e_attr_types[etype]["FromVertexTypeName"], 
                             etype,
                             e_attr_types[etype]["ToVertexTypeName"]].edge_index = edgelist[etype]
                del edgelist
                # Deal with edge attributes
                if e_in_feats:
                    for etype in edges:
                        if etype not in e_in_feats:
                            continue
                        add_attributes(e_in_feats[etype], e_attr_types[etype], edges[etype], 
                                       data, is_hetero, mode, "edge_feat", "edge", etype)
                if e_out_labels:
                    for etype in edges:
                        if etype not in e_out_labels:
                            continue
                        add_attributes(e_out_labels[etype], e_attr_types[etype], edges[etype], 
                                       data, is_hetero, mode, "edge_label", "edge", etype)
                if e_extra_feats:
                    for etype in edges:
                        if etype not in e_extra_feats:
                            continue
                        add_sep_attr(e_extra_feats[etype], e_attr_types[etype], edges[etype],
                                     data, is_hetero, mode, "edge", etype)   
                del edges
                # Deal with vertex attributes next
                if v_in_feats:
                    for vtype in vertices:
                        if vtype not in v_in_feats:
                            continue
                        add_attributes(v_in_feats[vtype], v_attr_types[vtype], vertices[vtype], 
                                       data, is_hetero, mode, "x", "vertex", vtype)
                if v_out_labels:
                    for vtype in vertices:
                        if vtype not in v_out_labels:
                            continue
                        add_attributes(v_out_labels[vtype], v_attr_types[vtype], vertices[vtype], 
                                       data, is_hetero, mode, "y", "vertex", vtype)
                if v_extra_feats:
                    for vtype in vertices:
                        if vtype not in v_extra_feats:
                            continue
                        add_sep_attr(v_extra_feats[vtype], v_attr_types[vtype], vertices[vtype],
                                     data, is_hetero, mode, "vertex", vtype)   
                del vertices
        elif out_format.lower() == "dataframe":
            pass
        else:
            raise NotImplementedError

        return data

    def _start(self) -> None:
        # This is a template. Implement your own logics here.
        # Create task and result queues
        self._request_task_q = Queue()
        self._read_task_q = Queue()
        self._data_q = Queue(self._buffer_size)
        self._exit_event = Event()

        # Start requesting thread. Finish with your logic.
        self._requester = Thread(target=self._request_kafka, args=())
        self._requester.start()

        # Start downloading thread. Finish with your logic.
        self._downloader = Thread(target=self._download_from_kafka, args=())
        self._downloader.start()

        # Start reading thread. Finish with your logic.
        self._reader = Thread(target=self._read_data, args=())
        self._reader.start()

        raise NotImplementedError

    def __iter__(self) -> Iterator:
        if self.num_batches == 1:
            return iter([self.data])
        self._reset()
        self._start()
        self._iterations += 1
        self._iterator = True
        return self

    def __next__(self) -> Any:
        if not self._iterator:
            raise TypeError(
                "Not an iterator. Call `iter` on it first or use it in a for loop."
            )
        if not self._data_q:
            self._iterator = False
            raise StopIteration
        data = self._data_q.get()
        if data is None:
            self._iterator = False
            raise StopIteration
        return data

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all data if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself"""
        if self.num_batches == 1:
            if self._data is None:
                self._reset()
                self._start()
                self._data = self._data_q.get()
            return self._data
        else:
            return self

    def _reset(self) -> None:
        logging.debug("Resetting the loader")
        if self._exit_event:
            self._exit_event.set()
        if self._request_task_q:
            self._request_task_q.put(None)
        if self._download_task_q:
            self._download_task_q.put(None)
        if self._read_task_q:
            while True:
                try:
                    self._read_task_q.get(block=False)
                except Empty:
                    break
            self._read_task_q.put(None)
        if self._data_q:
            while True:
                try:
                    self._data_q.get(block=False)
                except Empty:
                    break
        if self._requester:
            self._requester.join()
        if self._downloader:
            self._downloader.join()
        if self._reader:
            self._reader.join()
        del self._request_task_q, self._download_task_q, self._read_task_q, self._data_q
        self._exit_event = None
        self._requester, self._downloader, self._reader = None, None, None
        self._request_task_q, self._download_task_q, self._read_task_q, self._data_q = (
            None,
            None,
            None,
            None,
        )
        if self.delete_kafka_topic:
            if self._kafka_topic:
                self._kafka_consumer.unsubscribe()
                resp = self._kafka_admin.delete_topics([self._kafka_topic])
                del_res = resp.to_object()["topic_error_codes"][0]
                if del_res["error_code"] != 0:
                    raise TigerGraphException(
                        "Failed to delete topic {}".format(del_res["topic"])
                    )
                self._kafka_topic = None
        logging.debug("Successfully reset the loader")

    def fetch(self, payload: dict) -> None:
        """Fetch the specific data instances for inference/prediction.

        Args:
            payload (dict): The JSON payload to send to the API.
        """
        # Send request
        # Parse data
        # Return data
        raise NotImplementedError


class NeighborLoader(BaseLoader):
    """NeighborLoader

    A data loader that performs neighbor sampling.
    You can declare a `NeighborLoader` instance with the factory function `neighborLoader()`.

    A neighbor loader is an iterable.
    When you loop through a neighbor loader instance, it loads one batch of data from the graph to which you established a connection.

    In every iteration, it first chooses a specified number of vertices as seeds,
    then picks a specified number of neighbors of each seed at random,
    then the same number of neighbors of each neighbor, and repeat for a specified number of hops.
    It loads both the vertices and the edges connecting them to their neighbors.
    The vertices sampled this way along with their edges form one subgraph and is contained in one batch.

    You can iterate on the instance until every vertex has been picked as seed.

    Examples:

    The following example iterates over a neighbor loader instance.
    [.wrap,python]
    ----
    for i, batch in enumerate(neighbor_loader):
        print("----Batch {}----".format(i))
        print(batch)
    ----



    See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_neighborloader.ipynb[the ML Workbench tutorial notebook]
        for examples.
    See more details about the specific sampling method in
    link:https://arxiv.org/abs/1706.02216[Inductive Representation Learning on Large Graphs].
    """
    def __init__(
        self,
        graph: "TigerGraphConnection",
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
        filter_by: Union[str, dict] = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        reverse_edge: bool = False,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        kafka_security_protocol: str = "PLAINTEXT",
        kafka_sasl_mechanism: str = None,
        kafka_sasl_plain_username: str = None,
        kafka_sasl_plain_password: str = None,
        kafka_producer_ca_location: str = None,
        kafka_consumer_ca_location: str = None,
        timeout: int = 300000,
    ) -> None:
        """NO DOC"""

        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            kafka_security_protocol,
            kafka_sasl_mechanism,
            kafka_sasl_plain_username,
            kafka_sasl_plain_password,
            kafka_producer_ca_location,
            kafka_consumer_ca_location,
            timeout,
        )
        # Resolve attributes
        is_hetero = any(map(lambda x: isinstance(x, dict), 
                        (v_in_feats, v_out_labels, v_extra_feats,
                         e_in_feats, e_out_labels, e_extra_feats)))
        self.is_hetero = is_hetero
        self.v_in_feats = self._validate_vertex_attributes(v_in_feats, is_hetero)
        self.v_out_labels = self._validate_vertex_attributes(v_out_labels, is_hetero)
        self.v_extra_feats = self._validate_vertex_attributes(v_extra_feats, is_hetero)
        self.e_in_feats = self._validate_edge_attributes(e_in_feats, is_hetero)
        self.e_out_labels = self._validate_edge_attributes(e_out_labels, is_hetero)
        self.e_extra_feats = self._validate_edge_attributes(e_extra_feats, is_hetero)
        if is_hetero:
            self._vtypes = list(
                    set(self.v_in_feats.keys())
                    | set(self.v_out_labels.keys())
                    | set(self.v_extra_feats.keys())
                )
            if not self._vtypes:
                self._vtypes = list(self._v_schema.keys())
            self._etypes = list(
                set(self.e_in_feats.keys())
                | set(self.e_out_labels.keys())
                | set(self.e_extra_feats.keys())
            )
            if not self._etypes:
                self._etypes = list(self._e_schema.keys())
        else:
            self._vtypes = list(self._v_schema.keys())
            self._etypes = list(self._e_schema.keys())
        # Resolve seeds
        if batch_size:
            # If batch_size is given, calculate the number of batches
            if not filter_by:
                self._seed_types = self._vtypes
                num_vertices = sum(self._graph.getVertexCount(self._seed_types).values())
            elif isinstance(filter_by, str):
                self._seed_types = self._vtypes
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by))
                    for k in self._seed_types
                )
            elif isinstance(filter_by, dict):
                self._seed_types = list(filter_by.keys())
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by[k]))
                    for k in self._seed_types
                )
            else:
                raise ValueError("filter_by should be None, attribute name, or dict of {type name: attribute name}.")
            self.num_batches = math.ceil(num_vertices / batch_size)
        else:
            # Otherwise, take the number of batches as is.
            self._seed_types = self._vtypes if ((not filter_by) or isinstance(filter_by, str)) else list(filter_by.keys())
            self.num_batches = num_batches
        # Initialize parameters for the query
        self._payload["num_batches"] = self.num_batches
        self._payload["num_neighbors"] = num_neighbors
        self._payload["num_hops"] = num_hops
        if filter_by:
            if isinstance(filter_by, str):
                self._payload["filter_by"] = filter_by
            else:
                attr = set(filter_by.values())
                if len(attr) != 1:
                    raise NotImplementedError("Filtering by different attributes for different vertex types is not supported. Please use the same attribute for different types.")
                self._payload["filter_by"] = attr.pop()
        self._payload["shuffle"] = shuffle
        self._payload["v_types"] = self._vtypes
        self._payload["e_types"] = self._etypes
        self._payload["seed_types"] = self._seed_types
        # Output
        self.add_self_loop = add_self_loop
        # Install query
        self.query_name = self._install_query()

    def _install_query(self):
        # Install the right GSQL query for the loader.
        query_suffix = []
        query_replace = {}

        if isinstance(self.v_in_feats, dict) or isinstance(self.e_in_feats, dict):
            # Multiple vertex types
            print_query_seed = ""
            print_query_other = ""
            for idx, vtype in enumerate(self._vtypes):
                v_attr_names = (
                    self.v_in_feats.get(vtype, [])
                    + self.v_out_labels.get(vtype, [])
                    + self.v_extra_feats.get(vtype, [])
                )
                query_suffix.extend(v_attr_names)
                v_attr_types = self._v_schema[vtype]
                if v_attr_names:
                    print_attr = '+","+'.join(
                        "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                        for attr in v_attr_names
                    )
                    print_query_seed += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "," + {} + ",1\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype, print_attr)
                    print_query_other += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "," + {} + ",0\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype, print_attr)
                else:
                    print_query_seed += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + ",1\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype)
                    print_query_other += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + ",0\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype)
            print_query_seed += "END"
            print_query_other += "END"
            query_replace["{SEEDVERTEXATTRS}"] = print_query_seed
            query_replace["{OTHERVERTEXATTRS}"] = print_query_other
            # Multiple edge types
            print_query = ""
            for idx, etype in enumerate(self._etypes):
                e_attr_names = (
                    self.e_in_feats.get(etype, [])
                    + self.e_out_labels.get(etype, [])
                    + self.e_extra_feats.get(etype, [])
                )
                query_suffix.extend(e_attr_names)
                e_attr_types = self._e_schema[etype]
                if e_attr_names:
                    print_attr = '+","+'.join(
                        "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                        for attr in e_attr_names
                    )
                    print_query += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype, print_attr)
                else:
                    print_query += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype)
            print_query += "END"
            query_replace["{EDGEATTRS}"] = print_query
            query_suffix = list(dict.fromkeys(query_suffix))
        else:
            # Ignore vertex types
            v_attr_names = self.v_in_feats + self.v_out_labels + self.v_extra_feats
            query_suffix.extend(v_attr_names)
            v_attr_types = next(iter(self._v_schema.values()))
            if v_attr_names:
                print_attr = '+","+'.join(
                    "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                    for attr in v_attr_names
                )
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "," + {} + ",1\\n")'.format(
                    print_attr
                )
                query_replace["{SEEDVERTEXATTRS}"] = print_query
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "," + {} + ",0\\n")'.format(
                    print_attr
                )
                query_replace["{OTHERVERTEXATTRS}"] = print_query
            else:
                print_query = '@@v_batch += (int_to_string(getvid(s)) + ",1\\n")'
                query_replace["{SEEDVERTEXATTRS}"] = print_query
                print_query = '@@v_batch += (int_to_string(getvid(s)) + ",0\\n")'
                query_replace["{OTHERVERTEXATTRS}"] = print_query
            # Ignore edge types
            e_attr_names = self.e_in_feats + self.e_out_labels + self.e_extra_feats
            query_suffix.extend(e_attr_names)
            e_attr_types = next(iter(self._e_schema.values()))
            if e_attr_names:
                print_attr = '+","+'.join(
                    "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                    for attr in e_attr_names
                )
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + "\\n")'.format(
                    print_attr
                )
            else:
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "\\n")'
            query_replace["{EDGEATTRS}"] = print_query
        query_replace["{QUERYSUFFIX}"] = "_".join(query_suffix)
        # Install query
        query_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "gsql",
                "dataloaders",
                "neighbor_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size * 2)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        # Start requesting thread.
        if self.kafka_address_consumer:
            # If using kafka
            self._kafka_topic = "{}_{}".format(self.loader_id, self._iterations)
            self._payload["kafka_topic"] = self._kafka_topic
            self._requester = Thread(
                target=self._request_kafka,
                args=(
                    self._exit_event,
                    self._graph,
                    self.query_name,
                    self._kafka_consumer,
                    self._kafka_admin,
                    self._kafka_topic,
                    self.kafka_partitions,
                    self.kafka_replica,
                    self.max_kafka_msg_size,
                    self.kafka_retention_ms,
                    self.timeout,
                    self._payload,
                ),
            )
        else:
            # Otherwise, use rest api
            self._requester = Thread(
                target=self._request_rest,
                args=(
                    self._graph,
                    self.query_name,
                    self._read_task_q,
                    self.timeout,
                    self._payload,
                    "both",
                ),
            )
        self._requester.start()

        # If using Kafka, start downloading thread.
        if self.kafka_address_consumer:
            self._downloader = Thread(
                target=self._download_from_kafka,
                args=(
                    self._exit_event,
                    self._read_task_q,
                    self.num_batches,
                    True,
                    self._kafka_consumer,
                ),
            )
            self._downloader.start()

        # Start reading thread.
        if not self.is_hetero:
            v_extra_feats = self.v_extra_feats + ["is_seed"]
            v_attr_types = next(iter(self._v_schema.values()))
            v_attr_types["is_seed"] = "bool"
            e_attr_types = next(iter(self._e_schema.values()))
        else:
            v_extra_feats = {}
            for vtype in self._vtypes:
                v_extra_feats[vtype] = self.v_extra_feats.get(vtype, []) + ["is_seed"]
            v_attr_types = self._v_schema
            for vtype in v_attr_types:
                v_attr_types[vtype]["is_seed"] = "bool"
            e_attr_types = self._e_schema
        self._reader = Thread(
            target=self._read_data,
            args=(
                self._exit_event,
                self._read_task_q,
                self._data_q,
                "graph",
                self.output_format,
                self.v_in_feats,
                self.v_out_labels,
                v_extra_feats,
                v_attr_types,
                self.e_in_feats,
                self.e_out_labels,
                self.e_extra_feats,
                e_attr_types,
                self.add_self_loop,
                True,
                self.is_hetero
            ),
        )
        self._reader.start()

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all data if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself"""
        return super().data

    def fetch(self, vertices: list) -> None:
        """Fetch neighborhood subgraphs for specific vertices.

        Args:
            vertices (list of dict):
                Vertices to fetch with their neighborhood subgraphs.
                Each vertex corresponds to a dict with two mandatory keys
                {"primary_id": ..., "type": ...}
        """
        # Check input
        if not vertices:
            return None
        if not isinstance(vertices, list):
            raise ValueError(
                'Input to fetch() should be in format: [{"primary_id": ..., "type": ...}, ...]'
            )
        for i in vertices:
            if not (isinstance(i, dict) and ("primary_id" in i) and ("type" in i)):
                raise ValueError(
                    'Input to fetch() should be in format: [{"primary_id": ..., "type": ...}, ...]'
                )
        # Send request
        _payload = {}
        _payload["v_types"] = self._payload["v_types"]
        _payload["e_types"] = self._payload["e_types"]
        _payload["num_batches"] = 1
        _payload["num_neighbors"] = self._payload["num_neighbors"]
        _payload["num_hops"] = self._payload["num_hops"]
        _payload["input_vertices"] = []
        for i in vertices:
            _payload["input_vertices"].append((i["primary_id"], i["type"]))
        resp = self._graph.runInstalledQuery(
            self.query_name, params=_payload, timeout=self.timeout, usePost=True
        )
        # Parse data        
        if not self.is_hetero:
            v_extra_feats = self.v_extra_feats + ["is_seed"]
            v_attr_types = next(iter(self._v_schema.values()))
            v_attr_types["is_seed"] = "bool"
            v_attr_types["primary_id"] = "str"
            e_attr_types = next(iter(self._e_schema.values()))
        else:
            v_extra_feats = {}
            for vtype in self._vtypes:
                v_extra_feats[vtype] = self.v_extra_feats.get(vtype, []) + ["is_seed"]
            v_attr_types = self._v_schema
            for vtype in v_attr_types:
                v_attr_types[vtype]["is_seed"] = "bool"
                v_attr_types[vtype]["primary_id"] = "str"
            e_attr_types = self._e_schema
        i = resp[0]
        data = self._parse_data(
            raw = (i["vertex_batch"], i["edge_batch"]),
            in_format = "graph",
            out_format = self.output_format,
            v_in_feats = self.v_in_feats,
            v_out_labels = self.v_out_labels,
            v_extra_feats = v_extra_feats,
            v_attr_types = v_attr_types,
            e_in_feats = self.e_in_feats,
            e_out_labels = self.e_out_labels,
            e_extra_feats = self.e_extra_feats,
            e_attr_types = e_attr_types,
            add_self_loop = self.add_self_loop,
            reindex = True,
            primary_id = i["pids"],
            is_hetero = self.is_hetero
        )
        # Return data
        return data


class EdgeLoader(BaseLoader):
    """Edge Loader.

    Data loader that loads all edges from the graph in batches.
    You can define an edge loader using the `edgeLoader()` factory function.

    An edge loader instance is an iterable.
    When you loop through an edge loader instance, it loads one batch of data from the graph to which you established a connection in each iteration.
    The size and total number of batches are specified when you define the edge loader instance.

    The boolean attribute provided to `filter_by` indicates which edges are included.
    If you need random batches, set `shuffle` to True.

    Examples:
    The following for loop prints every edge in batches.

    [tabs]
    ====
    Input::
    +
    --
    [.wrap,python]
    ----
    edge_loader = conn.gds.edgeLoader(
        num_batches=10,
        attributes=["time", "is_train"],
        shuffle=True,
        filter_by=None
    )
    for i, batch in enumerate(edge_loader):
        print("----Batch {}: Shape {}----".format(i, batch.shape))
        print(batch.head(1))
    ----
    --
    Output::
    +
    --
    ----
    ----Batch 0: Shape (1129, 4)----
        source    target  time  is_train
    0  3145728  22020185     0         1
    ----Batch 1: Shape (1002, 4)----
        source    target  time  is_train
    0  1048577  20971586     0         1
    ----Batch 2: Shape (1124, 4)----
    source   target  time  is_train
    0       4  9437199     0         1
    ----Batch 3: Shape (1071, 4)----
        source    target  time  is_train
    0  11534340  32505859     0         1
    ----Batch 4: Shape (978, 4)----
        source    target  time  is_train
    0  11534341  16777293     0         1
    ----Batch 5: Shape (1149, 4)----
        source   target  time  is_train
    0  5242882  2097158     0         1
    ----Batch 6: Shape (1013, 4)----
        source    target  time  is_train
    0  4194305  23068698     0         1
    ----Batch 7: Shape (1037, 4)----
        source   target  time  is_train
    0  7340035  4194337     0         0
    ----Batch 8: Shape (1067, 4)----
    source   target  time  is_train
    0       3  1048595     0         1
    ----Batch 9: Shape (986, 4)----
        source    target  time  is_train
    0  9437185  13631508     0         1
    ----
    --
    ====


    See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_edgeloader.ipynb[the ML Workbench edge loader tutorial notebook]
        for examples.
    """
    def __init__(
        self,
        graph: "TigerGraphConnection",
        attributes: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "dataframe",
        loader_id: str = None,
        buffer_size: int = 4,
        reverse_edge: bool = False,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        kafka_security_protocol: str = "PLAINTEXT",
        kafka_sasl_mechanism: str = None,
        kafka_sasl_plain_username: str = None,
        kafka_sasl_plain_password: str = None,
        kafka_producer_ca_location: str = None,
        kafka_consumer_ca_location: str = None,
        timeout: int = 300000,
    ) -> None:
        """
        NO DOC.
        """
        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            kafka_security_protocol,
            kafka_sasl_mechanism,
            kafka_sasl_plain_username,
            kafka_sasl_plain_password,
            kafka_producer_ca_location,
            kafka_consumer_ca_location,
            timeout,
        )
        # Resolve attributes
        is_hetero = isinstance(attributes, dict)
        self.is_hetero = is_hetero
        self.attributes = self._validate_edge_attributes(attributes, is_hetero)
        if is_hetero:
            self._etypes = list(attributes.keys())
            if not self._etypes:
                self._etypes = list(self._e_schema.keys())
        else:
            self._etypes = list(self._e_schema.keys())
        # Initialize parameters for the query
        if batch_size:
            # If batch_size is given, calculate the number of batches
            if filter_by:
                # TODO: get edge count with filter
                raise NotImplementedError
            else:
                num_edges = sum(self._graph.getEdgeCount(i) for i in self._etypes)
            self.num_batches = math.ceil(num_edges / batch_size)
        else:
            # Otherwise, take the number of batches as is.
            self.num_batches = num_batches
        # Initialize the exporter
        self._payload["num_batches"] = self.num_batches
        if filter_by:
            self._payload["filter_by"] = filter_by
        self._payload["shuffle"] = shuffle
        self._payload["e_types"] = self._etypes
        # Output
        # Install query
        self.query_name = self._install_query()

    def _install_query(self):
        # Install the right GSQL query for the loader.
        query_suffix = []
        query_replace = {}

        if isinstance(self.attributes, dict):
            # Multiple edge types
            print_query = ""
            for idx, etype in enumerate(self._etypes):
                e_attr_names = self.attributes.get(etype, [])
                query_suffix.extend(e_attr_names)
                e_attr_types = self._e_schema[etype]
                if e_attr_names:
                    print_attr = '+","+'.join(
                        "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                        for attr in e_attr_names
                    )
                    print_query += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype, print_attr)
                else:
                    print_query += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype)
            print_query += "END"
            query_replace["{EDGEATTRS}"] = print_query
            query_suffix = list(dict.fromkeys(query_suffix))
        else:
            # Ignore edge types
            e_attr_names = self.attributes
            query_suffix.extend(e_attr_names)
            e_attr_types = next(iter(self._e_schema.values()))
            if e_attr_names:
                print_attr = '+","+'.join(
                    "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                    for attr in e_attr_names
                )
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + "\\n")'.format(
                    print_attr
                )
            else:
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "\\n")'
            query_replace["{EDGEATTRS}"] = print_query
        query_replace["{QUERYSUFFIX}"] = "_".join(query_suffix)
        # Install query
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "edge_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size * 2)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        # Start requesting thread.
        if self.kafka_address_consumer:
            # If using kafka
            self._kafka_topic = "{}_{}".format(self.loader_id, self._iterations)
            self._payload["kafka_topic"] = self._kafka_topic
            self._requester = Thread(
                target=self._request_kafka,
                args=(
                    self._exit_event,
                    self._graph,
                    self.query_name,
                    self._kafka_consumer,
                    self._kafka_admin,
                    self._kafka_topic,
                    self.kafka_partitions,
                    self.kafka_replica,
                    self.max_kafka_msg_size,
                    self.kafka_retention_ms,
                    self.timeout,
                    self._payload,
                ),
            )
        else:
            # Otherwise, use rest api
            self._requester = Thread(
                target=self._request_rest,
                args=(
                    self._graph,
                    self.query_name,
                    self._read_task_q,
                    self.timeout,
                    self._payload,
                    "edge",
                ),
            )
        self._requester.start()

        # If using Kafka, start downloading thread.
        if self.kafka_address_consumer:
            self._downloader = Thread(
                target=self._download_from_kafka,
                args=(
                    self._exit_event,
                    self._read_task_q,
                    self.num_batches,
                    False,
                    self._kafka_consumer,
                ),
            )
            self._downloader.start()

        # Start reading thread.
        if not self.is_hetero:
            e_attr_types = next(iter(self._e_schema.values()))
        else:
            e_attr_types = self._e_schema
        self._reader = Thread(
            target=self._read_data,
            args=(
                self._exit_event,
                self._read_task_q,
                self._data_q,
                "edge",
                self.output_format,
                [],
                [],
                [],
                {},
                self.attributes,
                {} if self.is_hetero else [],
                {} if self.is_hetero else [],
                e_attr_types,
                False,
                False,
                self.is_hetero
            ),
        )
        self._reader.start()

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all edges if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself."""
        return super().data


class VertexLoader(BaseLoader):
    """Vertex Loader.

    Data loader that loads all vertices from the graph in batches.

    A vertex loader instance is an iterable.
    When you loop through a vertex loader instance, it loads one batch of data from the graph to which you established a connection in each iteration.
    The size and total number of batches are specified when you define the vertex loader instance.

    The boolean attribute provided to `filter_by` indicates which vertices are included.
    If you need random batches, set `shuffle` to True.

    Examples:
    The following for loop loads all vertices in the graph and prints one from each batch:

    [tabs]
    ====
    Input::
    +
    --
    [.wrap,python]
    ----
    vertex_loader = conn.gds.vertexLoader(
        num_batches=10,
        attributes=["time", "is_train"],
        shuffle=True,
        filter_by=None
    )

    for i, batch in enumerate(edge_loader):
        print("----Batch {}: Shape {}----".format(i, batch.shape))
        print(batch.head(1)) <1>
    ----
    <1> Since the example does not provide an output format, the output format defaults to panda frames, have access to the methods of panda frame instances.
    --
    Output::
    +
    --
    [.wrap,python]
    ----
    ----Batch 0: Shape (1129, 4)----
    source    target  time  is_train
    0  3145728  22020185     0         1
    ----Batch 1: Shape (1002, 4)----
        source    target  time  is_train
    0  1048577  20971586     0         1
    ----Batch 2: Shape (1124, 4)----
    source   target  time  is_train
    0       4  9437199     0         1
    ----Batch 3: Shape (1071, 4)----
        source    target  time  is_train
    0  11534340  32505859     0         1
    ----Batch 4: Shape (978, 4)----
        source    target  time  is_train
    0  11534341  16777293     0         1
    ----Batch 5: Shape (1149, 4)----
        source   target  time  is_train
    0  5242882  2097158     0         1
    ----Batch 6: Shape (1013, 4)----
        source    target  time  is_train
    0  4194305  23068698     0         1
    ----Batch 7: Shape (1037, 4)----
        source   target  time  is_train
    0  7340035  4194337     0         0
    ----Batch 8: Shape (1067, 4)----
    source   target  time  is_train
    0       3  1048595     0         1
    ----Batch 9: Shape (986, 4)----
        source    target  time  is_train
    0  9437185  13631508     0         1
    ----
    --
    ====



    See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_vertexloader.ipynb[the ML Workbench tutorial notebook]
        for more examples.
    """
    def __init__(
        self,
        graph: "TigerGraphConnection",
        attributes: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "dataframe",
        loader_id: str = None,
        buffer_size: int = 4,
        reverse_edge: bool = False,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        kafka_security_protocol: str = "PLAINTEXT",
        kafka_sasl_mechanism: str = None,
        kafka_sasl_plain_username: str = None,
        kafka_sasl_plain_password: str = None,
        kafka_producer_ca_location: str = None,
        kafka_consumer_ca_location: str = None,
        timeout: int = 300000,
    ) -> None:
        """
        NO DOC
        """
        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            kafka_security_protocol,
            kafka_sasl_mechanism,
            kafka_sasl_plain_username,
            kafka_sasl_plain_password,
            kafka_producer_ca_location,
            kafka_consumer_ca_location,
            timeout,
        )
        # Resolve attributes
        is_hetero = isinstance(attributes, dict)
        self.is_hetero = is_hetero
        self.attributes = self._validate_vertex_attributes(attributes, is_hetero)
        if is_hetero:
            self._vtypes = list(self.attributes.keys())
            if not self._vtypes:
                self._vtypes = list(self._v_schema.keys())
        else:
            self._vtypes = list(self._v_schema.keys())
        # Initialize parameters for the query
        if batch_size:
            # If batch_size is given, calculate the number of batches
            num_vertices_by_type = self._graph.getVertexCount(self._vtypes)
            if filter_by:
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by))
                    for k in num_vertices_by_type
                )
            else:
                num_vertices = sum(num_vertices_by_type.values())
            self.num_batches = math.ceil(num_vertices / batch_size)
        else:
            # Otherwise, take the number of batches as is.
            self.num_batches = num_batches
        self._payload["num_batches"] = self.num_batches
        if filter_by:
            self._payload["filter_by"] = filter_by
        self._payload["shuffle"] = shuffle
        self._payload["v_types"] = self._vtypes
        # Install query
        self.query_name = self._install_query()

    def _install_query(self) -> str:
        # Install the right GSQL query for the loader.
        query_suffix = []
        query_replace = {}

        if isinstance(self.attributes, dict):
            # Multiple vertex types
            print_query = ""
            for idx, vtype in enumerate(self._vtypes):
                v_attr_names = self.attributes.get(vtype, [])
                query_suffix.extend(v_attr_names)
                v_attr_types = self._v_schema[vtype]
                if v_attr_names:
                    print_attr = '+","+'.join(
                        "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                        for attr in v_attr_names
                    )
                    print_query += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "," + {} + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype, print_attr)
                else:
                    print_query += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype)
            print_query += "END"
            query_replace["{VERTEXATTRS}"] = print_query
            query_suffix = list(dict.fromkeys(query_suffix))
        else:
            # Ignore vertex types
            v_attr_names = self.attributes
            query_suffix.extend(v_attr_names)
            v_attr_types = next(iter(self._v_schema.values()))
            if v_attr_names:
                print_attr = '+","+'.join(
                    "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                    for attr in v_attr_names
                )
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "," + {} + "\\n")'.format(
                    print_attr
                )
            else:
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "\\n")'
            query_replace["{VERTEXATTRS}"] = print_query
        query_replace["{QUERYSUFFIX}"] = "_".join(query_suffix)
        # Install query
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "vertex_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size * 2)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        # Start requesting thread.
        if self.kafka_address_consumer:
            # If using kafka
            self._kafka_topic = "{}_{}".format(self.loader_id, self._iterations)
            self._payload["kafka_topic"] = self._kafka_topic
            self._requester = Thread(
                target=self._request_kafka,
                args=(
                    self._exit_event,
                    self._graph,
                    self.query_name,
                    self._kafka_consumer,
                    self._kafka_admin,
                    self._kafka_topic,
                    self.kafka_partitions,
                    self.kafka_replica,
                    self.max_kafka_msg_size,
                    self.kafka_retention_ms,
                    self.timeout,
                    self._payload,
                ),
            )
        else:
            # Otherwise, use rest api
            self._requester = Thread(
                target=self._request_rest,
                args=(
                    self._graph,
                    self.query_name,
                    self._read_task_q,
                    self.timeout,
                    self._payload,
                    "vertex",
                ),
            )
        self._requester.start()

        # If using Kafka, start downloading thread.
        if self.kafka_address_consumer:
            self._downloader = Thread(
                target=self._download_from_kafka,
                args=(
                    self._exit_event,
                    self._read_task_q,
                    self.num_batches,
                    False,
                    self._kafka_consumer,
                ),
            )
            self._downloader.start()

        # Start reading thread.
        if not self.is_hetero:
            v_attr_types = next(iter(self._v_schema.values()))
        else:
            v_attr_types = self._v_schema
        self._reader = Thread(
            target=self._read_data,
            args=(
                self._exit_event,
                self._read_task_q,
                self._data_q,
                "vertex",
                self.output_format,
                self.attributes,
                {} if self.is_hetero else [],
                {} if self.is_hetero else [],
                v_attr_types,
                [],
                [],
                [],
                {},
                False,
                False,
                self.is_hetero
            ),
        )
        self._reader.start()

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all data if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself."""
        return super().data


class GraphLoader(BaseLoader):
    """Graph Loader.

    Data loader that loads all edges from the graph in batches, along with the vertices that are connected with each edge.

    Different from NeighborLoader which produces connected subgraphs, this loader
        loads all edges by batches and vertices attached to those edges.

    There are two ways to use the data loader:

    * It can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all data at once (`num_batches=1`),
          there will be only one batch (of all the data) in the iterator.
    * You can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader itself.

    Examples:
    The following for loop prints all edges and their connected vertices in batches.
    The output format is `PyG`:


    [tabs]
    ====
    Input::
    +
    --
    [.wrap,python]
    ----
    graph_loader = conn.gds.graphLoader(
        num_batches=10,
        v_in_feats = ["x"],
        v_out_labels = ["y"],
        v_extra_feats = ["train_mask", "val_mask", "test_mask"],
        e_in_feats=["time"],
        e_out_labels=[],
        e_extra_feats=["is_train", "is_val"],
        output_format = "PyG",
        shuffle=True,
        filter_by=None
    )
    for i, batch in enumerate(graph_loader):
        print("----Batch {}----".format(i))
        print(batch)
    ----
    --
    Output::
    +
    --
    ----
    ----Batch 0----
    Data(edge_index=[2, 1128], edge_feat=[1128], is_train=[1128], is_val=[1128], x=[1061, 1433], y=[1061], train_mask=[1061], val_mask=[1061], test_mask=[1061])
    ----Batch 1----
    Data(edge_index=[2, 997], edge_feat=[997], is_train=[997], is_val=[997], x=[1207, 1433], y=[1207], train_mask=[1207], val_mask=[1207], test_mask=[1207])
    ----Batch 2----
    Data(edge_index=[2, 1040], edge_feat=[1040], is_train=[1040], is_val=[1040], x=[1218, 1433], y=[1218], train_mask=[1218], val_mask=[1218], test_mask=[1218])
    ----Batch 3----
    Data(edge_index=[2, 1071], edge_feat=[1071], is_train=[1071], is_val=[1071], x=[1261, 1433], y=[1261], train_mask=[1261], val_mask=[1261], test_mask=[1261])
    ----Batch 4----
    Data(edge_index=[2, 1091], edge_feat=[1091], is_train=[1091], is_val=[1091], x=[1163, 1433], y=[1163], train_mask=[1163], val_mask=[1163], test_mask=[1163])
    ----Batch 5----
    Data(edge_index=[2, 1076], edge_feat=[1076], is_train=[1076], is_val=[1076], x=[1018, 1433], y=[1018], train_mask=[1018], val_mask=[1018], test_mask=[1018])
    ----Batch 6----
    Data(edge_index=[2, 1054], edge_feat=[1054], is_train=[1054], is_val=[1054], x=[1249, 1433], y=[1249], train_mask=[1249], val_mask=[1249], test_mask=[1249])
    ----Batch 7----
    Data(edge_index=[2, 1006], edge_feat=[1006], is_train=[1006], is_val=[1006], x=[1185, 1433], y=[1185], train_mask=[1185], val_mask=[1185], test_mask=[1185])
    ----Batch 8----
    Data(edge_index=[2, 1061], edge_feat=[1061], is_train=[1061], is_val=[1061], x=[1250, 1433], y=[1250], train_mask=[1250], val_mask=[1250], test_mask=[1250])
    ----Batch 9----
    Data(edge_index=[2, 1032], edge_feat=[1032], is_train=[1032], is_val=[1032], x=[1125, 1433], y=[1125], train_mask=[1125], val_mask=[1125], test_mask=[1125])
    ----
    --
    ====


    See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_graphloader.ipynb[the ML Workbench tutorial notebook for graph loaders]
         for examples.
    """
    def __init__(
        self,
        graph: "TigerGraphConnection",
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
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        kafka_security_protocol: str = "PLAINTEXT",
        kafka_sasl_mechanism: str = None,
        kafka_sasl_plain_username: str = None,
        kafka_sasl_plain_password: str = None,
        kafka_producer_ca_location: str = None,
        kafka_consumer_ca_location: str = None,
        timeout: int = 300000,
    ) -> None:
        """
        NO DOC
        """
        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            kafka_security_protocol,
            kafka_sasl_mechanism,
            kafka_sasl_plain_username,
            kafka_sasl_plain_password,
            kafka_producer_ca_location,
            kafka_consumer_ca_location,
            timeout,
        )
        # Resolve attributes
        is_hetero = any(map(lambda x: isinstance(x, dict), 
                        (v_in_feats, v_out_labels, v_extra_feats,
                         e_in_feats, e_out_labels, e_extra_feats)))
        self.is_hetero = is_hetero
        self.v_in_feats = self._validate_vertex_attributes(v_in_feats, is_hetero)
        self.v_out_labels = self._validate_vertex_attributes(v_out_labels, is_hetero)
        self.v_extra_feats = self._validate_vertex_attributes(v_extra_feats, is_hetero)
        self.e_in_feats = self._validate_edge_attributes(e_in_feats, is_hetero)
        self.e_out_labels = self._validate_edge_attributes(e_out_labels, is_hetero)
        self.e_extra_feats = self._validate_edge_attributes(e_extra_feats, is_hetero)
        if is_hetero:
            self._vtypes = list(
                    set(self.v_in_feats.keys())
                    | set(self.v_out_labels.keys())
                    | set(self.v_extra_feats.keys())
                )
            if not self._vtypes:
                self._vtypes = list(self._v_schema.keys())
            self._etypes = list(
                set(self.e_in_feats.keys())
                | set(self.e_out_labels.keys())
                | set(self.e_extra_feats.keys())
            )
            if not self._etypes:
                self._etypes = list(self._e_schema.keys())
        else:
            self._vtypes = list(self._v_schema.keys())
            self._etypes = list(self._e_schema.keys())
        # Initialize parameters for the query
        if batch_size:
            # If batch_size is given, calculate the number of batches
            if filter_by:
                # TODO: get edge count with filter
                raise NotImplementedError
            else:
                num_edges = sum(self._graph.getEdgeCount(i) for i in self._etypes)
            self.num_batches = math.ceil(num_edges / batch_size)
        else:
            # Otherwise, take the number of batches as is.
            self.num_batches = num_batches
        self._payload["num_batches"] = self.num_batches
        if filter_by:
            self._payload["filter_by"] = filter_by
        self._payload["shuffle"] = shuffle
        self._payload["v_types"] = self._vtypes
        self._payload["e_types"] = self._etypes
        # Output
        self.add_self_loop = add_self_loop
        # Install query
        self.query_name = self._install_query()

    def _install_query(self) -> str:
        # Install the right GSQL query for the loader.
        query_suffix = []
        query_replace = {}

        if isinstance(self.v_in_feats, dict):
            # Multiple vertex types
            print_query = ""
            for idx, vtype in enumerate(self._vtypes):
                v_attr_names = (
                    self.v_in_feats.get(vtype, [])
                    + self.v_out_labels.get(vtype, [])
                    + self.v_extra_feats.get(vtype, [])
                )
                query_suffix.extend(v_attr_names)
                v_attr_types = self._v_schema[vtype]
                if v_attr_names:
                    print_attr = '+","+'.join(
                        "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                        for attr in v_attr_names
                    )
                    print_query += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "," + {} + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype, print_attr)
                else:
                    print_query += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype)
            print_query += "END"
            query_replace["{VERTEXATTRS}"] = print_query
            # Multiple edge types
            print_query = ""
            for idx, etype in enumerate(self._etypes):
                e_attr_names = (
                    self.e_in_feats.get(etype, [])
                    + self.e_out_labels.get(etype, [])
                    + self.e_extra_feats.get(etype, [])
                )
                query_suffix.extend(e_attr_names)
                e_attr_types = self._e_schema[etype]
                if e_attr_names:
                    print_attr = '+","+'.join(
                        "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                        for attr in e_attr_names
                    )
                    print_query += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype, print_attr)
                else:
                    print_query += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype)
            print_query += "END"
            query_replace["{EDGEATTRS}"] = print_query
            query_suffix = list(dict.fromkeys(query_suffix))
        else:
            # Ignore vertex types
            v_attr_names = self.v_in_feats + self.v_out_labels + self.v_extra_feats
            query_suffix.extend(v_attr_names)
            v_attr_types = next(iter(self._v_schema.values()))
            if v_attr_names:
                print_attr = '+","+'.join(
                    "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                    for attr in v_attr_names
                )
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "," + {} + "\\n")'.format(
                    print_attr
                )
            else:
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "\\n")'
            query_replace["{VERTEXATTRS}"] = print_query
            # Ignore edge types
            e_attr_names = self.e_in_feats + self.e_out_labels + self.e_extra_feats
            query_suffix.extend(e_attr_names)
            e_attr_types = next(iter(self._e_schema.values()))
            if e_attr_names:
                print_attr = '+","+'.join(
                    "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                    for attr in e_attr_names
                )
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + "\\n")'.format(
                    print_attr
                )
            else:
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "\\n")'
            query_replace["{EDGEATTRS}"] = print_query
        query_replace["{QUERYSUFFIX}"] = "_".join(query_suffix)
        # Install query
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "graph_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size * 2)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        # Start requesting thread.
        if self.kafka_address_consumer:
            # If using kafka
            self._kafka_topic = "{}_{}".format(self.loader_id, self._iterations)
            self._payload["kafka_topic"] = self._kafka_topic
            self._requester = Thread(
                target=self._request_kafka,
                args=(
                    self._exit_event,
                    self._graph,
                    self.query_name,
                    self._kafka_consumer,
                    self._kafka_admin,
                    self._kafka_topic,
                    self.kafka_partitions,
                    self.kafka_replica,
                    self.max_kafka_msg_size,
                    self.kafka_retention_ms,
                    self.timeout,
                    self._payload,
                ),
            )
        else:
            # Otherwise, use rest api
            self._requester = Thread(
                target=self._request_rest,
                args=(
                    self._graph,
                    self.query_name,
                    self._read_task_q,
                    self.timeout,
                    self._payload,
                    "both",
                ),
            )
        self._requester.start()

        # If using Kafka, start downloading thread.
        if self.kafka_address_consumer:
            self._downloader = Thread(
                target=self._download_from_kafka,
                args=(
                    self._exit_event,
                    self._read_task_q,
                    self.num_batches,
                    True,
                    self._kafka_consumer,
                ),
            )
            self._downloader.start()

        # Start reading thread.
        if not self.is_hetero:
            v_attr_types = next(iter(self._v_schema.values()))
            e_attr_types = next(iter(self._e_schema.values()))
        else:
            v_attr_types = self._v_schema
            e_attr_types = self._e_schema
        self._reader = Thread(
            target=self._read_data,
            args=(
                self._exit_event,
                self._read_task_q,
                self._data_q,
                "graph",
                self.output_format,
                self.v_in_feats,
                self.v_out_labels,
                self.v_extra_feats,
                v_attr_types,
                self.e_in_feats,
                self.e_out_labels,
                self.e_extra_feats,
                e_attr_types,
                self.add_self_loop,
                True,
                self.is_hetero
            ),
        )
        self._reader.start()

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all data if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself"""
        return super().data


class EdgeNeighborLoader(BaseLoader):
    """EdgeNeighborLoader.

    A data loader that performs neighbor sampling from seed edges.
    You can declare a `EdgeNeighborLoader` instance with the factory function `edgeNeighborLoader()`.

    An edge neighbor loader is an iterable.
    When you loop through a loader instance, it loads one batch of data from the graph to which you established a connection.

    In every iteration, it first chooses a specified number of edges as seeds, 
    then starting from the vertices attached to those seed edges, it
    picks a specified number of neighbors of each vertex at random,
    then the same number of neighbors of each neighbor, and repeat for a specified number of hops.
    It loads both the vertices and the edges connecting them to their neighbors.
    The edges and vertices sampled this way form one subgraph and is contained in one batch.

    You can iterate on the instance until every edge has been picked as seed.

    Examples:

    The following example iterates over an edge neighbor loader instance.
    [.wrap,python]
    ----
    for i, batch in enumerate(edge_neighbor_loader):
        print("----Batch {}----".format(i))
        print(batch)
    ----



    See https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/1.0/tutorials/basics/3_edgeneighborloader.ipynb[the ML Workbench tutorial notebook]
        for examples.
    """
    def __init__(
        self,
        graph: "TigerGraphConnection",
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
        filter_by: Union[str, dict] = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        reverse_edge: bool = False,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        kafka_security_protocol: str = "PLAINTEXT",
        kafka_sasl_mechanism: str = None,
        kafka_sasl_plain_username: str = None,
        kafka_sasl_plain_password: str = None,
        kafka_producer_ca_location: str = None,
        kafka_consumer_ca_location: str = None,
        timeout: int = 300000,
    ) -> None:
        """NO DOC"""

        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            kafka_security_protocol,
            kafka_sasl_mechanism,
            kafka_sasl_plain_username,
            kafka_sasl_plain_password,
            kafka_producer_ca_location,
            kafka_consumer_ca_location,
            timeout,
        )
        # Resolve attributes
        is_hetero = any(map(lambda x: isinstance(x, dict), 
                        (v_in_feats, v_out_labels, v_extra_feats,
                         e_in_feats, e_out_labels, e_extra_feats)))
        self.is_hetero = is_hetero
        self.v_in_feats = self._validate_vertex_attributes(v_in_feats, is_hetero)
        self.v_out_labels = self._validate_vertex_attributes(v_out_labels, is_hetero)
        self.v_extra_feats = self._validate_vertex_attributes(v_extra_feats, is_hetero)
        self.e_in_feats = self._validate_edge_attributes(e_in_feats, is_hetero)
        self.e_out_labels = self._validate_edge_attributes(e_out_labels, is_hetero)
        self.e_extra_feats = self._validate_edge_attributes(e_extra_feats, is_hetero)
        if is_hetero:
            self._vtypes = list(
                    set(self.v_in_feats.keys())
                    | set(self.v_out_labels.keys())
                    | set(self.v_extra_feats.keys())
                )
            if not self._vtypes:
                self._vtypes = list(self._v_schema.keys())
            self._etypes = list(
                set(self.e_in_feats.keys())
                | set(self.e_out_labels.keys())
                | set(self.e_extra_feats.keys())
            )
            if not self._etypes:
                self._etypes = list(self._e_schema.keys())
        else:
            self._vtypes = list(self._v_schema.keys())
            self._etypes = list(self._e_schema.keys())
        # Resolve seeds
        self._seed_types = self._etypes if ((not filter_by) or isinstance(filter_by, str)) else list(filter_by.keys())
        # Resolve number of batches
        if batch_size:
            # If batch_size is given, calculate the number of batches
            if filter_by:
                # TODO: get edge count with filter
                raise NotImplementedError("Cannot specify batch_size and filter_by at the same time. Please use num_batches and filter_by.")
            else:
                num_edges = sum(self._graph.getEdgeCount(i) for i in self._etypes)
            self.num_batches = math.ceil(num_edges / batch_size)
        else:
            # Otherwise, take the number of batches as is.
            self.num_batches = num_batches
        # Initialize parameters for the query
        self._payload["num_batches"] = self.num_batches
        self._payload["num_neighbors"] = num_neighbors
        self._payload["num_hops"] = num_hops
        if filter_by:
            if isinstance(filter_by, str):
                self._payload["filter_by"] = filter_by
            else:
                attr = set(filter_by.values())
                if len(attr) != 1:
                    raise NotImplementedError("Filtering by different attributes for different edge types is not supported. Please use the same attribute for different types.")
                self._payload["filter_by"] = attr.pop()
        self._payload["shuffle"] = shuffle
        self._payload["v_types"] = self._vtypes
        self._payload["e_types"] = self._etypes
        self._payload["seed_types"] = self._seed_types
        # Output
        self.add_self_loop = add_self_loop
        # Install query
        self.query_name = self._install_query()

    def _install_query(self):
        # Install the right GSQL query for the loader.
        query_suffix = []
        query_replace = {}

        if self.is_hetero:
            # Multiple vertex types
            print_query = ""
            for idx, vtype in enumerate(self._vtypes):
                v_attr_names = (
                    self.v_in_feats.get(vtype, [])
                    + self.v_out_labels.get(vtype, [])
                    + self.v_extra_feats.get(vtype, [])
                )
                query_suffix.extend(v_attr_names)
                v_attr_types = self._v_schema[vtype]
                if v_attr_names:
                    print_attr = '+","+'.join(
                        "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                        for attr in v_attr_names
                    )
                    print_query += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "," + {} + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype, print_attr)
                else:
                    print_query += '{} s.type == "{}" THEN \n @@v_batch += (s.type + "," + int_to_string(getvid(s)) + "\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", vtype)
            print_query += "END"
            query_replace["{VERTEXATTRS}"] = print_query
            # Multiple edge types
            print_query_seed = ""
            print_query_other = ""
            for idx, etype in enumerate(self._etypes):
                e_attr_names = (
                    self.e_in_feats.get(etype, [])
                    + self.e_out_labels.get(etype, [])
                    + self.e_extra_feats.get(etype, [])
                )
                query_suffix.extend(e_attr_names)
                e_attr_types = self._e_schema[etype]
                if e_attr_names:
                    print_attr = '+","+'.join(
                        "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                        for attr in e_attr_names
                    )
                    print_query_seed += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + ",1\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype, print_attr)
                    print_query_other += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + ",0\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype, print_attr)
                else:
                    print_query_seed += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + ",1\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype)
                    print_query_other += '{} e.type == "{}" THEN \n @@e_batch += (e.type + "," + int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + ",0\\n")\n'.format(
                            "IF" if idx==0 else "ELSE IF", etype)
            print_query_seed += "END"
            print_query_other += "END"
            query_replace["{SEEDEDGEATTRS}"] = print_query_seed
            query_replace["{OTHEREDGEATTRS}"] = print_query_other
            query_suffix = list(dict.fromkeys(query_suffix))
        else:
            # Ignore vertex types
            v_attr_names = self.v_in_feats + self.v_out_labels + self.v_extra_feats
            query_suffix.extend(v_attr_names)
            v_attr_types = next(iter(self._v_schema.values()))
            if v_attr_names:
                print_attr = '+","+'.join(
                    "{}(s.{})".format('' if v_attr_types[attr]=="STRING" else _udf_funcs[v_attr_types[attr]], attr)
                    for attr in v_attr_names
                )
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "," + {} + "\\n")'.format(
                    print_attr
                )
                query_replace["{VERTEXATTRS}"] = print_query
            else:
                print_query = '@@v_batch += (int_to_string(getvid(s)) + "\\n")'
                query_replace["{VERTEXATTRS}"] = print_query
            # Ignore edge types
            e_attr_names = self.e_in_feats + self.e_out_labels + self.e_extra_feats
            query_suffix.extend(e_attr_names)
            e_attr_types = next(iter(self._e_schema.values()))
            if e_attr_names:
                print_attr = '+","+'.join(
                    "{}(e.{})".format('' if e_attr_types[attr]=="STRING" else _udf_funcs[e_attr_types[attr]], attr)
                    for attr in e_attr_names
                )
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + ",1\\n")'.format(
                    print_attr
                )
                query_replace["{SEEDEDGEATTRS}"] = print_query
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + "," + {} + ",0\\n")'.format(
                    print_attr
                )
                query_replace["{OTHEREDGEATTRS}"] = print_query
            else:
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + ",1\\n")'
                query_replace["{SEEDEDGEATTRS}"] = print_query
                print_query = '@@e_batch += (int_to_string(getvid(s)) + "," + int_to_string(getvid(t)) + ",0\\n")'
                query_replace["{OTHEREDGEATTRS}"] = print_query
        query_replace["{QUERYSUFFIX}"] = "_".join(query_suffix)
        # Install query
        query_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "gsql",
                "dataloaders",
                "edge_nei_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size * 2)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        # Start requesting thread.
        if self.kafka_address_consumer:
            # If using kafka
            self._kafka_topic = "{}_{}".format(self.loader_id, self._iterations)
            self._payload["kafka_topic"] = self._kafka_topic
            self._requester = Thread(
                target=self._request_kafka,
                args=(
                    self._exit_event,
                    self._graph,
                    self.query_name,
                    self._kafka_consumer,
                    self._kafka_admin,
                    self._kafka_topic,
                    self.kafka_partitions,
                    self.kafka_replica,
                    self.max_kafka_msg_size,
                    self.kafka_retention_ms,
                    self.timeout,
                    self._payload,
                ),
            )
        else:
            # Otherwise, use rest api
            self._requester = Thread(
                target=self._request_rest,
                args=(
                    self._graph,
                    self.query_name,
                    self._read_task_q,
                    self.timeout,
                    self._payload,
                    "both",
                ),
            )
        self._requester.start()

        # If using Kafka, start downloading thread.
        if self.kafka_address_consumer:
            self._downloader = Thread(
                target=self._download_from_kafka,
                args=(
                    self._exit_event,
                    self._read_task_q,
                    self.num_batches,
                    True,
                    self._kafka_consumer,
                ),
            )
            self._downloader.start()

        # Start reading thread.
        if not self.is_hetero:
            e_extra_feats = self.e_extra_feats + ["is_seed"]
            e_attr_types = next(iter(self._e_schema.values()))
            e_attr_types["is_seed"] = "bool"
            v_attr_types = next(iter(self._v_schema.values()))
        else:
            e_extra_feats = {}
            for etype in self._etypes:
                e_extra_feats[etype] = self.e_extra_feats.get(etype, []) + ["is_seed"]
            e_attr_types = self._e_schema
            for etype in e_attr_types:
                e_attr_types[etype]["is_seed"] = "bool"
            v_attr_types = self._v_schema
        self._reader = Thread(
            target=self._read_data,
            args=(
                self._exit_event,
                self._read_task_q,
                self._data_q,
                "graph",
                self.output_format,
                self.v_in_feats,
                self.v_out_labels,
                self.v_extra_feats,
                v_attr_types,
                self.e_in_feats,
                self.e_out_labels,
                e_extra_feats,
                e_attr_types,
                self.add_self_loop,
                True,
                self.is_hetero
            ),
        )
        self._reader.start()

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all data if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself"""
        return super().data
