"""Data Loaders
:description: Data loader classes in the pyTigerGraph GDS module. 

Data loaders are classes in the pyTigerGraph Graph Data Science (GDS) module. 
You can define an instance of each data loader class through a link:https://docs.tigergraph.com/pytigergraph/current/gds/factory-functions[factory function].

Requires `querywriters` user permissions for full functionality. 
"""

import hashlib
import json
import logging
import warnings
import math
import os
from collections import defaultdict
from queue import Empty, Queue
from threading import Event, Thread
from time import sleep
import pickle
import random
from typing import TYPE_CHECKING, Any, Iterator, NoReturn, Tuple, Union, Callable, List, Dict
#import re

#RE_SPLITTER = re.compile(r',(?![^\[]*\])')

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection
    from kafka import KafkaAdminClient, KafkaConsumer
    import torch
    import dgl
    import tensorflow as tf
    import spektral
    import scipy
    import torch_geometric as pyg
    from typing import Literal

import numpy as np
import pandas as pd

from ..pyTigerGraphException import TigerGraphException
from .utilities import install_query_file, random_string, add_attribute, install_query_files

__all__ = ["VertexLoader", "EdgeLoader", "NeighborLoader", "GraphLoader", "EdgeNeighborLoader", "NodePieceLoader", "HGTLoader"]

RANDOM_TOPIC_LEN = 8
logger = logging.getLogger(__name__)

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
        delimiter: str = "|",
        timeout: int = 300000,
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None,
        num_machines: int = 1,
        num_segments: int = 20,
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
            loader_id (str):
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
            delimiter (str, optional):
                What character (or combination of characters) to use to separate attributes as batches are being created.
                Defaults to ",".
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
            distributed_query (bool, optional):
                Whether to install the query in distributed mode. Defaults to False.
            kafka_address (str):
                Address of the Kafka broker. Defaults to localhost:9092.
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
            kafka_skip_produce (bool, optional):
                Whether or not to skip calling the producer.
            kafka_auto_offset_reset (str, optional):
                Where to start for a new consumer. "earliest" will move to the oldest available message, 
                "latest" will move to the most recent. Any other value will raise the exception.
                Defaults to "earliest".
            kafka_del_topic_per_epoch (bool, optional): 
                Whether to delete the topic after each epoch. It is effective only when
                `kafka_add_topic_per_epoch` is True. Defaults to False.
            kafka_add_topic_per_epoch (bool, optional):  
                Whether to add a topic for each epoch. Defaults to False.
            callback_fn (callable, optional):
                A callable function to apply to each batch in the dataloader. Defaults to None.
            kafka_group_id (str, optional):
                Consumer group ID if joining a consumer group for dynamic partition assignment and offset commits. Defaults to None.
        """
        # Thread to send requests, download and load data
        self._requester = None
        self._downloader = None
        self._reader = None
        # Queues to store tasks and data
        self._read_task_q = None
        self._data_q = None
        self._kafka_topic = None
        self._all_kafka_topics = set()
        # Exit signal to terminate threads
        self._exit_event = None
        # In-memory data cache. Only used if num_batches=1
        self._data = None
        # Kafka topic configs
        self._kafka_admin = None
        self._kafka_consumer = None
        self.kafka_partitions = kafka_num_partitions
        self.kafka_replica = kafka_replica_factor
        self.kafka_retention_ms = kafka_retention_ms
        if kafka_auto_del_topic is None:
            if kafka_topic:
                self.delete_all_topics = False
            else:
                self.delete_all_topics = True
        else:
            self.delete_all_topics = kafka_auto_del_topic
        self.kafka_skip_produce = kafka_skip_produce
        self.add_epoch_topic = kafka_add_topic_per_epoch
        if self.add_epoch_topic:
            self.delete_epoch_topic = kafka_del_topic_per_epoch
        else:
            self.delete_epoch_topic = False
        # Get graph info
        self.delimiter = delimiter # has to be set before self._get_schema()
        self.reverse_edge = reverse_edge
        self._graph = graph
        self._v_schema, self._e_schema = self._get_schema()
        # Initialize basic params
        if loader_id:
            self.loader_id = loader_id
        else:
            self.loader_id = "tg_" + random_string(RANDOM_TOPIC_LEN)
        self.kafka_group_id = kafka_group_id
        if kafka_topic:
            self._kafka_topic_base = kafka_topic
        elif kafka_group_id:
            self._kafka_topic_base = kafka_group_id + "_topic"
        else:
            self._kafka_topic_base = self.loader_id + "_topic"
        self.num_batches = num_batches
        self.output_format = output_format.lower()
        self.buffer_size = buffer_size
        self.timeout = timeout
        self._iterations = 0
        self._iterator = False
        self.callback_fn = callback_fn
        self.distributed_query = distributed_query
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
                    auto_offset_reset=kafka_auto_offset_reset,
                    security_protocol=kafka_security_protocol,
                    sasl_mechanism=kafka_sasl_mechanism,
                    sasl_plain_username=kafka_sasl_plain_username,
                    sasl_plain_password=kafka_sasl_plain_password,
                    ssl_cafile=kafka_consumer_ca_location,
                    ssl_check_hostname=kafka_ssl_check_hostname,
                    ssl_certfile=kafka_consumer_certificate_location,
                    ssl_keyfile=kafka_consumer_key_location,
                    ssl_password=kafka_consumer_key_password,
                    sasl_kerberos_service_name=kafka_sasl_kerberos_service_name,
                    sasl_kerberos_domain_name=kafka_sasl_kerberos_domain_name,
                    group_id=self.kafka_group_id
                )
                self._kafka_admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_address_consumer,
                    client_id=self.loader_id,
                    security_protocol=kafka_security_protocol,
                    sasl_mechanism=kafka_sasl_mechanism,
                    sasl_plain_username=kafka_sasl_plain_username,
                    sasl_plain_password=kafka_sasl_plain_password,
                    ssl_cafile=kafka_consumer_ca_location,
                    ssl_check_hostname=kafka_ssl_check_hostname,
                    ssl_certfile=kafka_consumer_certificate_location,
                    ssl_keyfile=kafka_consumer_key_location,
                    ssl_password=kafka_consumer_key_password,
                    sasl_kerberos_service_name=kafka_sasl_kerberos_service_name,
                    sasl_kerberos_domain_name=kafka_sasl_kerberos_domain_name
                )
            except:
                raise ConnectionError(
                    "Cannot reach Kafka broker. Please check Kafka settings."
                )
        # Initialize parameters for the query
        self._payload = {}
        self._payload["num_machines"] = num_machines
        self._payload["num_segments"] = num_segments
        if self.kafka_address_producer:
            self._payload["kafka_address"] = self.kafka_address_producer
            self._payload["kafka_topic_partitions"] = kafka_num_partitions
            self._payload["kafka_max_size"] = str(Kafka_max_msg_size)
            self._payload["kafka_timeout"] = self.timeout
            if kafka_security_protocol == "PLAINTEXT":
                pass
            elif kafka_security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
                self._payload["security_protocol"] = kafka_security_protocol
                self._payload["sasl_mechanism"] = kafka_sasl_mechanism
                if kafka_sasl_mechanism == "PLAIN":
                    if kafka_sasl_plain_username and kafka_sasl_plain_password:
                        self._payload["sasl_username"] = kafka_sasl_plain_username
                        self._payload["sasl_password"] = kafka_sasl_plain_password
                    else:
                        raise ValueError("Please provide kafka_sasl_plain_username and kafka_sasl_plain_password for Kafka.")
                elif kafka_sasl_mechanism == "GSSAPI":
                    if kafka_sasl_kerberos_service_name:
                        self._payload["sasl_kerberos_service_name"] = kafka_sasl_kerberos_service_name
                    else:
                        raise ValueError("Please provide Kerberos service name for Kafka.")
                    if kafka_sasl_kerberos_keytab:
                        self._payload["sasl_kerberos_keytab"] = kafka_sasl_kerberos_keytab
                    if kafka_sasl_kerberos_principal:
                        self._payload["sasl_kerberos_principal"] = kafka_sasl_kerberos_principal
                else:
                    raise NotImplementedError("Only PLAIN and GSSAPI mechanisms are supported for SASL.")
            elif kafka_security_protocol == "SSL":
                self._payload["security_protocol"] = kafka_security_protocol
            else:
                raise NotImplementedError("Only PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, and SSL are supported as Kafka security protocol.")
            if kafka_security_protocol in ("SSL", "SASL_SSL"):
                if kafka_producer_ca_location:
                    self._payload["ssl_ca_location"] = kafka_producer_ca_location
                if kafka_producer_certificate_location:
                    self._payload["ssl_certificate_location"] = kafka_producer_certificate_location
                if kafka_producer_key_location:
                    self._payload["ssl_key_location"] = kafka_producer_key_location
                if kafka_producer_key_password:
                    self._payload["ssl_key_password"] = kafka_producer_key_password
                if kafka_ssl_check_hostname:
                    self._payload["ssl_endpoint_identification_algorithm"] = "https"
            # kafka_topic will be filled in later.
        # Check ml workbench compatibility
        self._validate_mlwb_version()
        # Implement `_install_query()` that installs your query
        # self._install_query()

    def __del__(self) -> NoReturn:
        self._reset(theend=True)

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
                elif attr["AttributeType"]["Name"] == "MAP" and self.delimiter == ",":
                    raise TigerGraphException("MAP data types are not supported with the comma delimiter")
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

    def _validate_mlwb_version(self) -> None:
        mlwb = self._graph.getUDF()
        if ("init_kafka_producer" not in mlwb[0]) or ("class KafkaProducer" not in mlwb[1]):
            raise TigerGraphException("ML Workbench version incompatible. Please reactivate database with the activator whose version matches your pyTigerGraph's. See https://act.tigergraphlabs.com for details.")

    def _install_query(self) -> NoReturn:
        # Install the right GSQL query for the loader.
        self.query_name = ""
        raise NotImplementedError

    def _set_kafka_topic(self) -> None:
        # Generate kafka topic, add it to payload and consumer
        # Generate topic
        if self.add_epoch_topic:
            kafka_topic = "{}_{}".format(self._kafka_topic_base, self._iterations)
        else:
            kafka_topic = self._kafka_topic_base
        self._kafka_topic = kafka_topic
        self._payload["kafka_topic"] = kafka_topic
        self._all_kafka_topics.add(kafka_topic)
        # Create topic if not exist
        if (kafka_topic not in self._kafka_admin.list_topics()) and (self.kafka_skip_produce is False):
            try:
                from kafka.admin import NewTopic
            except ImportError:
                raise ImportError(
                    "kafka-python is not installed. Please install it to use kafka streaming."
                )
            new_topic = NewTopic(
                kafka_topic,
                self.kafka_partitions,
                self.kafka_replica,
                topic_configs={
                    "retention.ms": str(self.kafka_retention_ms),
                    "max.message.bytes": str(self.max_kafka_msg_size),
                },
            )
            resp = self._kafka_admin.create_topics([new_topic])
            if resp.to_object()["topic_errors"][0]["error_code"] != 0:
                raise ConnectionError(
                    "Failed to create Kafka topic {} at {}.".format(
                        kafka_topic, self._kafka_consumer.config["bootstrap_servers"]
                    )
                )
        else:
            # Topic exists. This means there might be data in kafka already. Skip 
            # calling producer as default unless explicitely set otherwise.
            if self.kafka_skip_produce is None:
                self.kafka_skip_produce = True
        # Double check the topic is fully created
        while not self._kafka_admin.describe_topics([kafka_topic])[0]["partitions"]:
            sleep(1)
        # Subscribe to the topic
        if (not self._kafka_consumer.subscription()) or kafka_topic not in self._kafka_consumer.subscription():
            self._kafka_consumer.subscribe([kafka_topic])
            _ = self._kafka_consumer.topics() # Call this to refresh metadata. Or the new subscription seems to be delayed.

    @staticmethod
    def _request_kafka(
        exit_event: Event,
        tgraph: "TigerGraphConnection",
        query_name: str,
        timeout: int = 600000,
        payload: dict = {},
        headers: dict = {},
    ) -> NoReturn:
        # Run query async
        _payload = {}
        _payload.update(payload)
        resp = tgraph.runInstalledQuery(query_name, params=_payload, timeout=timeout, usePost=True, runAsync=True)
        while not exit_event.is_set():
            # Check status
            status = tgraph.checkQueryStatus(resp)
            if status[0]["status"] == "running":
                sleep(1)
                continue
            elif status[0]["status"] == "success":
                res = tgraph.getQueryResult(resp)
                if res[0]["kafkaError"]:
                    raise TigerGraphException(
                        "Kafka Error: {}".format(res[0]["kafkaError"])
                    )
                else:
                    break
            else:
                raise TigerGraphException(
                    "Error generating data. {}".format(
                        status
                    )
                )
        # exiting
        tgraph.abortQuery(resp)

    @staticmethod
    def _request_graph_rest(
        tgraph: "TigerGraphConnection",
        query_name: str,
        read_task_q: Queue,
        timeout: int = 600000,
        payload: dict = {},
    ) -> NoReturn:
        # Run query
        resp = tgraph.runInstalledQuery(
            query_name, params=payload, timeout=timeout, usePost=True
        )
        # Put raw data into reading queue. 
        for i in resp:
            read_task_q.put((i["vertex_batch"], i["edge_batch"], i["seed"]))
        read_task_q.put(None)

    @staticmethod
    def _request_unimode_rest(
        tgraph: "TigerGraphConnection",
        query_name: str,
        read_task_q: Queue,
        timeout: int = 600000,
        payload: dict = {},
    ) -> NoReturn:
        # Run query
        #TODO: check what happens when the query times out
        resp = tgraph.runInstalledQuery(
            query_name, params=payload, timeout=timeout, usePost=True
        )
        # Put raw data into reading queue
        for i in resp:
            read_task_q.put(i["data_batch"])
        read_task_q.put(None)

    @staticmethod
    def _download_graph_kafka(
        exit_event: Event,
        read_task_q: Queue,
        kafka_consumer: "KafkaConsumer"
    ) -> NoReturn:
        empty = False
        buffer = {}
        while (not exit_event.is_set()) and (not empty):
            resp = kafka_consumer.poll(1000)
            if not resp:
                continue
            for msgs in resp.values():
                for message in msgs:
                    key = message.key.decode("utf-8")
                    if key == "STOP":
                        read_task_q.put(None)
                        empty = True
                        break
                    if key.startswith("vertex"):
                        companion_key = key.replace("vertex", "edge")
                        if companion_key in buffer:
                            read_task_q.put((message.value.decode("utf-8"), 
                                             buffer[companion_key],
                                             key.split("_", 2)[-1]))
                            del buffer[companion_key]
                        else:
                            buffer[key] = message.value.decode("utf-8")
                    elif key.startswith("edge"):
                        companion_key = key.replace("edge", "vertex")
                        if companion_key in buffer:
                            read_task_q.put((buffer[companion_key], 
                                             message.value.decode("utf-8"),
                                             key.split("_", 2)[-1]))
                            del buffer[companion_key]
                        else:
                            buffer[key] = message.value.decode("utf-8")
                    else:
                        warnings.warn(
                            "Unrecognized key {} for messages in kafka".format(key)
                        )
                if empty:
                    break

    @staticmethod
    def _download_unimode_kafka(
        exit_event: Event,
        read_task_q: Queue,
        kafka_consumer: "KafkaConsumer"
    ) -> NoReturn:
        empty = False
        while (not exit_event.is_set()) and (not empty):
            resp = kafka_consumer.poll(1000)
            if not resp:
                continue
            for msgs in resp.values():
                for message in msgs:
                    key = message.key.decode("utf-8")
                    if key == "STOP":
                        read_task_q.put(None)
                        empty = True
                    else:
                        read_task_q.put(message.value.decode("utf-8"))

    @staticmethod
    def _read_graph_data(
        exit_event: Event,
        in_q: Queue,
        out_q: Queue,
        batch_size: int,
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
        delimiter: str = "|",
        is_hetero: bool = False,
        callback_fn: Callable = None,
        seed_type: str = ""
    ) -> NoReturn:
        # Import the right libraries based on output format
        out_format = out_format.lower()
        if out_format == "pyg" or out_format == "dgl":
            try:
                import torch
            except ImportError:
                raise ImportError(
                    "PyTorch is not installed. Please install it to use PyG or DGL output."
                )
            if out_format == "dgl":
                try:
                    import dgl
                except ImportError:
                    raise ImportError(
                        "DGL is not installed. Please install DGL to use DGL format."
                    )
            elif out_format == "pyg":
                try:
                    import torch_geometric as pyg
                except ImportError:
                    raise ImportError(
                        "PyG is not installed. Please install PyG to use PyG format."
                    )
        elif out_format.lower() == "spektral":
            try:
                import tensorflow as tf
            except ImportError:
                raise ImportError(
                    "Tensorflow is not installed. Please install it to use spektral output."
                )
            try:
                import scipy
            except ImportError:
                raise ImportError(
                    "scipy is not installed. Please install it to use spektral output."
                )
            try:
                import spektral
            except ImportError:
                raise ImportError(
                    "Spektral is not installed. Please install it to use spektral output."
                )
        # Get raw data from queue and parse 
        vertex_buffer = dict()
        edge_buffer = dict()
        buffer_size = 0
        seeds = set()
        is_empty = False
        last_batch = False
        while (not exit_event.is_set()) and (not is_empty):
            try:
                raw = in_q.get(timeout=1)
            except Empty:
                continue
            if raw is None:
                is_empty = True
                if buffer_size > 0:
                    last_batch = True
            else:          
                vertex_buffer.update({i.strip():"" for i in raw[0].strip().splitlines()})
                edge_buffer.update({i.strip():"" for i in raw[1].strip().splitlines()})
                seeds.add(raw[2])
                buffer_size += 1
            if (buffer_size < batch_size) and (not last_batch):
                continue
            try:
                if seed_type:
                    raw_data = (vertex_buffer.keys(), edge_buffer.keys(), seeds)
                else:
                    raw_data = (vertex_buffer.keys(), edge_buffer.keys())
                data = BaseLoader._parse_graph_data_to_df(
                    raw = raw_data,
                    v_in_feats = v_in_feats,
                    v_out_labels = v_out_labels,
                    v_extra_feats = v_extra_feats,
                    v_attr_types = v_attr_types,
                    e_in_feats = e_in_feats,
                    e_out_labels = e_out_labels,
                    e_extra_feats = e_extra_feats,
                    e_attr_types = e_attr_types,
                    delimiter = delimiter,
                    primary_id = {},
                    is_hetero = is_hetero,
                    seed_type = seed_type
                )
                if out_format == "dataframe" or out_format == "df":
                    vertices, edges = data
                    if not is_hetero:
                        for column in vertices.columns:
                            vertices[column] = pd.to_numeric(vertices[column], errors="ignore")
                        for column in edges.columns:
                            edges[column] = pd.to_numeric(edges[column], errors="ignore")
                    else:
                        for key in vertices:
                            for column in vertices[key].columns:
                                vertices[key][column] = pd.to_numeric(vertices[key][column], errors="ignore")
                        for key in edges:
                            for column in edges[key].columns:
                                edges[key][column] = pd.to_numeric(edges[key][column], errors="ignore")
                    data = (vertices, edges)
                elif out_format == "pyg":
                    data = BaseLoader._parse_df_to_pyg(
                        raw = data,
                        v_in_feats = v_in_feats,
                        v_out_labels = v_out_labels,
                        v_extra_feats = v_extra_feats,
                        v_attr_types = v_attr_types,
                        e_in_feats = e_in_feats,
                        e_out_labels = e_out_labels,
                        e_extra_feats = e_extra_feats,
                        e_attr_types = e_attr_types,
                        add_self_loop = add_self_loop,
                        is_hetero = is_hetero,
                        torch = torch,
                        pyg = pyg
                   )
                elif out_format == "dgl":
                    data = BaseLoader._parse_df_to_dgl(
                        raw = data,
                        v_in_feats = v_in_feats,
                        v_out_labels = v_out_labels,
                        v_extra_feats = v_extra_feats,
                        v_attr_types = v_attr_types,
                        e_in_feats = e_in_feats,
                        e_out_labels = e_out_labels,
                        e_extra_feats = e_extra_feats,
                        e_attr_types = e_attr_types,
                        add_self_loop = add_self_loop,
                        is_hetero = is_hetero,
                        torch = torch,
                        dgl= dgl
                    )
                elif out_format == "spektral" and is_hetero==False:
                    data = BaseLoader._parse_df_to_spektral(
                        raw = data,
                        v_in_feats = v_in_feats,
                        v_out_labels = v_out_labels,
                        v_extra_feats = v_extra_feats,
                        v_attr_types = v_attr_types,
                        e_in_feats = e_in_feats,
                        e_out_labels = e_out_labels,
                        e_extra_feats = e_extra_feats,
                        e_attr_types = e_attr_types,
                        add_self_loop = add_self_loop,
                        is_hetero = is_hetero,
                        scipy = scipy,
                        spektral = spektral
                    )
                else:
                    raise NotImplementedError
                if callback_fn:
                    data = callback_fn(data)
                out_q.put(data)
            except Exception as err:
                warnings.warn("Error parsing a graph batch. Set logging level to ERROR for details.")
                logger.error(err, exc_info=True)
                logger.error("Error parsing data: {}".format((vertex_buffer, edge_buffer)))
                logger.error("Parameters:\n  out_format={}\n  v_in_feats={}\n  v_out_labels={}\n  v_extra_feats={}\n  v_attr_types={}\n  e_in_feats={}\n  e_out_labels={}\n  e_extra_feats={}\n  e_attr_types={}\n  delimiter={}\n".format(
                    out_format, v_in_feats, v_out_labels, v_extra_feats, v_attr_types, e_in_feats, e_out_labels, e_extra_feats, e_attr_types, delimiter))
            vertex_buffer.clear()
            edge_buffer.clear()
            seeds.clear()
            buffer_size = 0
        out_q.put(None)
            
    @staticmethod
    def _read_vertex_data(
        exit_event: Event,
        in_q: Queue,
        out_q: Queue,
        batch_size: int,
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        delimiter: str = "|",
        is_hetero: bool = False,
        callback_fn: Callable = None
    ) -> NoReturn:
        buffer = []
        last_batch = False
        is_empty = False
        while (not exit_event.is_set()) and (not is_empty):
            try:
                raw = in_q.get(timeout=1)
            except Empty:
                continue
            if raw is None:
                is_empty = True
                if len(buffer) > 0:
                    last_batch = True
            else:
                buffer.append(raw)
            if (len(buffer) < batch_size) and (not last_batch):
                continue
            try:
                data = BaseLoader._parse_vertex_data(
                    raw = buffer,
                    v_in_feats = v_in_feats,
                    v_out_labels = v_out_labels,
                    v_extra_feats = v_extra_feats,
                    v_attr_types = v_attr_types,
                    delimiter = delimiter,
                    is_hetero = is_hetero
                )
                if not is_hetero:
                    for column in data.columns:
                        data[column] = pd.to_numeric(data[column], errors="ignore")
                else:
                    for key in data:
                        for column in data[key].columns:
                            data[key][column] = pd.to_numeric(data[key][column], errors="ignore")
                if callback_fn:
                    data = callback_fn(data)
                out_q.put(data)
            except Exception as err:
                warnings.warn("Error parsing a vertex batch. Set logging level to ERROR for details.")
                logger.error(err, exc_info=True)
                logger.error("Error parsing data: {}".format(buffer))
                logger.error("Parameters:\n  v_in_feats={}\n  v_out_labels={}\n  v_extra_feats={}\n  v_attr_types={}\n  delimiter={}\n".format(
                    v_in_feats, v_out_labels, v_extra_feats, v_attr_types, delimiter))
            buffer.clear()
        out_q.put(None)
                
    @staticmethod
    def _read_edge_data(
        exit_event: Event,
        in_q: Queue,
        out_q: Queue,
        batch_size: int,
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        delimiter: str = "|",
        is_hetero: bool = False,
        callback_fn: Callable = None
    ) -> NoReturn:
        buffer = []
        is_empty = False
        last_batch = False
        while (not exit_event.is_set()) and (not is_empty):
            try:
                raw = in_q.get(timeout=1)
            except Empty:
                continue
            if raw is None:
                is_empty = True
                if len(buffer) > 0:
                    last_batch = True
            else:
                buffer.append(raw)
            if (len(buffer) < batch_size) and (not last_batch):
                continue
            try:
                data = BaseLoader._parse_edge_data(
                    raw = buffer,
                    e_in_feats = e_in_feats,
                    e_out_labels = e_out_labels,
                    e_extra_feats = e_extra_feats,
                    e_attr_types = e_attr_types,
                    delimiter = delimiter,
                    is_hetero = is_hetero
                )
                if not is_hetero:
                    for column in data.columns:
                        data[column] = pd.to_numeric(data[column], errors="ignore")
                else:
                    for key in data:
                        for column in data[key].columns:
                            data[key][column] = pd.to_numeric(data[key][column], errors="ignore")
                if callback_fn:
                    data = callback_fn(data)
                out_q.put(data)
            except Exception as err:
                warnings.warn("Error parsing an edge batch. Set logging level to ERROR for details.")
                logger.error(err, exc_info=True)
                logger.error("Error parsing data: {}".format(buffer))
                logger.error("Parameters:\n  e_in_feats={}\n  e_out_labels={}\n  e_extra_feats={}\n  e_attr_types={}\n  delimiter={}\n".format(
                    e_in_feats, e_out_labels, e_extra_feats, e_attr_types, delimiter))
            buffer.clear()
        out_q.put(None)

    @staticmethod
    def _parse_vertex_data(
        raw: List[str],
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        delimiter: str = "|",
        is_hetero: bool = False,
        seeds: list = []
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Parse raw vertex data into dataframes.
        """    
        # Read in vertex CSVs as dataframes
        # Each row is in format vid,v_in_feats,v_out_labels,v_extra_feats
        # or vtype,vid,v_in_feats,v_out_labels,v_extra_feats
        v_file = (line.split(delimiter) for line in raw)
        # If seeds are given, create the is_seed column
        if seeds:
            seed_df = pd.DataFrame({
                "vid": list(seeds),
                "is_seed": True
            })
        if not is_hetero:
            # String of vertices in format vid,v_in_feats,v_out_labels,v_extra_feats
            v_attributes = ["vid"] + v_in_feats + v_out_labels + v_extra_feats
            if seeds:
                try:
                    v_attributes.remove("is_seed")
                except ValueError:
                    pass
            data = pd.DataFrame(v_file, columns=v_attributes, dtype="object")
            for v_attr in v_extra_feats:
                if v_attr_types.get(v_attr, "") == "MAP":
                    # I am sorry that this is this ugly...
                    data[v_attr] = data[v_attr].apply(lambda x: {y.split(",")[0].strip("("): y.split(",")[1].strip(")") for y in x.strip("[").strip("]").split(" ")[:-1]} if x != "[]" else {})
            if seeds:
                data = data.merge(seed_df, on="vid", how="left")
                data.fillna({"is_seed": False}, inplace=True)
        else:
            v_file_dict = defaultdict(list)
            for line in v_file:
                v_file_dict[line[0]].append(line[1:])
            data = {}
            for vtype in v_file_dict:
                v_attributes = ["vid"] + \
                                v_in_feats.get(vtype, []) + \
                                v_out_labels.get(vtype, []) + \
                                v_extra_feats.get(vtype, [])
                if seeds:
                    try:
                        v_attributes.remove("is_seed")
                    except ValueError:
                        pass
                data[vtype] = pd.DataFrame(v_file_dict[vtype], columns=v_attributes, dtype="object")
                for v_attr in v_extra_feats.get(vtype, []):
                    if v_attr_types[vtype][v_attr] == "MAP":
                        # I am sorry that this is this ugly...
                        data[vtype][v_attr] = data[vtype][v_attr].apply(lambda x: {y.split(",")[0].strip("("): y.split(",")[1].strip(")") for y in x.strip("[").strip("]").split(" ")[:-1]} if x != "[]" else {})
                if seeds:
                    data[vtype] = data[vtype].merge(seed_df, on="vid", how="left")
                    data[vtype].fillna({"is_seed": False}, inplace=True)
        return data

    @staticmethod
    def _parse_edge_data(
        raw: List[str],
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        delimiter: str = "|",
        is_hetero: bool = False,
        seeds: list = []
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Parse raw edge data into dataframes.
        """    
        # Read in edge CSVs as dataframes
        # Each row is in format source_vid,target_vid,e_in_feats,e_out_labels,e_extra_feats
        # or etype,source_vid,target_vid,e_in_feats,e_out_labels,e_extra_feats             
        e_file = (line.split(delimiter) for line in raw)
        if not is_hetero:
            e_attributes = ["source", "target"] + e_in_feats + e_out_labels + e_extra_feats
            if seeds:
                try:
                    e_attributes.remove("is_seed")
                except ValueError:
                    pass
            data = pd.DataFrame(e_file, columns=e_attributes, dtype="object")
            for e_attr in e_extra_feats:
                if e_attr_types.get(e_attr, "") == "MAP":
                    # I am sorry that this is this ugly...
                    data[e_attr] = data[e_attr].apply(lambda x: {y.split(",")[0].strip("("): y.split(",")[1].strip(")") for y in x.strip("[").strip("]").split(" ")[:-1]} if x != "[]" else {})
            # If seeds are given, create the is_seed column
            if seeds:
                seed_df = pd.DataFrame.from_records(
                    [(i.split("_", 1)[0], i.rsplit("_", 1)[-1]) for i in seeds], 
                    columns=["source", "target"])
                seed_df["is_seed"] = True
                data = data.merge(seed_df, on=["source", "target"], how="left")
                data.fillna({"is_seed": False}, inplace=True)
        else:
            e_file_dict = defaultdict(list)
            for line in e_file:
                e_file_dict[line[0]].append(line[1:])
            data = {}
            # If seeds are given, create the is_seed column
            if seeds:
                seed_df = pd.DataFrame.from_records(
                    [(i.split("_", 1)[0], i.split("_", 1)[1].rsplit("_", 1)[0], i.rsplit("_", 1)[-1]) for i in seeds], 
                    columns=["source", "etype", "target"])
                seed_df["is_seed"] = True
            for etype in e_file_dict:
                e_attributes = ["source", "target"] + \
                                e_in_feats.get(etype, []) + \
                                e_out_labels.get(etype, [])  + \
                                e_extra_feats.get(etype, [])
                if seeds:
                    try:
                        e_attributes.remove("is_seed")
                    except ValueError:
                        pass
                data[etype] = pd.DataFrame(e_file_dict[etype], columns=e_attributes, dtype="object")
                for e_attr in e_extra_feats.get(etype, []):
                    if e_attr_types[etype][e_attr] == "MAP":
                        # I am sorry that this is this ugly...
                        data[etype][e_attr] = data[etype][e_attr].apply(lambda x: {y.split(",")[0].strip("("): y.split(",")[1].strip(")") for y in x.strip("[").strip("]").split(" ")[:-1]} if x != "[]" else {})
                if seeds:
                    tmp_df = seed_df[seed_df["etype"]==etype]
                    if len(tmp_df)>0:
                        data[etype] = data[etype].merge(
                            tmp_df[["source", "target", "is_seed"]], on=["source", "target"], how="left")
                        data[etype].fillna({"is_seed": False}, inplace=True)
                    else:
                        data[etype]["is_seed"] = False
        return data

    @staticmethod
    def _parse_graph_data_to_df(
        raw: Tuple[List[str], List[str]],
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        delimiter: str = "|",
        primary_id: dict = {},
        is_hetero: bool = False,
        seed_type: str = ""
    ) -> Union[Tuple[pd.DataFrame, pd.DataFrame], Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]]:
        """Parse raw data into dataframes.
        """    
        # Read in vertex and edge CSVs as dataframes              
        # A pair of in-memory CSVs (vertex, edge)
        if len(raw) == 3:
            v_file, e_file, seed_file = raw
        else:
            v_file, e_file = raw
            seed_file = []
        vertices = BaseLoader._parse_vertex_data(
            raw = v_file,
            v_in_feats = v_in_feats,
            v_out_labels = v_out_labels,
            v_extra_feats = v_extra_feats,
            v_attr_types = v_attr_types,
            delimiter = delimiter,
            is_hetero = is_hetero,
            seeds = seed_file if seed_type=="vertex" else []
        )
        if primary_id:
            id_map = pd.DataFrame({"vid": primary_id.keys(), "primary_id": primary_id.values()}, 
                                  dtype="object")
            if not is_hetero:
                vertices = vertices.merge(id_map, on="vid")
                v_extra_feats.append("primary_id")
            else:
                for vtype in vertices:
                    vertices[vtype] = vertices[vtype].merge(id_map, on="vid")
                    v_extra_feats[vtype].append("primary_id")
        edges = BaseLoader._parse_edge_data(
            raw = e_file,
            e_in_feats = e_in_feats,
            e_out_labels = e_out_labels,
            e_extra_feats = e_extra_feats,
            e_attr_types = e_attr_types,
            delimiter = delimiter,
            is_hetero = is_hetero,
            seeds = seed_file if seed_type=="edge" else []
        )
        return (vertices, edges)

    @staticmethod
    def _attributes_to_np(
            attributes: list, attr_types: dict, df: pd.DataFrame
        ) -> np.ndarray:
        """Turn multiple columns of a dataframe into a numpy array.
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
            elif dtype == "uint":
                # PyTorch only supports uint8. Need to convert it to int.
                x.append(df[[col]].to_numpy().astype("int"))
            else:
                x.append(df[[col]].to_numpy().astype(dtype))
        return np.hstack(x)
    
    @staticmethod
    def _get_edgelist(
        vertices: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
        edges: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
        is_hetero: bool,
        e_attr_types: dict = {} 
    ):
        if not is_hetero:
            vertices["tmp_id"] = range(len(vertices))
            id_map = vertices[["vid", "tmp_id"]]
            edgelist = edges[["source", "target"]].merge(id_map, left_on="source", right_on="vid")
            edgelist = edgelist.merge(id_map, left_on="target", right_on="vid")
            edgelist = edgelist[["tmp_id_x", "tmp_id_y"]]
        else:
            edgelist = {}
            id_map = {}
            for vtype in vertices:
                vertices[vtype]["tmp_id"] = range(len(vertices[vtype]))
                id_map[vtype] = vertices[vtype][["vid", "tmp_id"]]
            for etype in edges:
                source_type = e_attr_types[etype]["FromVertexTypeName"]
                target_type = e_attr_types[etype]["ToVertexTypeName"]
                if e_attr_types[etype]["IsDirected"] or source_type==target_type:
                    edgelist[etype] = edges[etype][["source", "target"]].merge(id_map[source_type], left_on="source", right_on="vid")
                    edgelist[etype] = edgelist[etype].merge(id_map[target_type], left_on="target", right_on="vid")
                    edgelist[etype] = edgelist[etype][["tmp_id_x", "tmp_id_y"]]
                else:
                    subdf1 = edges[etype].merge(id_map[source_type], left_on="source", right_on="vid")
                    subdf1 = subdf1.merge(id_map[target_type], left_on="target", right_on="vid")
                    if len(subdf1) < len(edges[etype]):
                        subdf2 = edges[etype].merge(id_map[source_type], left_on="target", right_on="vid")
                        subdf2 = subdf2.merge(id_map[target_type], left_on="source", right_on="vid")
                        subdf1 = pd.concat((subdf1, subdf2), ignore_index=True)
                    edges[etype] = subdf1
                    edgelist[etype] = edges[etype][["tmp_id_x", "tmp_id_y"]]
        return edgelist

    @staticmethod
    def _parse_df_to_pyg(
        raw: Union[Tuple[pd.DataFrame, pd.DataFrame], Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]],
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        add_self_loop: bool = False,
        is_hetero: bool = False,
        torch = None,
        pyg = None
    ) -> Union["pyg.data.Data", "pyg.data.HeteroData"]:
        """Parse dataframes to PyG graphs.
        """    
        def add_attributes(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                           graph, is_hetero: bool, feat_name: str, 
                           target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add multiple attributes as a single feature to edges or vertices.
            """                    
            if is_hetero:
                if not vetype:
                    raise ValueError("Vertex or edge type required for heterogeneous graphs")
                # Focus on a specific type
                if target == "edge":
                    data = graph[attr_types["FromVertexTypeName"], 
                                    vetype,
                                    attr_types["ToVertexTypeName"]]
                elif target == "vertex":
                    data = graph[vetype]
            else:
                data = graph
            array = BaseLoader._attributes_to_np(attr_names, attr_types, attr_df)
            data[feat_name] = torch.tensor(array).squeeze(dim=1)
        
        def add_sep_attr(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                         graph, is_hetero: bool, 
                         target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add each attribute as a single feature to edges or vertices.
            """
            if is_hetero:
                if not vetype:
                    raise ValueError("Vertex or edge type required for heterogeneous graphs")
                # Focus on a specific type
                if target == "edge":
                    data = graph[attr_types["FromVertexTypeName"], 
                                    vetype,
                                    attr_types["ToVertexTypeName"]]
                elif target == "vertex":
                    data = graph[vetype]
            else:
                data = graph

            for col in attr_names:
                dtype = attr_types[col].lower()
                if dtype.startswith("str") or dtype.startswith("map"):
                    data[col] = attr_df[col].to_list()
                elif dtype.startswith("list"):
                    dtype2 = dtype.split(":")[1]
                    if dtype2.startswith("str"):
                        data[col] = attr_df[col].str.split().to_list()
                    else:
                        data[col] = torch.tensor(
                            attr_df[col]
                            .str.split(expand=True)
                            .to_numpy()
                            .astype(dtype2)
                        )
                elif dtype.startswith("set") or dtype.startswith("date"):
                    raise NotImplementedError(
                        "{} type not supported for extra features yet.".format(dtype))
                elif dtype == "bool":
                    data[col] = torch.tensor(
                        attr_df[col].astype("int8").astype(dtype)
                    )
                elif dtype == "uint":
                    # PyTorch only supports uint8. Need to convert it to int.
                    data[col] = torch.tensor(
                        attr_df[col].astype("int")
                    )
                else:
                    data[col] = torch.tensor(
                        attr_df[col].astype(dtype)
                    )
        
        # Convert dataframes into PyG graphs
        # Reformat as a graph.
        # Need to have a pair of tables for edges and vertices.
        vertices, edges = raw
        edgelist = BaseLoader._get_edgelist(vertices, edges, is_hetero, e_attr_types)
        if not is_hetero:
            # Deal with edgelist first
            edgelist = torch.tensor(edgelist.to_numpy().T, dtype=torch.long)
            if add_self_loop:
                edgelist = pyg.utils.add_self_loops(edgelist)[0]
            data = pyg.data.Data()
            data["edge_index"] = edgelist 
            # Deal with edge attributes
            if e_in_feats:
                add_attributes(e_in_feats, e_attr_types, edges, 
                                data, is_hetero, "edge_feat", "edge")
            if e_out_labels:
                add_attributes(e_out_labels, e_attr_types, edges, 
                                data, is_hetero, "edge_label", "edge")
            if e_extra_feats:
                add_sep_attr(e_extra_feats, e_attr_types, edges,
                            data, is_hetero, "edge")
            del edges
            # Deal with vertex attributes next
            if v_in_feats:
                add_attributes(v_in_feats, v_attr_types, vertices, 
                                data, is_hetero, "x", "vertex")
            if v_out_labels:
                add_attributes(v_out_labels, v_attr_types, vertices, 
                                data, is_hetero, "y", "vertex")
            if v_extra_feats:
                add_sep_attr(v_extra_feats, v_attr_types, vertices,
                            data, is_hetero, "vertex")
            del vertices
        else:
            # Heterogeneous graph
            # Deal with edgelist first
            for etype in edges:
                edgelist[etype] = torch.tensor(edgelist[etype].to_numpy().T, dtype=torch.long)
                if add_self_loop:
                    edgelist[etype] = pyg.utils.add_self_loops(edgelist[etype])[0]
            data = pyg.data.HeteroData()
            for etype in edgelist:
                data[e_attr_types[etype]["FromVertexTypeName"], 
                    etype,
                    e_attr_types[etype]["ToVertexTypeName"]].edge_index = edgelist[etype]
            # Deal with edge attributes
            if e_in_feats:
                for etype in edges:
                    if etype not in e_in_feats:
                        continue
                    if e_in_feats[etype]:
                        add_attributes(e_in_feats[etype], e_attr_types[etype], edges[etype], 
                                    data, is_hetero, "edge_feat", "edge", etype)
            if e_out_labels:
                for etype in edges:
                    if etype not in e_out_labels:
                        continue
                    if e_out_labels[etype]:
                        add_attributes(e_out_labels[etype], e_attr_types[etype], edges[etype], 
                                    data, is_hetero, "edge_label", "edge", etype)
            if e_extra_feats:
                for etype in edges:
                    if etype not in e_extra_feats:
                        continue
                    if e_extra_feats[etype]:
                        add_sep_attr(e_extra_feats[etype], e_attr_types[etype], edges[etype],
                                data, is_hetero, "edge", etype)
            del edges 
            # Deal with vertex attributes next
            if v_in_feats:
                for vtype in vertices:
                    if vtype not in v_in_feats:
                        continue
                    if v_in_feats[vtype]:
                        add_attributes(v_in_feats[vtype], v_attr_types[vtype], vertices[vtype], 
                                    data, is_hetero, "x", "vertex", vtype)
            if v_out_labels:
                for vtype in vertices:
                    if vtype not in v_out_labels:
                        continue
                    if v_out_labels[vtype]:
                        add_attributes(v_out_labels[vtype], v_attr_types[vtype], vertices[vtype], 
                                    data, is_hetero, "y", "vertex", vtype)
            if v_extra_feats:
                for vtype in vertices:
                    if vtype not in v_extra_feats:
                        continue
                    if v_extra_feats[vtype]:
                        add_sep_attr(v_extra_feats[vtype], v_attr_types[vtype], vertices[vtype],
                                data, is_hetero, "vertex", vtype)
            del vertices
        return data

    @staticmethod
    def _parse_df_to_dgl(
        raw: Union[Tuple[pd.DataFrame, pd.DataFrame], Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]],
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        add_self_loop: bool = False,
        is_hetero: bool = False,
        torch = None,
        dgl = None
    ) -> Union["dgl.graph", "dgl.heterograph"]:
        """Parse dataframes to PyG graphs.
        """    
        def add_attributes(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                           graph, is_hetero: bool, feat_name: str, 
                           target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add multiple attributes as a single feature to edges or vertices.
            """                    
            if is_hetero:
                if not vetype:
                    raise ValueError("Vertex or edge type required for heterogeneous graphs")
                # Focus on a specific type
                if target == "edge":
                    data = graph.edges[vetype].data
                elif target == "vertex":
                    data = graph.nodes[vetype].data
            else:
                if target == "edge":
                    data = graph.edata
                elif target == "vertex":
                    data = graph.ndata
            array = BaseLoader._attributes_to_np(attr_names, attr_types, attr_df)
            data[feat_name] = torch.tensor(array).squeeze(dim=1)
        
        def add_sep_attr(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                         graph, is_hetero: bool,
                         target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add each attribute as a single feature to edges or vertices.
            """
            if is_hetero:
                if not vetype:
                    raise ValueError("Vertex or edge type required for heterogeneous graphs")
                # Focus on a specific type
                if target == "edge":
                    data = graph.edges[vetype].data
                elif target == "vertex":
                    data = graph.nodes[vetype].data
            else:
                if target == "edge":
                    data = graph.edata
                elif target == "vertex":
                    data = graph.ndata

            for col in attr_names:
                dtype = attr_types[col].lower()
                if dtype.startswith("str") or dtype.startswith("map"):
                    if vetype is None:
                        # Homogeneous graph, add column directly to extra data
                        graph.extra_data[col] = attr_df[col].to_list()
                    elif vetype not in graph.extra_data:
                        # Hetero graph, vetype doesn't exist in extra data
                        graph.extra_data[vetype] = {}
                        graph.extra_data[vetype][col] = attr_df[col].to_list()
                    else: 
                        # Hetero graph and vetype already exists
                        graph.extra_data[vetype][col] = attr_df[col].to_list()
                elif dtype.startswith("list"):
                    dtype2 = dtype.split(":")[1]
                    if dtype2.startswith("str"):
                        if vetype is None:
                            # Homogeneous graph, add column directly to extra data
                            graph.extra_data[col] = attr_df[col].str.split().to_list()
                        elif vetype not in graph.extra_data:
                            # Hetero graph, vetype doesn't exist in extra data
                            graph.extra_data[vetype] = {}
                            graph.extra_data[vetype][col] = attr_df[col].str.split().to_list()
                        else: 
                            # Hetero graph and vetype already exists
                            graph.extra_data[vetype][col] = attr_df[col].str.split().to_list()
                    else:
                        data[col] = torch.tensor(
                            attr_df[col]
                            .str.split(expand=True)
                            .to_numpy()
                            .astype(dtype2)
                        )
                elif dtype.startswith("set") or dtype.startswith("date"):
                    raise NotImplementedError(
                        "{} type not supported for extra features yet.".format(dtype))
                elif dtype == "bool":
                    data[col] = torch.tensor(
                        attr_df[col].astype("int8").astype(dtype)
                    )
                elif dtype == "uint":
                    # PyTorch only supports uint8. Need to convert it to int.
                    data[col] = torch.tensor(
                        attr_df[col].astype("int")
                    )
                else:
                    data[col] = torch.tensor(
                        attr_df[col].astype(dtype)
                    )

        # Reformat as a graph.
        # Need to have a pair of tables for edges and vertices.
        vertices, edges = raw
        edgelist = BaseLoader._get_edgelist(vertices, edges, is_hetero, e_attr_types)
        if not is_hetero:
            # Deal with edgelist first
            edgelist = torch.tensor(edgelist.to_numpy().T, dtype=torch.long)
            data = dgl.graph(data=(edgelist[0], edgelist[1]))
            if add_self_loop:
                data = dgl.add_self_loop(data)
            data.extra_data = {} 
            # Deal with edge attributes
            if e_in_feats:
                add_attributes(e_in_feats, e_attr_types, edges, 
                                data, is_hetero, "edge_feat", "edge")
            if e_out_labels:
                add_attributes(e_out_labels, e_attr_types, edges, 
                                data, is_hetero, "edge_label", "edge")
            if e_extra_feats:
                add_sep_attr(e_extra_feats, e_attr_types, edges,
                            data, is_hetero, "edge")            
            del edges
            # Deal with vertex attributes next
            if v_in_feats:
                add_attributes(v_in_feats, v_attr_types, vertices, 
                                data, is_hetero, "x", "vertex")
            if v_out_labels:
                add_attributes(v_out_labels, v_attr_types, vertices, 
                                data, is_hetero, "y", "vertex")
            if v_extra_feats:
                add_sep_attr(v_extra_feats, v_attr_types, vertices,
                            data, is_hetero, "vertex")
            del vertices
        else:
            # Heterogeneous graph
            # Deal with edgelist first
            for etype in edges:
                edgelist[etype] = torch.tensor(edgelist[etype].to_numpy().T, dtype=torch.long)
            data = dgl.heterograph({
                (e_attr_types[etype]["FromVertexTypeName"], etype, e_attr_types[etype]["ToVertexTypeName"]): (edgelist[etype][0], edgelist[etype][1]) for etype in edgelist})
            if add_self_loop:
                data = dgl.add_self_loop(data)
            data.extra_data = {}
            # Deal with edge attributes
            if e_in_feats:
                for etype in edges:
                    if etype not in e_in_feats:
                        continue
                    if e_in_feats[etype]:
                        add_attributes(e_in_feats[etype], e_attr_types[etype], edges[etype], 
                                    data, is_hetero, "edge_feat", "edge", etype)
            if e_out_labels:
                for etype in edges:
                    if etype not in e_out_labels:
                        continue
                    if e_out_labels[etype]:
                        add_attributes(e_out_labels[etype], e_attr_types[etype], edges[etype], 
                                    data, is_hetero, "edge_label", "edge", etype)
            if e_extra_feats:
                for etype in edges:
                    if etype not in e_extra_feats:
                        continue
                    if e_extra_feats[etype]:
                        add_sep_attr(e_extra_feats[etype], e_attr_types[etype], edges[etype],
                                data, is_hetero, "edge", etype)   
            del edges
            # Deal with vertex attributes next
            if v_in_feats:
                for vtype in vertices:
                    if vtype not in v_in_feats:
                        continue
                    if v_in_feats[vtype]:
                        add_attributes(v_in_feats[vtype], v_attr_types[vtype], vertices[vtype], 
                                    data, is_hetero, "x", "vertex", vtype)
            if v_out_labels:
                for vtype in vertices:
                    if vtype not in v_out_labels:
                        continue
                    if v_out_labels[vtype]:
                        add_attributes(v_out_labels[vtype], v_attr_types[vtype], vertices[vtype], 
                                    data, is_hetero, "y", "vertex", vtype)
            if v_extra_feats:
                for vtype in vertices:
                    if vtype not in v_extra_feats:
                        continue
                    if v_extra_feats[vtype]:
                        add_sep_attr(v_extra_feats[vtype], v_attr_types[vtype], vertices[vtype],
                                data, is_hetero, "vertex", vtype)   
            del vertices
        return data

    @staticmethod
    def _parse_df_to_spektral(
        raw: Union[Tuple[pd.DataFrame, pd.DataFrame], Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]],
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        add_self_loop: bool = False,
        is_hetero: bool = False,
        scipy = None,
        spektral = None
    ) -> "spektral.data.graph.Graph":
        """Parse dataframes to Spektral graphs.
        """
        def add_attributes(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                           graph, is_hetero: bool, feat_name: str, 
                           target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add multiple attributes as a single feature to edges or vertices.
            """                    
            data = graph
            array = BaseLoader._attributes_to_np(attr_names, attr_types, attr_df)
            try:
                array = np.squeeze(array, axis=1)
            except:
                pass
            data[feat_name] = array
        
        def add_sep_attr(attr_names: list, attr_types: dict, attr_df: pd.DataFrame, 
                         graph, is_hetero: bool,
                         target: 'Literal["edge", "vertex"]', vetype: str = None) -> None:
            """Add each attribute as a single feature to edges or vertices.
            """
            data = graph
            for col in attr_names:
                dtype = attr_types[col].lower()
                if dtype.startswith("str") or dtype.startswith("map"):
                    data[col] = attr_df[col].to_list()
                elif dtype.startswith("list"):
                    dtype2 = dtype.split(":")[1]
                    if dtype2.startswith("str"):
                        data[col] = attr_df[col].str.split().to_list()
                    else:
                        data[col] = attr_df[col].str.split(expand=True).to_numpy().astype(dtype2)
                elif dtype.startswith("set") or dtype.startswith("date"):
                    raise NotImplementedError(
                        "{} type not supported for extra features yet.".format(dtype))
                elif dtype == "bool":
                    data[col] = attr_df[col].astype("int8").astype(dtype)
                else:
                    data[col] = attr_df[col].astype(dtype)

        # Reformat as a graph.
        # Need to have a pair of tables for edges and vertices.
        vertices, edges = raw
        edgelist = BaseLoader._get_edgelist(vertices, edges, is_hetero, e_attr_types)
        if not is_hetero:
            # Deal with edgelist first
            n_edges = len(edgelist)
            n_vertices = len(vertices)
            adjacency_data = [1 for i in range(n_edges)] #spektral adjacency format requires weights for each edge to initialize
            adjacency = scipy.sparse.coo_matrix((adjacency_data, (edgelist["tmp_id_x"], edgelist["tmp_id_y"])), shape=(n_vertices, n_vertices))
            if add_self_loop:
                adjacency = spektral.utils.add_self_loops(adjacency, value=1)
            edge_index = np.stack((adjacency.row, adjacency.col), axis=-1)
            data = spektral.data.graph.Graph(A=adjacency)
            del edgelist     
            # Deal with edge attributes
            if e_in_feats:
                add_attributes(e_in_feats, e_attr_types, edges, 
                                data, is_hetero, "edge_feat", "edge")
                edge_data = data["edge_feat"]
                edge_index, edge_data = spektral.utils.reorder(edge_index, edge_features=edge_data)
                n_edges = len(edge_index)
                data["e"] = np.array([[i] for i in edge_data]) #if something breaks when you add self-loops it's here
                adjacency_data = [1 for i in range(n_edges)]
                data["a"] = scipy.sparse.coo_matrix((adjacency_data, (edge_index[:, 0], edge_index[:, 1])), shape=(n_vertices, n_vertices))
            if e_out_labels:
                add_attributes(e_out_labels, e_attr_types, edges, 
                                data, is_hetero, "edge_label", "edge")
            if e_extra_feats:
                add_sep_attr(e_extra_feats, e_attr_types, edges,
                            data, is_hetero, "edge")            
            del edges
            # Deal with vertex attributes next
            if v_in_feats:
                add_attributes(v_in_feats, v_attr_types, vertices, 
                                data, is_hetero, "x", "vertex")
            if v_out_labels:
                add_attributes(v_out_labels, v_attr_types, vertices, 
                                data, is_hetero, "y", "vertex")
            if v_extra_feats:
                add_sep_attr(v_extra_feats, v_attr_types, vertices,
                            data, is_hetero, "vertex")
            del vertices
        else:
            # Heterogeneous graph
            # Deal with edgelist first
            raise NotImplementedError
        return data

    def _start_request(self, is_graph: bool):
        # If using kafka
        if self.kafka_address_consumer:
            # Generate topic
            self._set_kafka_topic()
            # Start consumer thread
            if is_graph:
                self._downloader = Thread(
                    target=self._download_graph_kafka,
                    kwargs=dict(
                        exit_event = self._exit_event,
                        read_task_q = self._read_task_q,
                        kafka_consumer = self._kafka_consumer,
                    ),
                )
            else:
                self._downloader = Thread(
                    target=self._download_unimode_kafka,
                    kwargs=dict(
                        exit_event = self._exit_event,
                        read_task_q = self._read_task_q,
                        kafka_consumer = self._kafka_consumer
                    ),
                )
            self._downloader.start()
            # Start requester thread
            if not self.kafka_skip_produce:
                self._requester = Thread(
                    target=self._request_kafka,
                    args=(
                        self._exit_event,
                        self._graph,
                        self.query_name,
                        self.timeout,
                        self._payload,
                    ),
                )
                self._requester.start()
        else:
            # Otherwise, use rest api
            if is_graph:
                self._requester = Thread(
                    target=self._request_graph_rest,
                    kwargs=dict(
                        tgraph = self._graph,
                        query_name = self.query_name,
                        read_task_q = self._read_task_q,
                        timeout = self.timeout,
                        payload = self._payload
                    ),
                )
            else:
                self._requester = Thread(
                    target=self._request_unimode_rest,
                    kwargs=dict(
                        tgraph = self._graph,
                        query_name = self.query_name,
                        read_task_q = self._read_task_q,
                        timeout = self.timeout,
                        payload = self._payload
                    ),
                )
            self._requester.start()

    def _start(self) -> None:
        # This is a template. Implement your own logics here.
        # Create task and result queues
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

    def stop(self, remove_topics=False) -> None:
        """Stop the dataloading.
        Stop loading data from the database. Will kill the asynchronous query producing batches for kafka.
        Args:
            remove_topics (bool, optional):
                If set to True, the Kafka topics created by the dataloader will be deleted, thus removing the data residing in the topic.
        """
        self._reset(theend=remove_topics)

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

    def _reset(self, theend=False) -> None:
        logger.debug("Resetting data loader")
        if self._exit_event:
            self._exit_event.set()
        logger.debug("Set exit event")
        if self._read_task_q:
            while True:
                try:
                    self._read_task_q.get(timeout=1)
                except Empty:
                    break
        logger.debug("Emptied read task queue")
        if self._data_q:
            while True:
                try:
                    self._data_q.get(timeout=1)
                except Empty:
                    break
        logger.debug("Emptied data queue")
        if self._requester:
            self._requester.join()
        logger.debug("Stopped requester thread")
        if self._downloader:
            self._downloader.join()
        logger.debug("Stopped downloader thread")
        if self._reader:
            self._reader.join()
        logger.debug("Stopped reader thread")
        del self._read_task_q, self._data_q
        self._exit_event = None
        self._requester, self._downloader, self._reader = None, None, None
        self._read_task_q, self._data_q = None, None
        logger.debug("Deleted all queues and threads")
        if theend:
            if self._kafka_topic and self._kafka_consumer:
                self._kafka_consumer.unsubscribe()
            if self.delete_all_topics and self._kafka_admin:
                topics_to_delete = self._all_kafka_topics.intersection(self._kafka_admin.list_topics())
                resp = self._kafka_admin.delete_topics(list(topics_to_delete))
                for del_res in resp.to_object()["topic_error_codes"]:
                    if del_res["error_code"] != 0:
                        raise TigerGraphException(
                            "Failed to delete topic {}".format(del_res["topic"])
                        )
            logger.debug("Finished with Kafka. Reached the end.")
        else:
            if self.delete_epoch_topic and self._kafka_admin:
                if self._kafka_topic and self._kafka_consumer:
                    self._kafka_consumer.unsubscribe()
                resp = self._kafka_admin.delete_topics([self._kafka_topic])
                del_res = resp.to_object()["topic_error_codes"][0]
                if del_res["error_code"] != 0:
                    raise TigerGraphException(
                        "Failed to delete topic {}".format(del_res["topic"])
                    )
                self._kafka_topic = None
            logger.debug("Finished with Kafka")
        logger.debug("Reset data loader successfully")

    def _generate_attribute_string(self, schema_type, attr_names, attr_types) -> str:
        if schema_type.lower() == "vertex":
            print_attr = '+delimiter+'.join(
                            "stringify(s.{})".format(attr) if (attr_types[attr] != "MAP" and attr_types[attr] != "DATETIME") else 
                            '"["+stringify(s.{})+"]"'.format(attr) if attr_types[attr] == "MAP" 
                            else "stringify(datetime_to_epoch(s.{}))".format(attr)
                            for attr in attr_names
                        )
        if schema_type.lower() == "edge":
            print_attr = '+delimiter+'.join(
                        "stringify(e.{})".format(attr) 
                        if (attr_types[attr] != "MAP" and attr_types[attr] != "DATETIME") else 
                        '"["+stringify(e.{})+"]"'.format(attr) if attr_types[attr] == "MAP" 
                        else "stringify(datetime_to_epoch(e.{}))".format(attr)
                        for attr in attr_names
                    )
        return print_attr
    
    def metadata(self, additional_v_types=None, additional_e_types=None) -> Tuple[list, list]:
        v_types = self._vtypes
        if additional_v_types:
            if isinstance(additional_v_types, list):
                v_types += additional_v_types
            elif isinstance(additional_v_types, str):
                v_types.append(additional_v_types)
        edges = []
        for e in self._etypes:
            edges.append((self._e_schema[e]["FromVertexTypeName"], e, self._e_schema[e]["ToVertexTypeName"]))
        if additional_e_types:
            if isinstance(additional_e_types, list):
                edges += additional_e_types
            elif isinstance(additional_e_types, tuple):
                edges.append(additional_e_types)
        return (v_types, edges)
    
    def fetch(self, payload: dict) -> None:
        """Fetch the specific data instances for inference/prediction.

        Args:
            payload (dict): The JSON payload to send to the API.
        """
        # Send request
        # Parse data
        # Return data
        raise NotImplementedError

    def __len__(self) -> int:
        return self.num_batches

    def reinstall_query(self) -> str:
        """Reinstall the dataloader query.

        Returns:
            The name of the query installed (str)
        """
        return self._install_query(force=True)


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
        v_seed_types: Union[str, list] = None,
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
        delimiter: str = "|",
        timeout: int = 300000,
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None
    ) -> None:
        """NO DOC"""

        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            delimiter,
            timeout,
            distributed_query,
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
            kafka_sasl_kerberos_service_name,
            kafka_sasl_kerberos_keytab,
            kafka_sasl_kerberos_principal,
            kafka_sasl_kerberos_domain_name,
            kafka_ssl_check_hostname,
            kafka_producer_ca_location,
            kafka_producer_certificate_location,
            kafka_producer_key_location,
            kafka_producer_key_password,
            kafka_consumer_ca_location,
            kafka_consumer_certificate_location,
            kafka_consumer_key_location,
            kafka_consumer_key_password,
            kafka_skip_produce,
            kafka_auto_offset_reset,
            kafka_del_topic_per_epoch,
            kafka_add_topic_per_epoch,
            callback_fn,
            kafka_group_id,
            kafka_topic
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
        self._vtypes = sorted(self._vtypes)
        self._etypes = sorted(self._etypes)
        # Resolve seeds
        if v_seed_types:
            if isinstance(v_seed_types, list):
                self._seed_types = v_seed_types
            elif isinstance(v_seed_types, str):
                self._seed_types = [v_seed_types]
            else:
                raise TigerGraphException("v_seed_types must be either of type list or string.")
        elif isinstance(filter_by, dict):
            self._seed_types = list(filter_by.keys())
        else:
            self._seed_types = self._vtypes
        if set(self._seed_types) - set(self._vtypes):
            raise ValueError("Seed type has to be one of the vertex types to retrieve")

        if batch_size:
            # batch size takes precedence over number of batches
            self.batch_size = batch_size
            self.num_batches = None
        else:
            # If number of batches is given, calculate batch size
            if not filter_by:
                num_vertices = sum(self._graph.getVertexCount(self._seed_types).values())
            elif isinstance(filter_by, str):
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by))
                    for k in self._seed_types
                )
            elif isinstance(filter_by, dict):
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by[k]))
                    for k in self._seed_types
                )
            else:
                raise ValueError("filter_by should be None, attribute name, or dict of {type name: attribute name}.")
            self.batch_size = math.ceil(num_vertices / num_batches)
            self.num_batches = num_batches
        # Initialize parameters for the query
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
        self._payload["delimiter"] = self.delimiter
        self._payload["input_vertices"] = []
        # Output
        self.add_self_loop = add_self_loop
        # Install query
        self.query_name = self._install_query()

    def _install_query(self, force: bool = False):
        # Install the right GSQL query for the loader.
        query_suffix = {
            "v_in_feats": self.v_in_feats,
            "v_out_labels": self.v_out_labels,
            "v_extra_feats": self.v_extra_feats,
            "e_in_feats": self.e_in_feats,
            "e_out_labels": self.e_out_labels,
            "e_extra_feats": self.e_extra_feats,
            "distributed_query": self.distributed_query
        }
        md5 = hashlib.md5()
        md5.update(json.dumps(query_suffix).encode())
        query_replace = {"{QUERYSUFFIX}": md5.hexdigest()}

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
                v_attr_types = self._v_schema[vtype]
                print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
                print_query_seed += """
                    {} s.type == "{}" THEN
                        @@v_batch += (s.type + delimiter + stringify(getvid(s)) {} + delimiter + "1\\n")"""\
                    .format("IF" if idx==0 else "ELSE IF", 
                            vtype, 
                            "+ delimiter + " + print_attr if v_attr_names else "")
                print_query_other += """
                    {} s.type == "{}" THEN
                        @@v_batch += (s.type + delimiter + stringify(getvid(s)) {} + delimiter + "0\\n")"""\
                    .format("IF" if idx==0 else "ELSE IF", 
                            vtype, 
                            "+ delimiter + " + print_attr if v_attr_names else "")
            print_query_seed += """
                    END"""
            print_query_other += """
                    END"""
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
                e_attr_types = self._e_schema[etype]
                print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
                print_query += """
                    {} e.type == "{}" THEN
                        @@e_batch += (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) {} + "\\n")"""\
                    .format("IF" if idx==0 else "ELSE IF", 
                            etype,                                 
                            "+ delimiter + " + print_attr if e_attr_names else "")
            print_query += """
                    END"""
            query_replace["{EDGEATTRS}"] = print_query
        else:
            # Ignore vertex types
            v_attr_names = self.v_in_feats + self.v_out_labels + self.v_extra_feats
            v_attr_types = next(iter(self._v_schema.values()))
            print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
            print_query = '@@v_batch += (stringify(getvid(s)) {} + delimiter + "1\\n")'.format(
                "+ delimiter + " + print_attr if v_attr_names else ""
            )
            query_replace["{SEEDVERTEXATTRS}"] = print_query
            print_query = '@@v_batch += (stringify(getvid(s)) {} + delimiter + "0\\n")'.format(
                "+ delimiter + " + print_attr if v_attr_names else ""
            )
            query_replace["{OTHERVERTEXATTRS}"] = print_query
            # Ignore edge types
            e_attr_names = self.e_in_feats + self.e_out_labels + self.e_extra_feats
            e_attr_types = next(iter(self._e_schema.values()))
            print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
            print_query = '@@e_batch += (stringify(getvid(s)) + delimiter + stringify(getvid(t)) {} + "\\n")'.format(
                " + delimiter + " + print_attr if e_attr_names else ""
            )
            query_replace["{EDGEATTRS}"] = print_query
        # Install query
        query_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "gsql",
                "dataloaders",
                "neighbor_loader.gsql",
        )
        sub_query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "neighbor_loader_sub.gsql",
        )
        return install_query_files(self._graph, [sub_query_path, query_path], query_replace, force=force, distributed=[False, self.distributed_query])

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        self._start_request(True)

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
            target=self._read_graph_data,
            kwargs=dict(
                exit_event = self._exit_event,
                in_q = self._read_task_q,
                out_q = self._data_q,
                batch_size = self.batch_size,
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
                delimiter = self.delimiter,
                is_hetero = self.is_hetero,
                callback_fn = self.callback_fn
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
        _payload["num_neighbors"] = self._payload["num_neighbors"]
        _payload["num_hops"] = self._payload["num_hops"]
        _payload["delimiter"] = self._payload["delimiter"]
        _payload["input_vertices"] = []
        for i in vertices:
            _payload["input_vertices"].append({"id": i["primary_id"], "type": i["type"]})
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
        vertex_batch = set()
        edge_batch = set()
        for i in resp:
            if "pids" in i:
                break
            vertex_batch.update(i["vertex_batch"].splitlines())
            edge_batch.update(i["edge_batch"].splitlines())
        data = self._parse_graph_data_to_df(
            raw = (vertex_batch, edge_batch),
            v_in_feats = self.v_in_feats,
            v_out_labels = self.v_out_labels,
            v_extra_feats = v_extra_feats,
            v_attr_types = v_attr_types,
            e_in_feats = self.e_in_feats,
            e_out_labels = self.e_out_labels,
            e_extra_feats = self.e_extra_feats,
            e_attr_types = e_attr_types,
            delimiter = self.delimiter,
            primary_id = i["pids"],
            is_hetero = self.is_hetero,
        )
        if self.output_format == "dataframe" or self.output_format== "df":
            vertices, edges = data
            if not self.is_hetero:
                for column in vertices.columns:
                    vertices[column] = pd.to_numeric(vertices[column], errors="ignore")
                for column in edges.columns:
                    edges[column] = pd.to_numeric(edges[column], errors="ignore")
            else:
                for key in vertices:
                    for column in vertices[key].columns:
                        vertices[key][column] = pd.to_numeric(vertices[key][column], errors="ignore")
                for key in edges:
                    for column in edges[key].columns:
                        edges[key][column] = pd.to_numeric(edges[key][column], errors="ignore")
            data = (vertices, edges)
        elif self.output_format == "pyg":
            try:
                import torch
            except ImportError:
                raise ImportError(
                    "PyTorch is not installed. Please install it to use PyG or DGL output."
                )
            try:
                import torch_geometric as pyg
            except ImportError:
                raise ImportError(
                    "PyG is not installed. Please install PyG to use PyG format."
                )
            data = BaseLoader._parse_df_to_pyg(
                raw = data,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = self.e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                is_hetero = self.is_hetero,
                torch = torch,
                pyg = pyg
            )
        elif self.output_format == "dgl":
            try:
                import torch
            except ImportError:
                raise ImportError(
                    "PyTorch is not installed. Please install it to use PyG or DGL output."
                )
            try:
                import dgl
            except ImportError:
                raise ImportError(
                    "DGL is not installed. Please install DGL to use DGL format."
                )
            data = BaseLoader._parse_df_to_dgl(
                raw = data,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = self.e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                is_hetero = self.is_hetero,
                torch = torch,
                dgl= dgl
            )
        elif self.output_format == "spektral" and self.is_hetero==False:
            try:
                import tensorflow as tf
            except ImportError:
                raise ImportError(
                    "Tensorflow is not installed. Please install it to use spektral output."
                )
            try:
                import scipy
            except ImportError:
                raise ImportError(
                    "scipy is not installed. Please install it to use spektral output."
                )
            try:
                import spektral
            except ImportError:
                raise ImportError(
                    "Spektral is not installed. Please install it to use spektral output."
                )
            data = BaseLoader._parse_df_to_spektral(
                raw = data,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = self.e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                is_hetero = self.is_hetero,
                scipy = scipy,
                spektral = spektral
            )
        else:
            raise NotImplementedError
        if self.callback_fn:
            data = self.callback_fn(data)
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
        delimiter: str = "|",
        timeout: int = 300000,
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None
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
            delimiter,
            timeout,
            distributed_query,
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
            kafka_sasl_kerberos_service_name,
            kafka_sasl_kerberos_keytab,
            kafka_sasl_kerberos_principal,
            kafka_sasl_kerberos_domain_name,
            kafka_ssl_check_hostname,
            kafka_producer_ca_location,
            kafka_producer_certificate_location,
            kafka_producer_key_location,
            kafka_producer_key_password,
            kafka_consumer_ca_location,
            kafka_consumer_certificate_location,
            kafka_consumer_key_location,
            kafka_consumer_key_password,
            kafka_skip_produce,
            kafka_auto_offset_reset,
            kafka_del_topic_per_epoch,
            kafka_add_topic_per_epoch,
            callback_fn,
            kafka_group_id,
            kafka_topic
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
        self._etypes = sorted(self._etypes)
        # Initialize parameters for the query
        if batch_size:
            # batch size takes precedence over number of batches
            self.batch_size = batch_size
            self.num_batches = None
        else:
            # If number of batches is given, calculate batch size
            if filter_by:
                num_edges = 0
                for e_type in self._etypes:
                    tmp = self._graph.getEdgeStats(e_type)[e_type][filter_by if isinstance(filter_by, str) else filter_by[e_type]]["TRUE"]
                    if self._e_schema[e_type]["IsDirected"]:
                        num_edges += tmp
                    else:
                        num_edges += 2*tmp
            else:
                num_edges = 0
                for e_type in self._etypes:
                    tmp = self._graph.getEdgeCount(e_type)
                    if self._e_schema[e_type]["IsDirected"]:
                        num_edges += tmp
                    else:
                        num_edges += 2*tmp
            if num_edges==0:
                raise ValueError("Cannot find any edge as seed. Please check your configuration and data. If they all look good, please use batch_size instead of num_batches or refresh metadata following https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_parameters_15")
            self.batch_size = math.ceil(num_edges / num_batches)
            self.num_batches = num_batches
        # Initialize the exporter
        if filter_by:
            self._payload["filter_by"] = filter_by
        self._payload["shuffle"] = shuffle
        self._payload["e_types"] = self._etypes
        self._payload["delimiter"] = self.delimiter
        # Output
        # Install query
        self.query_name = self._install_query()

    def _install_query(self, force: bool = False):
        # Install the right GSQL query for the loader.
        query_suffix = {
            "attributes": self.attributes,
            "distributed_query": self.distributed_query
        }
        md5 = hashlib.md5()
        md5.update(json.dumps(query_suffix).encode())
        query_replace = {"{QUERYSUFFIX}": md5.hexdigest()}

        if isinstance(self.attributes, dict):
            # Multiple edge types
            print_query_kafka = ""
            print_query_http = ""
            for idx, etype in enumerate(self._etypes):
                e_attr_names = self.attributes.get(etype, [])
                e_attr_types = self._e_schema[etype]
                if e_attr_names:
                    print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
                    print_query_http += """
                        {} e.type == "{}" THEN
                            @@e_batch += (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) + delimiter + {} + "\\n")\n"""\
                        .format("IF" if idx==0 else "ELSE IF", etype, print_attr)
                    print_query_kafka += """
                        {} e.type == "{}" THEN 
                            STRING msg = (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) + delimiter + {} + "\\n"),
                            INT kafka_errcode = write_to_kafka(producer, kafka_topic, (getvid(s)+getvid(t))%kafka_topic_partitions, "edge_" + stringify(getvid(s)) + "_" + stringify(getvid(t)), msg),
                            IF kafka_errcode!=0 THEN 
                                @@kafka_error += ("Error sending data for edge " + stringify(getvid(s)) + "_" + stringify(getvid(t)) + ": "+ stringify(kafka_errcode) + "\\n")
                            END\n""".format("IF" if idx==0 else "ELSE IF", etype, print_attr)
                else:
                    print_query_http += """
                        {} e.type == "{}" THEN 
                            @@e_batch += (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) + "\\n")\n"""\
                        .format("IF" if idx==0 else "ELSE IF", etype)
                    print_query_kafka += """
                        {} e.type == "{}" THEN
                            STRING msg = (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) + "\\n"),
                            INT kafka_errcode = write_to_kafka(producer, kafka_topic, (getvid(s)+getvid(t))%kafka_topic_partitions, "edge_" + stringify(getvid(s)) + "_" + stringify(getvid(t)), msg),
                            IF kafka_errcode!=0 THEN 
                                @@kafka_error += ("Error sending data for edge " + stringify(getvid(s)) + "_" + stringify(getvid(t)) + ": "+ stringify(kafka_errcode) + "\\n")
                            END\n""".format("IF" if idx==0 else "ELSE IF", etype)
            print_query_http += "\
                        END"
            print_query_kafka += "\
                        END"
        else:
            # Ignore edge types
            e_attr_names = self.attributes
            e_attr_types = next(iter(self._e_schema.values()))
            if e_attr_names:
                print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
                print_query_http = '@@e_batch += (stringify(getvid(s)) + delimiter + stringify(getvid(t)) + delimiter + {} + "\\n")'.format(
                    print_attr
                )
                print_query_kafka = """
                    STRING msg = (stringify(getvid(s)) + delimiter + stringify(getvid(t)) + delimiter + {} + "\\n"),
                    INT kafka_errcode = write_to_kafka(producer, kafka_topic, (getvid(s)+getvid(t))%kafka_topic_partitions, "edge_" + stringify(getvid(s)) + "_" + stringify(getvid(t)), msg),
                    IF kafka_errcode!=0 THEN 
                        @@kafka_error += ("Error sending data for edge " + stringify(getvid(s)) + "_" + stringify(getvid(t)) + ": "+ stringify(kafka_errcode) + "\\n")
                    END""".format(print_attr)
            else:
                print_query_http = '@@e_batch += (stringify(getvid(s)) + delimiter + stringify(getvid(t)) + "\\n")'
                print_query_kafka = """
                    STRING msg = (stringify(getvid(s)) + delimiter + stringify(getvid(t)) + "\\n"),
                    INT kafka_errcode = write_to_kafka(producer, kafka_topic, (getvid(s)+getvid(t))%kafka_topic_partitions, "edge_" + stringify(getvid(s)) + "_" + stringify(getvid(t)), msg),
                    IF kafka_errcode!=0 THEN 
                        @@kafka_error += ("Error sending data for edge " + stringify(getvid(s)) + "_" + stringify(getvid(t)) + ": "+ stringify(kafka_errcode) + "\\n")
                    END"""
        query_replace["{EDGEATTRSHTTP}"] = print_query_http
        query_replace["{EDGEATTRSKAFKA}"] = print_query_kafka
        # Install query
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "edge_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace, force=force, distributed=self.distributed_query)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        self._start_request(False)

        # Start reading thread.
        if not self.is_hetero:
            e_attr_types = next(iter(self._e_schema.values()))
        else:
            e_attr_types = self._e_schema
        self._reader = Thread(
            target=self._read_edge_data,
            kwargs=dict(
                exit_event = self._exit_event,
                in_q = self._read_task_q,
                out_q = self._data_q,
                batch_size = self.batch_size,
                e_in_feats = self.attributes,
                e_out_labels = {} if self.is_hetero else [],
                e_extra_feats = {} if self.is_hetero else [],
                e_attr_types = e_attr_types,
                delimiter = self.delimiter,
                is_hetero = self.is_hetero,
                callback_fn = self.callback_fn
            )
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
    <1> The output format is Pandas dataframe.
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
        delimiter: str = "|",
        timeout: int = 300000,
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None
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
            delimiter,
            timeout,
            distributed_query,
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
            kafka_sasl_kerberos_service_name,
            kafka_sasl_kerberos_keytab,
            kafka_sasl_kerberos_principal,
            kafka_sasl_kerberos_domain_name,
            kafka_ssl_check_hostname,
            kafka_producer_ca_location,
            kafka_producer_certificate_location,
            kafka_producer_key_location,
            kafka_producer_key_password,
            kafka_consumer_ca_location,
            kafka_consumer_certificate_location,
            kafka_consumer_key_location,
            kafka_consumer_key_password,
            kafka_skip_produce,
            kafka_auto_offset_reset,
            kafka_del_topic_per_epoch,
            kafka_add_topic_per_epoch,
            callback_fn,
            kafka_group_id,
            kafka_topic
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
        self._vtypes = sorted(self._vtypes)
        # Initialize parameters for the query
        if batch_size:
            # batch size takes precedence over number of batches
            self.batch_size = batch_size
            self.num_batches = None
        else:
            # If number of batches is given, calculate batch size
            num_vertices_by_type = self._graph.getVertexCount(self._vtypes)
            if filter_by:
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by))
                    for k in num_vertices_by_type
                )
            else:
                num_vertices = sum(num_vertices_by_type.values())
            self.batch_size = math.ceil(num_vertices / num_batches)
            self.num_batches = num_batches
        if filter_by:
            self._payload["filter_by"] = filter_by
        self._payload["shuffle"] = shuffle
        self._payload["delimiter"] = delimiter
        self._payload["v_types"] = self._vtypes
        self._payload["input_vertices"] = []
        # Install query
        self.query_name = self._install_query()

    def _install_query(self, force: bool = False) -> str:
        # Install the right GSQL query for the loader.
        query_suffix = {
            "attributes": self.attributes,
            "distributed_query": self.distributed_query
        }
        md5 = hashlib.md5()
        md5.update(json.dumps(query_suffix).encode())
        query_replace = {"{QUERYSUFFIX}": md5.hexdigest()}

        if isinstance(self.attributes, dict):
            # Multiple vertex types
            print_query_kafka = ""
            print_query_http = ""
            for idx, vtype in enumerate(self._vtypes):
                v_attr_names = self.attributes.get(vtype, [])
                v_attr_types = self._v_schema[vtype]
                if v_attr_names:
                    print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
                    print_query_http += """
                        {} s.type == "{}" THEN
                            @@v_batch += (s.type + delimiter + stringify(getvid(s)) + delimiter + {} + "\\n")\n"""\
                        .format("IF" if idx==0 else "ELSE IF", vtype, print_attr)
                    print_query_kafka += """
                        {} s.type == "{}" THEN 
                            STRING msg = (s.type + delimiter + stringify(getvid(s)) + delimiter + {} + "\\n"),
                            INT kafka_errcode = write_to_kafka(producer, kafka_topic, getvid(s)%kafka_topic_partitions, "vertex_" + stringify(getvid(s)), msg),
                            IF kafka_errcode!=0 THEN 
                                @@kafka_error += ("Error sending data for vertex " + stringify(getvid(s)) + ": "+ stringify(kafka_errcode) + "\\n")
                            END\n""".format("IF" if idx==0 else "ELSE IF", vtype, print_attr)
                else:
                    print_query_http += """
                        {} s.type == "{}" THEN
                            @@v_batch += (s.type + delimiter + stringify(getvid(s)) + "\\n")\n"""\
                        .format("IF" if idx==0 else "ELSE IF", vtype)
                    print_query_kafka += """
                        {} s.type == "{}" THEN
                            STRING msg = (s.type + delimiter + stringify(getvid(s)) + "\\n"),
                            INT kafka_errcode = write_to_kafka(producer, kafka_topic, getvid(s)%kafka_topic_partitions, "vertex_" + stringify(getvid(s)), msg),
                            IF kafka_errcode!=0 THEN 
                                @@kafka_error += ("Error sending data for vertex " + stringify(getvid(s)) + ": "+ stringify(kafka_errcode) + "\\n")
                            END\n""".format("IF" if idx==0 else "ELSE IF", vtype)
            print_query_http += "\
                        END"
            print_query_kafka += "\
                        END"
        else:
            # Ignore vertex types
            v_attr_names = self.attributes
            v_attr_types = next(iter(self._v_schema.values()))
            if v_attr_names:
                print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
                print_query_http = '@@v_batch += (stringify(getvid(s)) + delimiter + {} + "\\n")'.format(
                    print_attr
                )
                print_query_kafka = """
                    STRING msg = (stringify(getvid(s)) + delimiter + {} + "\\n"),
                    INT kafka_errcode = write_to_kafka(producer, kafka_topic, getvid(s)%kafka_topic_partitions, "vertex_" + stringify(getvid(s)), msg),
                    IF kafka_errcode!=0 THEN 
                        @@kafka_error += ("Error sending data for vertex " + stringify(getvid(s)) + ": "+ stringify(kafka_errcode) + "\\n")
                    END""".format(print_attr)
            else:
                print_query_http = '@@v_batch += (stringify(getvid(s)) + "\\n")'
                print_query_kafka = """
                    STRING msg = (stringify(getvid(s)) + "\\n"),
                    INT kafka_errcode = write_to_kafka(producer, kafka_topic, getvid(s)%kafka_topic_partitions, "vertex_" + stringify(getvid(s)), msg),
                    IF kafka_errcode!=0 THEN 
                        @@kafka_error += ("Error sending data for vertex " + stringify(getvid(s)) + ": "+ stringify(kafka_errcode) + "\\n")
                    END"""
        query_replace["{VERTEXATTRSHTTP}"] = print_query_http
        query_replace["{VERTEXATTRSKAFKA}"] = print_query_kafka
        # Install query
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "vertex_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace, force=force, distributed=self.distributed_query)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        self._start_request(False)
            
        # Start reading thread.
        if not self.is_hetero:
            v_attr_types = next(iter(self._v_schema.values()))
        else:
            v_attr_types = self._v_schema
        self._reader = Thread(
            target=self._read_vertex_data,
            kwargs=dict(
                exit_event = self._exit_event,
                in_q = self._read_task_q,
                out_q = self._data_q,
                batch_size = self.batch_size,
                v_in_feats = self.attributes,
                v_out_labels = {} if self.is_hetero else [],
                v_extra_feats = {} if self.is_hetero else [],
                v_attr_types = v_attr_types,
                delimiter = self.delimiter,
                is_hetero = self.is_hetero,
                callback_fn = self.callback_fn
            )
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
        delimiter: str = "|",
        timeout: int = 300000,
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None
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
            delimiter,
            timeout,
            distributed_query,
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
            kafka_sasl_kerberos_service_name,
            kafka_sasl_kerberos_keytab,
            kafka_sasl_kerberos_principal,
            kafka_sasl_kerberos_domain_name,
            kafka_ssl_check_hostname,
            kafka_producer_ca_location,
            kafka_producer_certificate_location,
            kafka_producer_key_location,
            kafka_producer_key_password,
            kafka_consumer_ca_location,
            kafka_consumer_certificate_location,
            kafka_consumer_key_location,
            kafka_consumer_key_password,
            kafka_skip_produce,
            kafka_auto_offset_reset,
            kafka_del_topic_per_epoch,
            kafka_add_topic_per_epoch,
            callback_fn,
            kafka_group_id,
            kafka_topic
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
        self._vtypes = sorted(self._vtypes)
        self._etypes = sorted(self._etypes)
        # Initialize parameters for the query
        if batch_size:
            # batch size takes precedence over number of batches
            self.batch_size = batch_size
            self.num_batches = None
        else:
            # If number of batches is given, calculate batch size
            if filter_by:
                num_edges = 0
                for e_type in self._etypes:
                    tmp = self._graph.getEdgeStats(e_type)[e_type][filter_by if isinstance(filter_by, str) else filter_by[e_type]]["TRUE"]
                    if self._e_schema[e_type]["IsDirected"]:
                        num_edges += tmp
                    else:
                        num_edges += 2*tmp
            else:
                num_edges = 0
                for e_type in self._etypes:
                    tmp = self._graph.getEdgeCount(e_type)
                    if self._e_schema[e_type]["IsDirected"]:
                        num_edges += tmp
                    else:
                        num_edges += 2*tmp
            self.batch_size = math.ceil(num_edges / num_batches)
            self.num_batches = num_batches
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
        self._payload["delimiter"] = self.delimiter
        # Output
        self.add_self_loop = add_self_loop
        # Install query
        self.query_name = self._install_query()

    def _install_query(self, force: bool = False) -> str:
        # Install the right GSQL query for the loader.
        query_suffix = {
            "v_in_feats": self.v_in_feats,
            "v_out_labels": self.v_out_labels,
            "v_extra_feats": self.v_extra_feats,
            "e_in_feats": self.e_in_feats,
            "e_out_labels": self.e_out_labels,
            "e_extra_feats": self.e_extra_feats,
            "distributed_query": self.distributed_query
        }
        md5 = hashlib.md5()
        md5.update(json.dumps(query_suffix).encode())
        query_replace = {"{QUERYSUFFIX}": md5.hexdigest()}

        print_vertex_attr = ""
        print_edge_http = ""
        print_edge_kafka = ""
        if isinstance(self.v_in_feats, dict):
            # Multiple vertex types
            for idx, vtype in enumerate(self._vtypes):
                v_attr_names = (
                    self.v_in_feats.get(vtype, [])
                    + self.v_out_labels.get(vtype, [])
                    + self.v_extra_feats.get(vtype, [])
                )
                v_attr_types = self._v_schema[vtype]
                print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
                print_vertex_attr += """
                    {} s.type == "{}" THEN
                        ret = (s.type + delimiter + stringify(getvid(s)) {}+ "\\n")"""\
                    .format("IF" if idx==0 else "ELSE IF", 
                            vtype, 
                            "+ delimiter + {}".format(print_attr) if v_attr_names else "")
            print_vertex_attr += """
                    END"""
            # Multiple edge types
            for idx, etype in enumerate(self._etypes):
                e_attr_names = (
                    self.e_in_feats.get(etype, [])
                    + self.e_out_labels.get(etype, [])
                    + self.e_extra_feats.get(etype, [])
                )
                e_attr_types = self._e_schema[etype]
                print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
                print_edge_http += """
                    {} e.type == "{}" THEN
                        STRING e_msg = (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) {}+ "\\n"),
                        @@e_batch += (stringify(getvid(s))+e.type+stringify(getvid(t)) -> e_msg)"""\
                    .format("IF" if idx==0 else "ELSE IF", etype, 
                            "+ delimiter + " + print_attr if e_attr_names else "")
                print_edge_kafka += """
                    {} e.type == "{}" THEN
                        STRING e_msg = (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) {}+ "\\n"),
                        INT kafka_errcode = write_to_kafka(producer, kafka_topic, (getvid(s)+getvid(t))%kafka_topic_partitions, "edge_batch_" + stringify(getvid(s))+e.type+stringify(getvid(t)), e_msg),
                        IF kafka_errcode!=0 THEN 
                            @@kafka_error += ("Error sending edge data for edge " + stringify(getvid(s))+e.type+stringify(getvid(t)) + ": "+ stringify(kafka_errcode) + "\\n")
                        END""".format("IF" if idx==0 else "ELSE IF", etype, 
                                        "+ delimiter + " + print_attr if e_attr_names else "")
            print_edge_http += """
                        END"""
            print_edge_kafka += """
                        END"""
        else:
            # Ignore vertex types
            v_attr_names = self.v_in_feats + self.v_out_labels + self.v_extra_feats
            v_attr_types = next(iter(self._v_schema.values()))
            print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
            print_vertex_attr += """
                ret = (stringify(getvid(s)) {}+ "\\n")"""\
            .format("+ delimiter + " + print_attr if v_attr_names else "")
            # Ignore edge types
            e_attr_names = self.e_in_feats + self.e_out_labels + self.e_extra_feats
            e_attr_types = next(iter(self._e_schema.values()))
            print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
            print_edge_http += """
                STRING e_msg = (stringify(getvid(s)) + delimiter + stringify(getvid(t)) {}+ "\\n"),
                @@e_batch += (stringify(getvid(s))+e.type+stringify(getvid(t)) -> e_msg)"""\
                .format("+ delimiter + " + print_attr if e_attr_names else "")
            print_edge_kafka += """
                STRING e_msg = (stringify(getvid(s)) + delimiter + stringify(getvid(t)) {}+ "\\n"),
                INT kafka_errcode2 = write_to_kafka(producer, kafka_topic, (getvid(s)+getvid(t))%kafka_topic_partitions, "edge_batch_" + stringify(getvid(s))+e.type+stringify(getvid(t)), e_msg),
                IF kafka_errcode2!=0 THEN 
                    @@kafka_error += ("Error sending edge data for edge " + stringify(getvid(s))+e.type+stringify(getvid(t)) + ": "+ stringify(kafka_errcode2) + "\\n")
                END""".format("+ delimiter + " + print_attr if e_attr_names else "")
        query_replace["{VERTEXATTRS}"] = print_vertex_attr
        query_replace["{EDGEATTRSKAFKA}"] = print_edge_kafka
        query_replace["{EDGEATTRSHTTP}"] = print_edge_http

        # Install query
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "graph_loader.gsql",
        )
        sub_query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "graph_loader_sub.gsql",
        )
        return install_query_files(self._graph, [sub_query_path, query_path], query_replace, force=force, distributed=[False, self.distributed_query])

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        self._start_request(True)

        # Start reading thread.
        if not self.is_hetero:
            v_attr_types = next(iter(self._v_schema.values()))
            e_attr_types = next(iter(self._e_schema.values()))
        else:
            v_attr_types = self._v_schema
            e_attr_types = self._e_schema
        self._reader = Thread(
            target=self._read_graph_data,
            kwargs=dict(
                exit_event = self._exit_event,
                in_q = self._read_task_q,
                out_q = self._data_q,
                batch_size = self.batch_size,
                out_format = self.output_format,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = self.v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = self.e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                delimiter = self.delimiter,
                is_hetero = self.is_hetero,
                callback_fn = self.callback_fn
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
        e_seed_types: Union[str, list] = None,
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
        delimiter: str = "|",
        timeout: int = 300000,
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None, 
        num_machines: int = 1,
        num_segments: int = 20
    ) -> None:
        """NO DOC"""

        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            delimiter,
            timeout,
            distributed_query,
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
            kafka_sasl_kerberos_service_name,
            kafka_sasl_kerberos_keytab,
            kafka_sasl_kerberos_principal,
            kafka_sasl_kerberos_domain_name,
            kafka_ssl_check_hostname,
            kafka_producer_ca_location,
            kafka_producer_certificate_location,
            kafka_producer_key_location,
            kafka_producer_key_password,
            kafka_consumer_ca_location,
            kafka_consumer_certificate_location,
            kafka_consumer_key_location,
            kafka_consumer_key_password,
            kafka_skip_produce,
            kafka_auto_offset_reset,
            kafka_del_topic_per_epoch,
            kafka_add_topic_per_epoch,
            callback_fn,
            kafka_group_id,
            kafka_topic,
            num_machines,
            num_segments
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
        self._vtypes = sorted(self._vtypes)
        self._etypes = sorted(self._etypes)
        # Resolve seeds
        if e_seed_types:
            if isinstance(e_seed_types, list):
                self._seed_types = e_seed_types
            elif isinstance(e_seed_types, str):
                self._seed_types = [e_seed_types]
            else:
                raise TigerGraphException("e_seed_types must be either of type list or string.")
        elif isinstance(filter_by, dict):
            self._seed_types = list(filter_by.keys())
        else:
            self._seed_types = self._etypes
        if set(self._seed_types) - set(self._etypes):
            raise ValueError("Seed type has to be one of the edge types to retrieve")
        # Resolve number of batches
        if batch_size:
            # batch size takes precedence over number of batches
            self.batch_size = batch_size
            self.num_batches = None
        else:
            # If number of batches is given, calculate batch size
            if filter_by:
                num_edges = 0
                for e_type in self._seed_types:
                    tmp = self._graph.getEdgeStats(e_type)[e_type][filter_by if isinstance(filter_by, str) else filter_by[e_type]]["TRUE"]
                    if self._e_schema[e_type]["IsDirected"]:
                        num_edges += tmp
                    else:
                        num_edges += 2*tmp
            else:
                num_edges = 0
                for e_type in self._seed_types:
                    tmp = self._graph.getEdgeCount(e_type)
                    if self._e_schema[e_type]["IsDirected"]:
                        num_edges += tmp
                    else:
                        num_edges += 2*tmp
            if num_edges==0:
                raise ValueError("Cannot find any edge as seed. Please check the configuration and the data. If they all look right, please use batch_size instead of num_batches or refresh metadata following https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_parameters_15")
            self.batch_size = math.ceil(num_edges / num_batches)
            self.num_batches = num_batches          
        # Initialize parameters for the query
        self._payload["num_neighbors"] = num_neighbors
        self._payload["num_hops"] = num_hops
        self._payload["delimiter"] = delimiter
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

    def _install_query(self, force: bool = False):
        # Install the right GSQL query for the loader.
        query_suffix = {
            "v_in_feats": self.v_in_feats,
            "v_out_labels": self.v_out_labels,
            "v_extra_feats": self.v_extra_feats,
            "e_in_feats": self.e_in_feats,
            "e_out_labels": self.e_out_labels,
            "e_extra_feats": self.e_extra_feats,
            "distributed_query": self.distributed_query
        }
        md5 = hashlib.md5()
        md5.update(json.dumps(query_suffix).encode())
        query_replace = {"{QUERYSUFFIX}": md5.hexdigest()}

        if self.is_hetero:
            # Multiple vertex types
            print_query_seed = ""
            print_query_other = ""
            for idx, vtype in enumerate(self._vtypes):
                v_attr_names = (
                    self.v_in_feats.get(vtype, [])
                    + self.v_out_labels.get(vtype, [])
                    + self.v_extra_feats.get(vtype, [])
                )
                v_attr_types = self._v_schema[vtype]
                print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
                print_query_seed += """
                    {} s.type == "{}" THEN
                        @@v_batch += (s->(s.type + delimiter + stringify(getvid(s)) {}+ "\\n"))"""\
                    .format("IF" if idx==0 else "ELSE IF", vtype, 
                            "+ delimiter + " + print_attr if v_attr_names else "")
                print_query_other += """
                    {} s.type == "{}" THEN
                        @@v_batch += (tmp_seed->(s.type + delimiter + stringify(getvid(s)) {}+ "\\n"))"""\
                    .format("IF" if idx==0 else "ELSE IF", vtype, 
                            "+ delimiter + " + print_attr if v_attr_names else "")
            print_query_seed += """
                    END"""
            print_query_other += """
                    END"""
            query_replace["{SEEDVERTEXATTRS}"] = print_query_seed
            query_replace["{OTHERVERTEXATTRS}"] = print_query_other
            # Multiple edge types
            print_query = ""
            print_query_kafka = ""
            for idx, etype in enumerate(self._etypes):
                e_attr_names = (
                    self.e_in_feats.get(etype, [])
                    + self.e_out_labels.get(etype, [])
                    + self.e_extra_feats.get(etype, [])
                )
                e_attr_types = self._e_schema[etype]
                print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
                print_query += """
                    {} e.type == "{}" THEN
                        @@e_batch += (tmp_seed->(e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) {}+ "\\n"))"""\
                    .format("IF" if idx==0 else "ELSE IF", etype, 
                            "+ delimiter + " + print_attr if e_attr_names else "")
                print_query_kafka += """
                    {} e.type == "{}" THEN
                        SET<STRING> tmp_e = (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) {}+ "\\n", ""),
                        tmp_e_batch = tmp_e_batch UNION tmp_e"""\
                    .format("IF" if idx==0 else "ELSE IF", etype, 
                            "+ delimiter + " + print_attr if e_attr_names else "")
            print_query += """
                    END"""
            print_query_kafka += """
                    END"""
            query_replace["{EDGEATTRS}"] = print_query
            query_replace["{EDGEATTRSKAFKA}"] = print_query_kafka
        else:
            # Ignore vertex types
            v_attr_names = self.v_in_feats + self.v_out_labels + self.v_extra_feats
            v_attr_types = next(iter(self._v_schema.values()))
            print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
            print_query_seed = '@@v_batch += (s->(stringify(getvid(s)) {}+ "\\n"))'.format(
                "+ delimiter + " + print_attr if v_attr_names else ""
            )
            print_query_other = '@@v_batch += (tmp_seed->(stringify(getvid(s)) {}+ "\\n"))'.format(
                "+ delimiter + " + print_attr if v_attr_names else ""
            )
            query_replace["{SEEDVERTEXATTRS}"] = print_query_seed
            query_replace["{OTHERVERTEXATTRS}"] = print_query_other
            # Ignore edge types
            e_attr_names = self.e_in_feats + self.e_out_labels + self.e_extra_feats
            e_attr_types = next(iter(self._e_schema.values()))
            print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
            print_query = '@@e_batch += (tmp_seed->(stringify(getvid(s)) + delimiter + stringify(getvid(t)) {} + "\\n"))'.format(
                "+ delimiter + " + print_attr if e_attr_names else ""
            )
            query_replace["{EDGEATTRS}"] = print_query
            print_query = """SET<STRING> tmp_e = (stringify(getvid(s)) + delimiter + stringify(getvid(t)) {} + "\\n", ""),
                             tmp_e_batch = tmp_e_batch UNION tmp_e""".format(
                "+ delimiter + " + print_attr if e_attr_names else ""
            )
            query_replace["{EDGEATTRSKAFKA}"] = print_query
        # Install query
        query_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "gsql",
                "dataloaders",
                "edge_nei_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace, force=force, distributed=self.distributed_query)

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        self._start_request(True)

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
            target=self._read_graph_data,
            kwargs=dict(
                exit_event = self._exit_event,
                in_q = self._read_task_q,
                out_q = self._data_q,
                batch_size = self.batch_size,
                out_format = self.output_format,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = self.v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                delimiter = self.delimiter,
                is_hetero = self.is_hetero,
                callback_fn = self.callback_fn,
                seed_type = "edge"
            ),
        )
        self._reader.start()

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all data if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself"""
        return super().data


class NodePieceLoader(BaseLoader):
    """NodePieceLoader.

    A data loader that performs NodePiece sampling from the graph.
    You can declare a `NodePieceLoader` instance with the factory function `nodePieceLoader()`.

    A NodePiece loader is an iterable.
    When you loop through a loader instance, it loads one batch of data from the graph to which you established a connection.

    In every iteration, the NodePiece loader selects a group of seed vertices of size batch size. 
    For each vertex in the batch, it will produce a set of the k closest "anchor" vertices in the graph,
    as well as up to j edge types. For more information on the NodePiece data loading scheme, the
    https://towardsdatascience.com/nodepiece-tokenizing-knowledge-graphs-6dd2b91847aa[blog article] and
    https://arxiv.org/abs/2106.12144[paper] are good places to start.

    You can iterate on the instance until every vertex has been picked as seed.

    Examples:

    The following example iterates over an NodePiece loader instance.
    [.wrap,python]
    ----
    for i, batch in enumerate(node_piece_loader):
        print("----Batch {}----".format(i))
        print(batch)
    ----
    """
    def __init__(
        self,
        graph: "TigerGraphConnection",
        v_feats: Union[list, dict] = None,
        target_vertex_types: Union[str, list] = None,
        compute_anchors: bool = False,
        use_cache: bool = False,
        clear_cache: bool = False,
        anchor_method: str = "random",
        anchor_cache_attr: str = "anchors",
        special_tokens: list = ["MASK", "CLS", "SEP"],
        max_distance: int = 5,
        max_anchors: int = 10,
        max_relational_context: int = 10,
        anchor_percentage: float = 0.01,
        anchor_attribute: str = "is_anchor",
        tokenMap: Union[dict, str] = None,
        e_types: list = None,
        global_schema_change: bool = False,
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
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None
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
            delimiter,
            timeout,
            distributed_query,
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
            kafka_sasl_kerberos_service_name,
            kafka_sasl_kerberos_keytab,
            kafka_sasl_kerberos_principal,
            kafka_sasl_kerberos_domain_name,
            kafka_ssl_check_hostname,
            kafka_producer_ca_location,
            kafka_producer_certificate_location,
            kafka_producer_key_location,
            kafka_producer_key_password,
            kafka_consumer_ca_location,
            kafka_consumer_certificate_location,
            kafka_consumer_key_location,
            kafka_consumer_key_password,
            kafka_skip_produce,
            kafka_auto_offset_reset,
            kafka_del_topic_per_epoch,
            kafka_add_topic_per_epoch,
            callback_fn,
            kafka_group_id,
            kafka_topic
        )
        # Resolve attributes
        is_hetero = isinstance(v_feats, dict)
        self.is_hetero = is_hetero
        self.anchor_cache_attr = anchor_cache_attr
        self.attributes = self._validate_vertex_attributes(v_feats, is_hetero)
        self._anchor_perc = anchor_percentage
        self.num_edge_batches = 10
        if is_hetero:
            self._vtypes = list(self.attributes.keys())
            if not self._vtypes:
                self._vtypes = list(self._v_schema.keys())
        else:
            self._vtypes = list(self._v_schema.keys())
        self._vtypes = sorted(self._vtypes)
        # Initialize parameters for the query
        if isinstance(target_vertex_types, str):
            self._seed_types = [target_vertex_types]
            self._target_v_types = target_vertex_types
        elif isinstance(target_vertex_types, list):
            self._seed_types = target_vertex_types
            self._target_v_types = target_vertex_types
        else:
            self._seed_types = self._vtypes
            self._target_v_types = self._vtypes

        if batch_size:
            # batch size takes precedence over number of batches
            self.batch_size = batch_size
            self.num_batches = None
        else:
            if not filter_by:
                num_vertices = sum(self._graph.getVertexCount(self._seed_types).values())
            elif isinstance(filter_by, str):
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
            self.batch_size = math.ceil(num_vertices / num_batches)
            self.num_batches = num_batches
        self.filter_by = filter_by
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
        self._payload["seed_types"] = self._seed_types
        self._payload["max_distance"] = max_distance
        self._payload["max_anchors"] = max_anchors
        self._payload["max_rel_context"] = max_relational_context
        self._payload["anchor_attr"] = anchor_attribute
        self._payload["use_cache"] = use_cache
        self._payload["clear_cache"] = clear_cache
        self._payload["delimiter"] = delimiter
        self._payload["input_vertices"] = []
        self._payload["num_edge_batches"] = self.num_edge_batches
        if e_types:
            self._payload["e_types"] = e_types
        elif e_types == []:
            self._payload["e_types"] = e_types
        else:
            self._payload["e_types"] = list(self._e_schema.keys())
            e_types = list(self._e_schema.keys())
        # Compute Anchors
        if compute_anchors:
            to_change = []
            for v_type in self._vtypes:
                if anchor_attribute not in self._v_schema[v_type].keys():
                    to_change.append(v_type)
            if to_change:
                print("Adding anchor attribute")
                ret = add_attribute(self._graph, "VERTEX", "BOOL", anchor_attribute, to_change, global_change=global_schema_change)
                print(ret)
            self._compute_anchors(anchor_attribute, anchor_method)
        if anchor_cache_attr:
            to_change = []
            for v_type in self._vtypes:
                if anchor_cache_attr not in self._v_schema[v_type].keys():
                    # add anchor cache attribute
                    to_change.append(v_type)
            if to_change:
                print("Adding anchor cache attribute")
                ret = add_attribute(self._graph, "VERTEX", "MAP<INT, INT>", anchor_cache_attr, to_change, global_change=global_schema_change)
                print(ret)
        # Install query
        self.query_name = self._install_query()

        if self.is_hetero:
            for key in self.attributes.keys():
                self.attributes[key] = ["relational_context", "closest_anchors"] + self.attributes[key]
        else:
            self.attributes = ["relational_context", "closest_anchors"] + self.attributes

        # Get number of tokens for embedding table
        if tokenMap:
            if isinstance(tokenMap, dict):
                self.idToIdx = tokenMap
            elif isinstance(tokenMap, str):
                self.idToIdx = pickle.load(open(tokenMap, "rb"))
        else:
            self.idToIdx = {}
            self.curIdx = 0
            self.specialTokens = ["PAD"] + special_tokens
            self.baseTokens = self.specialTokens + ["dist_"+str(i) for i in range(self._payload["max_distance"]+1)] + e_types
            query_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "gsql",
                "dataloaders",
                "get_anchors.gsql",
            )
            params = {"anchor_attr": anchor_attribute, "v_types": self._vtypes}
            install_query_file(self._graph, query_path)
            ancs = self._graph.runInstalledQuery("get_anchors", params=params, timeout=self.timeout)[0]["@@vids"]
            print("Number of Anchors:", len(ancs))
            for tok in self.baseTokens + ancs:
                self.idToIdx[str(tok)] = self.curIdx
                self.curIdx += 1
            
        self.num_tokens = len(self.idToIdx.keys())

    def saveTokens(self, filename) -> None:
        """Save tokens to pickle file
        Args:
            filename (str):
                Filename to save the tokens to.
        """
        pickle.dump(self.idToIdx, open(filename, "wb"))

    def _compute_anchors(self, anchor_attr, method="random") -> str:
        if method.lower() == "random":
            query_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "gsql",
                "splitters",
                "random_anchor_selection.gsql",
            )
            install_query_file(self._graph, query_path)
            params = {
                "percentage": self._anchor_perc,
                "v_type": self._vtypes,
                "tgt_v_type": self._target_v_types,
                "anchor_attr": anchor_attr,
                "random_seed": 42
            }
            if self.filter_by:
                if isinstance(self.filter_by, str):
                    params["filter_by"] = self.filter_by
                else:
                    attr = set(self.filter_by.values())
                    if len(attr) != 1:
                        raise NotImplementedError("Filtering by different attributes for different vertex types is not supported. Please use the same attribute for different types.")
            self._graph.runInstalledQuery("random_anchor_selection", params=params)
        else:
            raise NotImplementedError("{} anchor selection method is not supported. Please try 'random' anchor selection method".format(method))

    def _install_query(self, force: bool = False) -> str:
        # Install the right GSQL query for the loader.
        query_suffix = []
        query_replace = {}

        if isinstance(self.attributes, dict):
            # Multiple vertex types
            print_query_kafka = ""
            print_query_http = ""
            print_query = ""
            for idx, vtype in enumerate(self._seed_types):
                v_attr_names = self.attributes.get(vtype, [])
                query_suffix.extend(v_attr_names)
                v_attr_types = self._v_schema[vtype]
                print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
                print_query_http += """
                    {} s.type == "{}" THEN
                        @@v_batch += (s.type + delimiter + stringify(getvid(s)) + delimiter + s.@rel_context_set + delimiter + s.@ancs {}+ "\\n")"""\
                    .format("IF" if idx==0 else "ELSE IF", vtype, 
                            "+ delimiter + " + print_attr if v_attr_names else "")
                print_query_kafka += """
                    {} s.type == "{}" THEN 
                        STRING msg = (s.type + delimiter + stringify(getvid(s)) + delimiter + s.@rel_context_set + delimiter + s.@ancs {}+ "\\n"),
                        INT kafka_errcode = write_to_kafka(producer, kafka_topic, getvid(s)%kafka_topic_partitions, "vertex_" + stringify(getvid(s)), msg),
                        IF kafka_errcode!=0 THEN 
                            @@kafka_error += ("Error sending data for vertex " + stringify(getvid(s)) + ": "+ stringify(kafka_errcode) + "\\n")
                        END""".format("IF" if idx==0 else "ELSE IF", vtype, 
                                      "+ delimiter + " + print_attr if v_attr_names else "")
            print_query_http += """
                    END"""
            print_query_kafka += """
                    END"""
            query_suffix = list(dict.fromkeys(query_suffix))
        else:
            # Ignore vertex types
            v_attr_names = self.attributes
            query_suffix.extend(v_attr_names)
            v_attr_types = next(iter(self._v_schema.values()))
            print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
            print_query_http = '@@v_batch += (stringify(getvid(s)) + delimiter + s.@rel_context_set + delimiter + s.@ancs {}+ "\\n")'.format(
                "+ delimiter + " + print_attr if v_attr_names else ""
            )
            print_query_kafka = """
                STRING msg = (stringify(getvid(s)) + delimiter + s.@rel_context_set + delimiter + s.@ancs {}+ "\\n"),
                INT kafka_errcode = write_to_kafka(producer, kafka_topic, getvid(s)%kafka_topic_partitions, "vertex_" + stringify(getvid(s)), msg),
                IF kafka_errcode!=0 THEN 
                    @@kafka_error += ("Error sending data for vertex " + stringify(getvid(s)) + ": "+ stringify(kafka_errcode) + "\\n")
                END""".format("+ delimiter + " + print_attr if v_attr_names else "")
        query_replace["{VERTEXATTRSHTTP}"] = print_query_http
        query_replace["{VERTEXATTRSKAFKA}"] = print_query_kafka
        md5 = hashlib.md5()
        query_suffix.extend([self.distributed_query])
        md5.update(json.dumps(query_suffix).encode())
        query_replace["{QUERYSUFFIX}"] = md5.hexdigest()
        query_replace["{ANCHOR_CACHE_ATTRIBUTE}"] = self.anchor_cache_attr
        # Install query
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "nodepiece_loader.gsql",
        )
        return install_query_file(self._graph, query_path, query_replace, force=force, distributed=self.distributed_query)

    def reinstall_query(self) -> str:
        """Reinstall the dataloader query.

        Returns:
            The name of the query installed (str)
        """
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "get_anchors.gsql",
        )
        install_query_file(self._graph, query_path, force=True)
        query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "splitters",
            "random_anchor_selection.gsql",
        )
        install_query_file(self._graph, query_path, force=True)
        return self._install_query(force=True)

    def nodepiece_process(self, data):
        """NO DOC"""
        def processRelContext(row):
            context = row.split(" ")[:-1]
            context = [self.idToIdx[str(x)] for x in context][:self._payload["max_rel_context"]]
            context = context + [self.idToIdx["PAD"] for x in range(len(context), self._payload["max_rel_context"])]
            return context
        
        def processAnchors(row):
            try:
                ancs = row.split(" ")[:-1]
            except:
                ancs = []
            dists = []
            toks = []
            for anc in ancs:
                tmp = anc.split(":")
                dists.append(self.idToIdx["dist_"+str(tmp[1])])
                toks.append(self.idToIdx[str(tmp[0])])
            dists += [self.idToIdx["PAD"] for x in range(len(dists), self._payload["max_anchors"])]
            toks += [self.idToIdx["PAD"] for x in range(len(toks), self._payload["max_anchors"])]
            return {"ancs":toks, "dists": dists}
        
        if self.is_hetero:
            for v_type in data.keys():
                data[v_type]["relational_context"] = data[v_type]["relational_context"].apply(lambda x: processRelContext(x))
                ancs = data[v_type]["closest_anchors"].apply(lambda x: processAnchors(x))
                ancs = pd.DataFrame(list(ancs))
                data[v_type].drop(columns="closest_anchors", inplace=True)
                data[v_type]["anchors"] = ancs["ancs"]
                data[v_type]["anchor_distances"] = ancs["dists"]
        else:
            data["relational_context"] = data["relational_context"].apply(lambda x: processRelContext(x))
            ancs = data["closest_anchors"].apply(lambda x: processAnchors(x))
            ancs = pd.DataFrame(list(ancs))
            data.drop(columns="closest_anchors", inplace=True)
            data["anchors"] = ancs["ancs"]
            data["anchor_distances"] = ancs["dists"]
        if self.callback_fn:
            return self.callback_fn(data)
        else:
            return data

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        self._start_request(False)
            
        # Start reading thread.
        if not self.is_hetero:
            v_attr_types = next(iter(self._v_schema.values()))
        else:
            v_attr_types = self._v_schema
        self._reader = Thread(
            target=self._read_vertex_data,
            kwargs=dict(
                exit_event = self._exit_event,
                in_q = self._read_task_q,
                out_q = self._data_q,
                batch_size = self.batch_size,
                v_in_feats = self.attributes,
                v_out_labels = {} if self.is_hetero else [],
                v_extra_feats = {} if self.is_hetero else [],
                v_attr_types = v_attr_types,
                delimiter = self.delimiter,
                is_hetero = self.is_hetero,
                callback_fn = self.nodepiece_process
            ),
        )
        self._reader.start()

    @property
    def data(self) -> Any:
        """A property of the instance.
        The `data` property stores all data if all data is loaded in a single batch.
        If there are multiple batches of data, the `data` property returns the instance itself."""
        return super().data

    def fetch(self, vertices: list) -> None:
        """Fetch NodePiece results (anchors, distances, and relational context) for specific vertices.

        Args:
            vertices (list of dict):
                Vertices to fetch with their NodePiece results.
                Each vertex corresponds to a dict with two mandatory keys
                {"primary_id": ..., "type": ...}
        """
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
        _payload = {}
        _payload["v_types"] = self._vtypes
        _payload["max_distance"] = self._payload["max_distance"]
        _payload["max_anchors"] = self._payload["max_anchors"]
        _payload["max_rel_context"] = self._payload["max_rel_context"]
        _payload["anchor_attr"] = self._payload["anchor_attr"]
        _payload["use_cache"] = self._payload["use_cache"]
        _payload["clear_cache"] = self._payload["clear_cache"]
        _payload["e_types"] = self._payload["e_types"]
        _payload["seed_types"] = []
        _payload["delimiter"] = self._payload["delimiter"]
        _payload["input_vertices"] = []
        for i in vertices:
            _payload["input_vertices"].append({"id": i["primary_id"], "type": i["type"]})
        resp = self._graph.runInstalledQuery(
            self.query_name, params=_payload, timeout=self.timeout, usePost=True
        )
        attributes = self.attributes
        if not self.is_hetero:
            v_attr_types = next(iter(self._v_schema.values()))
        else:
            v_attr_types = self._v_schema
        vertex_batch = set()
        for i in resp:
            if "pids" in i:
                break
            vertex_batch.add(i["data_batch"])
        data = BaseLoader._parse_vertex_data(
            raw = vertex_batch,
            v_in_feats = attributes,
            v_out_labels = {} if self.is_hetero else [],
            v_extra_feats = {} if self.is_hetero else [],
            v_attr_types = v_attr_types,
            delimiter = self.delimiter,
            is_hetero = self.is_hetero
        )
        if not self.is_hetero:
            for column in data.columns:
                data[column] = pd.to_numeric(data[column], errors="ignore")
        else:
            for key in data:
                for column in data[key].columns:
                    data[key][column] = pd.to_numeric(data[key][column], errors="ignore")
        data = self.nodepiece_process(data)
        return data

    def precompute(self) -> None:
        """Compute NodePiece results (anchors and their distances) to cache attribute.
        """
        _payload = dict(self._payload)
        _payload["precompute"] = True
        resp = self._graph.runInstalledQuery(
            self.query_name, params=_payload, timeout=self.timeout, usePost=True
        )
       

class HGTLoader(BaseLoader):
    """HGTLoader

    A data loader that performs stratified neighbor sampling as in  
    link:https://arxiv.org/abs/2003.01332[Heterogeneous Graph Transformer].
    You can declare a `HGTLoader` instance with the factory function `hgtLoader()`.

    A HGT loader is an iterable.
    When you loop through a HGT loader instance, it loads one batch of data at a time from the graph.

    In every iteration, it first chooses a specified number of vertices as seeds,
    then picks a specified number of neighbors of each type at random,
    then the specified number of neighbors of every type of each neighbor, and repeat for a specified number of hops.
    It loads both the vertices and the edges connecting them to their neighbors.
    The vertices sampled this way along with their edges form one subgraph and is contained in one batch.

    You can iterate on the instance until every vertex has been picked as seed.

    Examples:

    The following example iterates over a HGT loader instance.
    [.wrap,python]
    ----
    for i, batch in enumerate(hgt_loader):
        print("----Batch {}----".format(i))
        print(batch)
    ----

    See more details about the specific sampling method in
    link:https://arxiv.org/abs/2003.01332[Heterogeneous Graph Transformer].
    """
    def __init__(
        self,
        graph: "TigerGraphConnection",
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
        filter_by: Union[str, dict] = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        reverse_edge: bool = False,
        delimiter: str = "|",
        timeout: int = 300000,
        distributed_query: bool = False,
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
        callback_fn: Callable = None,
        kafka_group_id: str = None,
        kafka_topic: str = None
    ) -> None:
        """NO DOC"""

        super().__init__(
            graph,
            loader_id,
            num_batches,
            buffer_size,
            output_format,
            reverse_edge,
            delimiter,
            timeout,
            distributed_query,
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
            kafka_sasl_kerberos_service_name,
            kafka_sasl_kerberos_keytab,
            kafka_sasl_kerberos_principal,
            kafka_sasl_kerberos_domain_name,
            kafka_ssl_check_hostname,
            kafka_producer_ca_location,
            kafka_producer_certificate_location,
            kafka_producer_key_location,
            kafka_producer_key_password,
            kafka_consumer_ca_location,
            kafka_consumer_certificate_location,
            kafka_consumer_key_location,
            kafka_consumer_key_password,
            kafka_skip_produce,
            kafka_auto_offset_reset,
            kafka_del_topic_per_epoch,
            kafka_add_topic_per_epoch,
            callback_fn,
            kafka_group_id,
            kafka_topic
        )
        self.num_neighbors = num_neighbors
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
            raise ValueError("HGTLoader only works with heterogeneous graphs. Please use the dict format for the feature parameters.")
        self._vtypes = sorted(self._vtypes)
        self._etypes = sorted(self._etypes)
        # Resolve seeds
        if v_seed_types:
            if isinstance(v_seed_types, list):
                self._seed_types = v_seed_types
            elif isinstance(v_seed_types, str):
                self._seed_types = [v_seed_types]
            else:
                raise TigerGraphException("v_seed_types must be either of type list or string.")
        elif isinstance(filter_by, dict):
            self._seed_types = list(filter_by.keys())
        else:
            self._seed_types = self._vtypes
        if set(self._seed_types) - set(self._vtypes):
            raise ValueError("Seed type has to be one of the vertex types to retrieve")

        if batch_size:
            # batch size takes precedence over number of batches
            self.batch_size = batch_size
            self.num_batches = None
        else:
            # If number of batches is given, calculate batch size
            if not filter_by:
                num_vertices = sum(self._graph.getVertexCount(self._seed_types).values())
            elif isinstance(filter_by, str):
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by))
                    for k in self._seed_types
                )
            elif isinstance(filter_by, dict):
                num_vertices = sum(
                    self._graph.getVertexCount(k, where="{}!=0".format(filter_by[k]))
                    for k in self._seed_types
                )
            else:
                raise ValueError("filter_by should be None, attribute name, or dict of {type name: attribute name}.")
            self.batch_size = math.ceil(num_vertices / num_batches)
            self.num_batches = num_batches
        # Initialize parameters for the query
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
        self._payload["delimiter"] = self.delimiter
        self._payload["input_vertices"] = []
        # Output
        self.add_self_loop = add_self_loop
        # Install query
        self.query_name = self._install_query()

    def _install_query(self, force: bool = False):
        # Install the right GSQL query for the loader.
        query_suffix = {
            "num_neighbors": self.num_neighbors,
            "v_in_feats": self.v_in_feats,
            "v_out_labels": self.v_out_labels,
            "v_extra_feats": self.v_extra_feats,
            "e_in_feats": self.e_in_feats,
            "e_out_labels": self.e_out_labels,
            "e_extra_feats": self.e_extra_feats,
            "distributed_query": self.distributed_query
        }
        md5 = hashlib.md5()
        md5.update(json.dumps(query_suffix).encode())
        query_replace = {"{QUERYSUFFIX}": md5.hexdigest()}

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
                v_attr_types = self._v_schema[vtype]
                print_attr = self._generate_attribute_string("vertex", v_attr_names, v_attr_types)
                print_query_seed += """
                    {} s.type == "{}" THEN
                        @@v_batch += (s.type + delimiter + stringify(getvid(s)) {} + delimiter + "1\\n")"""\
                    .format("IF" if idx==0 else "ELSE IF", 
                            vtype, 
                            "+ delimiter + " + print_attr if v_attr_names else "")
                print_query_other += """
                    {} s.type == "{}" THEN
                        @@v_batch += (s.type + delimiter + stringify(getvid(s)) {} + delimiter + "0\\n")"""\
                    .format("IF" if idx==0 else "ELSE IF", 
                            vtype, 
                            "+ delimiter + " + print_attr if v_attr_names else "")
            print_query_seed += """
                    END"""
            print_query_other += """
                    END"""
            query_replace["{SEEDVERTEXATTRS}"] = print_query_seed
            query_replace["{OTHERVERTEXATTRS}"] = print_query_other
            # Generate select for each type of neighbors
            print_select = ""
            seeds = []
            vidx = 0
            for vtype in self.num_neighbors:
                # Print edges and attributes
                print_query = ""
                eidx = 0
                for etype in self._etypes:
                    e_attr_names = (
                        self.e_in_feats.get(etype, [])
                        + self.e_out_labels.get(etype, [])
                        + self.e_extra_feats.get(etype, [])
                    )
                    e_attr_types = self._e_schema[etype]
                    if vtype!=e_attr_types["FromVertexTypeName"] and vtype!=e_attr_types["ToVertexTypeName"]:
                        continue
                    print_attr = self._generate_attribute_string("edge", e_attr_names, e_attr_types)
                    print_query += """
                        {} e.type == "{}" THEN
                            @@e_batch += (e.type + delimiter + stringify(getvid(s)) + delimiter + stringify(getvid(t)) {} + "\\n")"""\
                        .format("IF" if eidx==0 else "ELSE IF", 
                                etype,                                 
                                "+ delimiter + " + print_attr if e_attr_names else "")
                    eidx += 1
                if print_query:   
                    print_query += """
                        END"""
                    print_select += """
            seed{} = SELECT t
                FROM seeds:s -(e_types:e)- {}:t 
                SAMPLE {} EDGE WHEN s.outdegree() >= 1
                ACCUM
                    IF NOT @@printed_edges.contains(e) THEN
                        @@printed_edges += e,
                        {}
                    END;""".format(vidx, vtype, self.num_neighbors[vtype], print_query)
                    seeds.append("seed{}".format(vidx))
                    vidx += 1          
            print_select += """
            seeds = {};""".format(" UNION ".join(seeds))
            query_replace["{SELECTNEIGHBORS}"] = print_select
        # Install query
        query_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "gsql",
                "dataloaders",
                "hgt_loader.gsql",
        )
        sub_query_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "gsql",
            "dataloaders",
            "hgt_loader_sub.gsql",
        )
        return install_query_files(self._graph, [sub_query_path, query_path], query_replace, force=force, distributed=[False, self.distributed_query])

    def _start(self) -> None:
        # Create task and result queues
        self._read_task_q = Queue(self.buffer_size)
        self._data_q = Queue(self.buffer_size)
        self._exit_event = Event()

        self._start_request(True)

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
            target=self._read_graph_data,
            kwargs=dict(
                exit_event = self._exit_event,
                in_q = self._read_task_q,
                out_q = self._data_q,
                batch_size = self.batch_size,
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
                delimiter = self.delimiter,
                is_hetero = self.is_hetero,
                callback_fn = self.callback_fn
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
        _payload["num_hops"] = self._payload["num_hops"]
        _payload["delimiter"] = self._payload["delimiter"]
        _payload["input_vertices"] = []
        for i in vertices:
            _payload["input_vertices"].append({"id": i["primary_id"], "type": i["type"]})
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
        vertex_batch = set()
        edge_batch = set()
        for i in resp:
            if "pids" in i:
                break
            vertex_batch.update(i["vertex_batch"].splitlines())
            edge_batch.update(i["edge_batch"].splitlines())
        data = self._parse_graph_data_to_df(
            raw = (vertex_batch, edge_batch),
            v_in_feats = self.v_in_feats,
            v_out_labels = self.v_out_labels,
            v_extra_feats = v_extra_feats,
            v_attr_types = v_attr_types,
            e_in_feats = self.e_in_feats,
            e_out_labels = self.e_out_labels,
            e_extra_feats = self.e_extra_feats,
            e_attr_types = e_attr_types,
            delimiter = self.delimiter,
            primary_id = i["pids"],
            is_hetero = self.is_hetero,
        )
        if self.output_format == "dataframe" or self.output_format== "df":
            vertices, edges = data
            if not self.is_hetero:
                for column in vertices.columns:
                    vertices[column] = pd.to_numeric(vertices[column], errors="ignore")
                for column in edges.columns:
                    edges[column] = pd.to_numeric(edges[column], errors="ignore")
            else:
                for key in vertices:
                    for column in vertices[key].columns:
                        vertices[key][column] = pd.to_numeric(vertices[key][column], errors="ignore")
                for key in edges:
                    for column in edges[key].columns:
                        edges[key][column] = pd.to_numeric(edges[key][column], errors="ignore")
            data = (vertices, edges)
        elif self.output_format == "pyg":
            try:
                import torch
            except ImportError:
                raise ImportError(
                    "PyTorch is not installed. Please install it to use PyG or DGL output."
                )
            try:
                import torch_geometric as pyg
            except ImportError:
                raise ImportError(
                    "PyG is not installed. Please install PyG to use PyG format."
                )
            data = BaseLoader._parse_df_to_pyg(
                raw = data,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = self.e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                is_hetero = self.is_hetero,
                torch = torch,
                pyg = pyg
            )
        elif self.output_format == "dgl":
            try:
                import torch
            except ImportError:
                raise ImportError(
                    "PyTorch is not installed. Please install it to use PyG or DGL output."
                )
            try:
                import dgl
            except ImportError:
                raise ImportError(
                    "DGL is not installed. Please install DGL to use DGL format."
                )
            data = BaseLoader._parse_df_to_dgl(
                raw = data,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = self.e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                is_hetero = self.is_hetero,
                torch = torch,
                dgl= dgl
            )
        elif self.output_format == "spektral" and self.is_hetero==False:
            try:
                import tensorflow as tf
            except ImportError:
                raise ImportError(
                    "Tensorflow is not installed. Please install it to use spektral output."
                )
            try:
                import scipy
            except ImportError:
                raise ImportError(
                    "scipy is not installed. Please install it to use spektral output."
                )
            try:
                import spektral
            except ImportError:
                raise ImportError(
                    "Spektral is not installed. Please install it to use spektral output."
                )
            data = BaseLoader._parse_df_to_spektral(
                raw = data,
                v_in_feats = self.v_in_feats,
                v_out_labels = self.v_out_labels,
                v_extra_feats = v_extra_feats,
                v_attr_types = v_attr_types,
                e_in_feats = self.e_in_feats,
                e_out_labels = self.e_out_labels,
                e_extra_feats = self.e_extra_feats,
                e_attr_types = e_attr_types,
                add_self_loop = self.add_self_loop,
                is_hetero = self.is_hetero,
                scipy = scipy,
                spektral = spektral
            )
        else:
            raise NotImplementedError
        if self.callback_fn:
            data = self.callback_fn(data)
        # Return data
        return data
