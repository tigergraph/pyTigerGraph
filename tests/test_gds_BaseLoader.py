import io
import unittest
from queue import Queue
from threading import Event

import pandas as pd
import torch
from dgl import DGLGraph
from pandas.testing import assert_frame_equal
from pyTigerGraphUnitTest import make_connection
from torch.testing import assert_close as assert_close_torch
from torch_geometric.data import Data as pygData
from torch_geometric.data import HeteroData as pygHeteroData

from pyTigerGraph.gds.dataloaders import BaseLoader


class TestGDSBaseLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = make_connection(graphname="Cora")
        cls.loader = BaseLoader(cls.conn, delimiter="|")

    def test_get_schema(self):
        self.assertDictEqual(
            self.loader._v_schema,
            {
                "Paper": {
                    "x": "LIST:INT",
                    "y": "INT",
                    "train_mask": "BOOL",
                    "val_mask": "BOOL",
                    "test_mask": "BOOL",
                    "id": "INT",
                }
            },
        )
        self.assertDictEqual(
            self.loader._e_schema,
            {
                "Cite": {
                    "FromVertexTypeName": "Paper",
                    "ToVertexTypeName": "Paper",
                    "time": "INT",
                    "is_train": "BOOL",
                    "is_val": "BOOL",
                    "IsDirected": True,
                }
            },
        )

    def test_get_schema_no_primary_id_attr(self):
        conn = make_connection(graphname="Social")
        loader = BaseLoader(conn, delimiter="|")
        self.assertDictEqual(
            loader._v_schema,
            {
                "Person": {
                    "name": "STRING",
                    "age": "INT",
                    "gender": "STRING",
                    "state": "STRING",
                }
            },
        )
        self.assertDictEqual(
            loader._e_schema,
            {
                "Friendship": {
                    "FromVertexTypeName": "Person",
                    "ToVertexTypeName": "Person",
                    "connect_day": "DATETIME",
                    "duration": "LIST:STRING",
                    "IsDirected": False,
                }
            },
        )

    def test_validate_vertex_attributes(self):
        # Empty input
        self.assertListEqual(self.loader._validate_vertex_attributes(None), [])
        self.assertListEqual(self.loader._validate_vertex_attributes([]), [])
        self.assertListEqual(self.loader._validate_vertex_attributes({}), [])
        self.assertDictEqual(
            self.loader._validate_vertex_attributes(None, True), {})
        self.assertDictEqual(
            self.loader._validate_vertex_attributes([], True), {})
        self.assertDictEqual(
            self.loader._validate_vertex_attributes({}, True), {})
        # Extra spaces
        self.assertListEqual(
            self.loader._validate_vertex_attributes(["x ", " y"]), ["x", "y"]
        )
        self.assertDictEqual(
            self.loader._validate_vertex_attributes(
                {"Paper": ["x ", " y"]}, True),
            {"Paper": ["x", "y"]},
        )
        # Wrong input
        with self.assertRaises(ValueError):
            self.loader._validate_vertex_attributes("x")
        with self.assertRaises(ValueError):
            self.loader._validate_vertex_attributes(["nonexist"])
        with self.assertRaises(ValueError):
            self.loader._validate_vertex_attributes({"Paper": ["nonexist"]})
        with self.assertRaises(ValueError):
            self.loader._validate_vertex_attributes(1)
        with self.assertRaises(ValueError):
            self.loader._validate_vertex_attributes(["x"], is_hetero=True)
        with self.assertRaises(ValueError):
            self.loader._validate_vertex_attributes(
                {"Paper": ["x"]}, is_hetero=False)

    def test_validate_edge_attributes(self):
        # Empty input
        self.assertListEqual(self.loader._validate_edge_attributes(None), [])
        self.assertListEqual(self.loader._validate_edge_attributes([]), [])
        self.assertListEqual(self.loader._validate_edge_attributes({}), [])
        self.assertDictEqual(
            self.loader._validate_edge_attributes(None, True), {})
        self.assertDictEqual(
            self.loader._validate_edge_attributes([], True), {})
        self.assertDictEqual(
            self.loader._validate_edge_attributes({}, True), {})
        # Extra spaces
        self.assertListEqual(
            self.loader._validate_edge_attributes(["time ", "is_train"]),
            ["time", "is_train"],
        )
        self.assertDictEqual(
            self.loader._validate_edge_attributes(
                {"Cite": ["time ", "is_train"]}, True
            ),
            {"Cite": ["time", "is_train"]},
        )
        # Wrong input
        with self.assertRaises(ValueError):
            self.loader._validate_edge_attributes("time")
        with self.assertRaises(ValueError):
            self.loader._validate_edge_attributes(["nonexist"])
        with self.assertRaises(ValueError):
            self.loader._validate_edge_attributes({"Cite": ["nonexist"]})
        with self.assertRaises(ValueError):
            self.loader._validate_edge_attributes(1)
        with self.assertRaises(ValueError):
            self.loader._validate_edge_attributes(["time"], is_hetero=True)
        with self.assertRaises(ValueError):
            self.loader._validate_edge_attributes(
                {"Cite": ["time"]}, is_hetero=False)

    def test_read_vertex(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = "99|1 0 0 1 |1|0|1\n8|1 0 0 1 |1|1|1\n"
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "vertex",
            "dataframe",
            ["x"],
            ["y"],
            ["train_mask", "is_seed"],
            {"x": "INT", "y": "INT", "train_mask": "BOOL", "is_seed": "BOOL"},
            delimiter="|"
        )
        data = data_q.get()
        truth = pd.read_csv(
            io.StringIO(raw),
            header=None,
            names=["vid", "x", "y", "train_mask", "is_seed"],
            sep=self.loader.delimiter
        )
        assert_frame_equal(data, truth)
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_vertex_callback(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = "99|1 0 0 1 |1|0|1\n8|1 0 0 1 |1|1|1\n"
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "vertex",
            "dataframe",
            ["x"],
            ["y"],
            ["train_mask", "is_seed"],
            {"x": "INT", "y": "INT", "train_mask": "BOOL", "is_seed": "BOOL"},
            callback_fn=lambda x: 1,
            delimiter="|"
        )
        data = data_q.get()
        self.assertEqual(1, data)

    def test_read_edge(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = "1|2|0.1|2021|1|0\n2|1|1.5|2020|0|1\n"
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "edge",
            "dataframe",
            [],
            [],
            [],
            {},
            ["x", "time"],
            ["y"],
            ["is_train"],
            {"x": "FLOAT", "time": "INT", "y": "INT", "is_train": "BOOL"},
            delimiter="|"
        )
        data = data_q.get()
        truth = pd.read_csv(
            io.StringIO(raw),
            header=None,
            names=["source", "target", "x", "time", "y", "is_train"],
            sep=self.loader.delimiter,
        )
        assert_frame_equal(data, truth)
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_edge_callback(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = "1|2|0.1|2021|1|0\n2|1|1.5|2020|0|1\n"
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "edge",
            "dataframe",
            [],
            [],
            [],
            {},
            ["x", "time"],
            ["y"],
            ["is_train"],
            {"x": "FLOAT", "time": "INT", "y": "INT", "is_train": "BOOL"},
            callback_fn=lambda x: 1,
            delimiter="|"
        )
        data = data_q.get()
        self.assertEqual(data, 1)

    def test_read_graph_out_df(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "99|1 0 0 1 |1|0|1\n8|1 0 0 1 |1|1|1\n",
            "1|2|0.1|2021|1|0\n2|1|1.5|2020|0|1\n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "dataframe",
            ["x"],
            ["y"],
            ["train_mask", "is_seed"],
            {"x": "INT", "y": "INT", "train_mask": "BOOL", "is_seed": "BOOL"},
            ["x", "time"],
            ["y"],
            ["is_train"],
            {"x": "FLOAT", "time": "INT", "y": "INT", "is_train": "BOOL"},
            delimiter="|"
        )
        data = data_q.get()
        vertices = pd.read_csv(
            io.StringIO(raw[0]),
            header=None,
            names=["vid", "x", "y", "train_mask", "is_seed"],
            sep=self.loader.delimiter
        )
        edges = pd.read_csv(
            io.StringIO(raw[1]),
            header=None,
            names=["source", "target", "x", "time", "y", "is_train"],
            sep=self.loader.delimiter
        )
        assert_frame_equal(data[0], vertices)
        assert_frame_equal(data[1], edges)
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_graph_out_df_callback(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "99|1 0 0 1 |1|0|1\n8|1 0 0 1 |1|1|1\n",
            "1|2|0.1|2021|1|0\n2|1|1.5|2020|0|1\n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "dataframe",
            ["x"],
            ["y"],
            ["train_mask", "is_seed"],
            {"x": "INT", "y": "INT", "train_mask": "BOOL", "is_seed": "BOOL"},
            ["x", "time"],
            ["y"],
            ["is_train"],
            {"x": "FLOAT", "time": "INT", "y": "INT", "is_train": "BOOL"},
            callback_fn=lambda x: (1, 2),
            delimiter="|"
        )
        data = data_q.get()
        self.assertEqual(data[0], 1)
        self.assertEqual(data[1], 2)

    def test_read_graph_out_pyg(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "99|1 0 0 1 |1|0|Alex|1\n8|1 0 0 1 |1|1|Bill|0\n",
            "99|8|0.1|2021|1|0|a b \n8|99|1.5|2020|0|1|c d \n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "pyg",
            ["x"],
            ["y"],
            ["train_mask", "name", "is_seed"],
            {
                "x": "LIST:INT",
                "y": "INT",
                "train_mask": "BOOL",
                "name": "STRING",
                "is_seed": "BOOL",
            },
            ["x", "time"],
            ["y"],
            ["is_train", "category"],
            {"x": "DOUBLE", "time": "INT", "y": "INT",
                "is_train": "BOOL", "category": "LIST:STRING"},
            delimiter="|"
        )
        data = data_q.get()
        self.assertIsInstance(data, pygData)
        assert_close_torch(data["edge_index"], torch.tensor([[0, 1], [1, 0]]))
        assert_close_torch(
            data["edge_feat"],
            torch.tensor([[0.1, 2021], [1.5, 2020]], dtype=torch.double),
        )
        assert_close_torch(data["edge_label"], torch.tensor([1, 0]))
        assert_close_torch(data["is_train"], torch.tensor([False, True]))
        assert_close_torch(data["x"], torch.tensor(
            [[1, 0, 0, 1], [1, 0, 0, 1]]))
        assert_close_torch(data["y"], torch.tensor([1, 1]))
        assert_close_torch(data["train_mask"], torch.tensor([False, True]))
        assert_close_torch(data["is_seed"], torch.tensor([True, False]))
        self.assertListEqual(data["name"], ["Alex", "Bill"])
        self.assertListEqual(data["category"], [['a', 'b'], ['c', 'd']])
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_graph_out_dgl(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "99|1 0 0 1 |1|0|Alex|1\n8|1 0 0 1 |1|1|Bill|0\n",
            "99|8|0.1|2021|1|0|a b \n8|99|1.5|2020|0|1|c d \n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "dgl",
            ["x"],
            ["y"],
            ["train_mask", "name", "is_seed"],
            {
                "x": "LIST:INT",
                "y": "INT",
                "train_mask": "BOOL",
                "name": "STRING",
                "is_seed": "BOOL",
            },
            ["x", "time"],
            ["y"],
            ["is_train", "category"],
            {"x": "DOUBLE", "time": "INT", "y": "INT",
                "is_train": "BOOL", "category": "LIST:STRING"},
            delimiter="|"
        )
        data = data_q.get()
        self.assertIsInstance(data, DGLGraph)
        assert_close_torch(
            data.edges(), (torch.tensor([0, 1]), torch.tensor([1, 0])))
        assert_close_torch(
            data.edata["edge_feat"],
            torch.tensor([[0.1, 2021], [1.5, 2020]], dtype=torch.double),
        )
        assert_close_torch(data.edata["edge_label"], torch.tensor([1, 0]))
        assert_close_torch(data.edata["is_train"], torch.tensor([False, True]))
        assert_close_torch(data.ndata["x"], torch.tensor(
            [[1, 0, 0, 1], [1, 0, 0, 1]]))
        assert_close_torch(data.ndata["y"], torch.tensor([1, 1]))
        assert_close_torch(data.ndata["train_mask"],
                           torch.tensor([False, True]))
        assert_close_torch(data.ndata["is_seed"], torch.tensor([True, False]))
        self.assertListEqual(data.extra_data["name"], ["Alex", "Bill"])
        self.assertListEqual(data.extra_data["category"], [
                             ['a', 'b'], ['c', 'd']])
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_graph_parse_error(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "99|1 0 0 1 |1|0|Alex\n8|1 0 0 1 |1|1|Bill|0\n",
            "99|8|0.1|2021|1|0|a b \n8|99|1.5|2020|0|1|c d \n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "dgl",
            ["x"],
            ["y"],
            ["train_mask", "name", "is_seed"],
            {
                "x": "LIST:INT",
                "y": "INT",
                "train_mask": "BOOL",
                "name": "STRING",
                "is_seed": "BOOL",
            },
            ["x", "time"],
            ["y"],
            ["is_train", "category"],
            {"x": "DOUBLE", "time": "INT", "y": "INT",
                "is_train": "BOOL", "category": "LIST:STRING"},
            delimiter="|"
        )
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_graph_no_attr(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = ("99|1\n8|0\n", "99|8\n8|99\n")
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "pyg",
            [],
            [],
            ["is_seed"],
            {
                "x": "INT",
                "y": "INT",
                "train_mask": "BOOL",
                "name": "STRING",
                "is_seed": "BOOL",
            },
            [],
            [],
            [],
            {},
            delimiter="|"
        )
        data = data_q.get()
        self.assertIsInstance(data, pygData)
        assert_close_torch(data["edge_index"], torch.tensor([[0, 1], [1, 0]]))
        assert_close_torch(data["is_seed"], torch.tensor([True, False]))
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_graph_no_edge(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "99|1 0 0 1 |1|0|Alex|1\n8|1 0 0 1 |1|1|Bill|0\n",
            "",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "pyg",
            ["x"],
            ["y"],
            ["train_mask", "name", "is_seed"],
            {
                "x": "LIST:INT",
                "y": "INT",
                "train_mask": "BOOL",
                "name": "STRING",
                "is_seed": "BOOL",
            },
            ["x", "time"],
            ["y"],
            ["is_train"],
            {"x": "DOUBLE", "time": "INT", "y": "INT", "is_train": "BOOL"},
            delimiter="|"
        )
        data = data_q.get()
        self.assertIsInstance(data, pygData)
        self.assertListEqual(list(data["edge_index"].shape), [2, 0])
        self.assertListEqual(list(data["edge_feat"].shape), [0, 2])
        self.assertListEqual(list(data["edge_label"].shape), [0,])
        self.assertListEqual(list(data["is_train"].shape), [0,])
        assert_close_torch(data["x"], torch.tensor(
            [[1, 0, 0, 1], [1, 0, 0, 1]]))
        assert_close_torch(data["y"], torch.tensor([1, 1]))
        assert_close_torch(data["train_mask"], torch.tensor([False, True]))
        assert_close_torch(data["is_seed"], torch.tensor([True, False]))
        self.assertListEqual(data["name"], ["Alex", "Bill"])
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_hetero_graph_out_pyg(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "People|99|1 0 0 1 |1|0|Alex|1\nPeople|8|1 0 0 1 |1|1|Bill|0\nCompany|2|0.3|0\n",
            "Colleague|99|8|0.1|2021|1|0\nColleague|8|99|1.5|2020|0|1\nWork|99|2\nWork|2|8\n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "pyg",
            {"People": ["x"], "Company": ["x"]},
            {"People": ["y"]},
            {"People": ["train_mask", "name", "is_seed"],
                "Company": ["is_seed"]},
            {
                "People": {
                    "x": "LIST:INT",
                    "y": "INT",
                    "train_mask": "BOOL",
                    "name": "STRING",
                    "is_seed": "BOOL",
                },
                "Company": {"x": "FLOAT", "is_seed": "BOOL"},
            },
            {"Colleague": ["x", "time"]},
            {"Colleague": ["y"]},
            {"Colleague": ["is_train"]},
            {
                "Colleague": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "People",
                    "IsDirected": False,
                    "x": "DOUBLE",
                    "time": "INT",
                    "y": "INT",
                    "is_train": "BOOL",
                },
                "Work": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "Company",
                    "IsDirected": False,
                }
            },
            False,
            "|",
            True,
            True,
        )
        data = data_q.get()
        self.assertIsInstance(data, pygHeteroData)
        assert_close_torch(
            data["Colleague"]["edge_index"], torch.tensor([[0, 1], [1, 0]])
        )
        assert_close_torch(
            data["Colleague"]["edge_feat"],
            torch.tensor([[0.1, 2021], [1.5, 2020]], dtype=torch.double),
        )
        assert_close_torch(
            data["Colleague"]["edge_label"], torch.tensor([1, 0]))
        assert_close_torch(data["Colleague"]["is_train"],
                           torch.tensor([False, True]))
        assert_close_torch(
            data["People"]["x"], torch.tensor([[1, 0, 0, 1], [1, 0, 0, 1]])
        )
        assert_close_torch(data["People"]["y"], torch.tensor([1, 1]))
        assert_close_torch(data["People"]["train_mask"],
                           torch.tensor([False, True]))
        assert_close_torch(data["People"]["is_seed"],
                           torch.tensor([True, False]))
        self.assertListEqual(data["People"]["name"], ["Alex", "Bill"])
        assert_close_torch(
            data["Company"]["x"], torch.tensor([0.3], dtype=torch.double)
        )
        assert_close_torch(data["Company"]["is_seed"], torch.tensor([False]))
        assert_close_torch(
            data["Work"]["edge_index"], torch.tensor([[0, 1], [0, 0]])
        )
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_hetero_graph_no_attr(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "People|99|1\nPeople|8|0\nCompany|2|0\n",
            "Colleague|99|8\nColleague|8|99\nWork|99|2\nWork|2|8\n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "pyg",
            {"People": [], "Company": []},
            {"People": [], "Company": []},
            {"People": ["is_seed"], "Company": ["is_seed"]},
            {
                "People": {
                    "x": "LIST:INT",
                    "y": "INT",
                    "train_mask": "BOOL",
                    "name": "STRING",
                    "is_seed": "BOOL",
                },
                "Company": {"x": "FLOAT", "is_seed": "BOOL"},
            },
            {"Colleague": [], "Work": []},
            {"Colleague": [], "Work": []},
            {"Colleague": [], "Work": []},
            {
                "Colleague": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "People",
                    "IsDirected": False,
                    "x": "DOUBLE",
                    "time": "INT",
                    "y": "INT",
                    "is_train": "BOOL",
                },
                "Work": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "Company",
                    "IsDirected": False,
                }
            },
            False,
            "|",
            True,
            True,
        )
        data = data_q.get()
        self.assertIsInstance(data, pygHeteroData)
        assert_close_torch(
            data["Colleague"]["edge_index"], torch.tensor([[0, 1], [1, 0]])
        )
        assert_close_torch(
            data["Work"]["edge_index"], torch.tensor([[0, 1], [0, 0]])
        )
        assert_close_torch(data["People"]["is_seed"],
                           torch.tensor([True, False]))
        assert_close_torch(data["Company"]["is_seed"], torch.tensor([False]))
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_hetero_graph_no_edge(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "People|99|1 0 0 1 |1|0|Alex|1\nPeople|8|1 0 0 1 |1|1|Bill|0\nCompany|2|0.3|0\n",
            "",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "pyg",
            {"People": ["x"], "Company": ["x"]},
            {"People": ["y"]},
            {"People": ["train_mask", "name", "is_seed"],
                "Company": ["is_seed"]},
            {
                "People": {
                    "x": "LIST:INT",
                    "y": "INT",
                    "train_mask": "BOOL",
                    "name": "STRING",
                    "is_seed": "BOOL",
                },
                "Company": {"x": "FLOAT", "is_seed": "BOOL"},
            },
            {"Colleague": ["x", "time"]},
            {"Colleague": ["y"]},
            {"Colleague": ["is_train"]},
            {
                "Colleague": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "People",
                    "IsDirected": False,
                    "x": "DOUBLE",
                    "time": "INT",
                    "y": "INT",
                    "is_train": "BOOL",
                },
                "Work": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "Company",
                    "IsDirected": False,
                }
            },
            False,
            "|",
            True,
            True,
        )
        data = data_q.get()
        self.assertIsInstance(data, pygHeteroData)
        self.assertNotIn("Colleague", data)
        assert_close_torch(
            data["People"]["x"], torch.tensor([[1, 0, 0, 1], [1, 0, 0, 1]])
        )
        assert_close_torch(data["People"]["y"], torch.tensor([1, 1]))
        assert_close_torch(data["People"]["train_mask"],
                           torch.tensor([False, True]))
        assert_close_torch(data["People"]["is_seed"],
                           torch.tensor([True, False]))
        self.assertListEqual(data["People"]["name"], ["Alex", "Bill"])
        assert_close_torch(
            data["Company"]["x"], torch.tensor([0.3], dtype=torch.double)
        )
        assert_close_torch(data["Company"]["is_seed"], torch.tensor([False]))
        self.assertNotIn("Work", data)
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_hetero_graph_out_dgl(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "People|99|1 0 0 1 |1|0|Alex|1\nPeople|8|1 0 0 1 |1|1|Bill|0\nCompany|2|0.3|0\n",
            "Colleague|99|8|0.1|2021|1|0\nColleague|8|99|1.5|2020|0|1\nWork|99|2|a b \nWork|2|8|c d \n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "dgl",
            {"People": ["x"], "Company": ["x"]},
            {"People": ["y"]},
            {"People": ["train_mask", "name", "is_seed"],
                "Company": ["is_seed"]},
            {
                "People": {
                    "x": "LIST:INT",
                    "y": "INT",
                    "train_mask": "BOOL",
                    "name": "STRING",
                    "is_seed": "BOOL",
                },
                "Company": {"x": "FLOAT", "is_seed": "BOOL"},
            },
            {"Colleague": ["x", "time"]},
            {"Colleague": ["y"]},
            {"Colleague": ["is_train"], "Work": ["category"]},
            {
                "Colleague": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "People",
                    "IsDirected": False,
                    "x": "DOUBLE",
                    "time": "INT",
                    "y": "INT",
                    "is_train": "BOOL",
                },
                "Work": {
                    "FromVertexTypeName": "People",
                    "ToVertexTypeName": "Company",
                    "IsDirected": False,
                    "category": "LIST:STRING"
                }
            },
            False,
            "|",
            True,
            True,
        )
        data = data_q.get()
        self.assertIsInstance(data, DGLGraph)
        assert_close_torch(
            data.edges(etype="Colleague"), (torch.tensor(
                [0, 1]), torch.tensor([1, 0]))
        )
        assert_close_torch(
            data.edges["Colleague"].data["edge_feat"],
            torch.tensor([[0.1, 2021], [1.5, 2020]], dtype=torch.double),
        )
        assert_close_torch(
            data.edges["Colleague"].data["edge_label"], torch.tensor([1, 0]))
        assert_close_torch(
            data.edges["Colleague"].data["is_train"], torch.tensor([False, True]))
        assert_close_torch(
            data.nodes["People"].data["x"], torch.tensor(
                [[1, 0, 0, 1], [1, 0, 0, 1]])
        )
        assert_close_torch(
            data.nodes["People"].data["y"], torch.tensor([1, 1]))
        assert_close_torch(
            data.nodes["People"].data["train_mask"], torch.tensor([False, True]))
        assert_close_torch(
            data.nodes["People"].data["is_seed"], torch.tensor([True, False]))
        self.assertListEqual(
            data.extra_data["People"]["name"], ["Alex", "Bill"])
        assert_close_torch(
            data.nodes["Company"].data["x"], torch.tensor(
                [0.3], dtype=torch.double)
        )
        assert_close_torch(
            data.nodes["Company"].data["is_seed"], torch.tensor([False]))
        assert_close_torch(
            data.edges(etype="Work"), (torch.tensor(
                [0, 1]), torch.tensor([0, 0]))
        )
        self.assertListEqual(data.extra_data["Work"]["category"], [
                             ['a', 'b'], ['c', 'd']])
        data = data_q.get()
        self.assertIsNone(data)

    def test_read_bool_label(self):
        read_task_q = Queue()
        data_q = Queue(4)
        exit_event = Event()
        raw = (
            "99|1 0 0 1 |1|0|Alex|1\n8|1 0 0 1 |1|1|Bill|0\n",
            "99|8|0.1|2021|1|0\n8|99|1.5|2020|0|1\n",
        )
        read_task_q.put(raw)
        read_task_q.put(None)
        self.loader._read_data(
            exit_event,
            read_task_q,
            data_q,
            "graph",
            "pyg",
            ["x"],
            ["y"],
            ["train_mask", "name", "is_seed"],
            {
                "x": "LIST:INT",
                "y": "BOOL",
                "train_mask": "BOOL",
                "name": "STRING",
                "is_seed": "BOOL",
            },
            ["x", "time"],
            ["y"],
            ["is_train"],
            {"x": "DOUBLE", "time": "INT", "y": "BOOL", "is_train": "BOOL"},
            delimiter="|"
        )
        data = data_q.get()
        self.assertIsInstance(data, pygData)
        assert_close_torch(data["edge_index"], torch.tensor([[0, 1], [1, 0]]))
        assert_close_torch(
            data["edge_feat"],
            torch.tensor([[0.1, 2021], [1.5, 2020]], dtype=torch.double),
        )
        assert_close_torch(data["edge_label"], torch.tensor([True, False]))
        assert_close_torch(data["is_train"], torch.tensor([False, True]))
        assert_close_torch(data["x"], torch.tensor(
            [[1, 0, 0, 1], [1, 0, 0, 1]]))
        assert_close_torch(data["y"], torch.tensor([True, True]))
        assert_close_torch(data["train_mask"], torch.tensor([False, True]))
        assert_close_torch(data["is_seed"], torch.tensor([True, False]))
        self.assertListEqual(data["name"], ["Alex", "Bill"])
        data = data_q.get()
        self.assertIsNone(data)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSBaseLoader("test_get_schema"))
    suite.addTest(TestGDSBaseLoader("test_get_schema_no_primary_id_attr"))
    suite.addTest(TestGDSBaseLoader("test_validate_vertex_attributes"))
    suite.addTest(TestGDSBaseLoader("test_validate_edge_attributes"))
    suite.addTest(TestGDSBaseLoader("test_read_vertex"))
    suite.addTest(TestGDSBaseLoader("test_read_vertex_callback"))
    suite.addTest(TestGDSBaseLoader("test_read_edge"))
    suite.addTest(TestGDSBaseLoader("test_read_edge_callback"))
    suite.addTest(TestGDSBaseLoader("test_read_graph_out_df"))
    suite.addTest(TestGDSBaseLoader("test_read_graph_out_df_callback"))
    suite.addTest(TestGDSBaseLoader("test_read_graph_out_pyg"))
    suite.addTest(TestGDSBaseLoader("test_read_graph_out_dgl"))
    suite.addTest(TestGDSBaseLoader("test_read_graph_parse_error"))
    suite.addTest(TestGDSBaseLoader("test_read_graph_no_attr"))
    suite.addTest(TestGDSBaseLoader("test_read_graph_no_edge"))
    suite.addTest(TestGDSBaseLoader("test_read_hetero_graph_out_pyg"))
    suite.addTest(TestGDSBaseLoader("test_read_hetero_graph_no_attr"))
    suite.addTest(TestGDSBaseLoader("test_read_hetero_graph_no_edge"))
    suite.addTest(TestGDSBaseLoader("test_read_hetero_graph_out_dgl"))
    suite.addTest(TestGDSBaseLoader("test_read_bool_label"))
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
