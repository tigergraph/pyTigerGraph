import unittest
from pyTigerGraph.gds.transforms.pyg_transforms import TemporalPyGTransform
import torch_geometric as pyg
import torch


class TestPyGTemporalTransform(unittest.TestCase):
    def test_init(self):
        vertex_start_attrs = {"Customer": "start_dt",
                              "Item": "start_ts", "ItemInstance": "start_dt"}
        vertex_end_attrs = {"Item": "end_ts", "ItemInstance": "end_dt"}
        edge_start_attrs = {("Customer", "PURCHASED", "Item"): "purchase_time"}
        edge_end_attrs = {("Customer", "PURCHASED", "Item"): "purchase_time"}
        feature_transforms = {
            ("ItemInstance", "reverse_DESCRIBED_BY", "Item"): ["x"]}
        transform = TemporalPyGTransform(vertex_start_attrs=vertex_start_attrs,
                                         vertex_end_attrs=vertex_end_attrs,
                                         edge_start_attrs=edge_start_attrs,
                                         edge_end_attrs=edge_end_attrs,
                                         start_dt=0,
                                         end_dt=6,
                                         feature_transforms=feature_transforms)
        self.assertEqual(transform.timestep, 86400)

    def test_homogeneous_transform(self):
        data = pyg.data.Data()
        data.x = torch.randn(4, 3)
        data.edge_index = torch.tensor([[0, 3, 2],
                                        [1, 2, 0]])
        data.vertex_start = torch.tensor([0, 0, 2, 1])
        data.vertex_end = torch.tensor([5, 5, 5, 5])
        data.edge_start = torch.tensor([0, 2, 3])
        data.edge_end = torch.tensor([5, 5, 5])
        vertex_start_attrs = "vertex_start"
        vertex_end_attrs = "vertex_end"
        edge_start_attrs = "edge_start"
        edge_end_attrs = "edge_end"
        transform = TemporalPyGTransform(vertex_start_attrs=vertex_start_attrs,
                                         vertex_end_attrs=vertex_end_attrs,
                                         edge_start_attrs=edge_start_attrs,
                                         edge_end_attrs=edge_end_attrs,
                                         start_dt=0,
                                         end_dt=5,
                                         timestep=1)
        seq = transform(data)
        self.assertEqual(seq[0].edge_index.shape[1], 1)
        self.assertEqual(seq[-1].edge_index.shape[1], 3)
        self.assertEqual(len(seq), 5)

    def test_heterogeneous_transform(self):
        data = pyg.data.HeteroData()
        data["ItemInstance"].x = torch.tensor([5, 6, 3, 2.5])
        data["ItemInstance"].start_dt = torch.tensor([0, 4, 3, 1])
        data["ItemInstance"].end_dt = torch.tensor([6, 6, 4, 3])
        data["ItemInstance"].id = ["2_1", "1_3", "1_2", "1_1"]
        data["ItemInstance"].is_seed = torch.tensor([True, True, True, True])
        data["Item"].start_ts = torch.tensor([1, 0])
        data["Item"].end_ts = torch.tensor([-1, -1])
        data["Item"].is_seed = torch.tensor([True, True])
        data["Customer"].start_dt = torch.tensor([0, 1])
        data["Customer"].is_seed = torch.tensor([True, True])
        data["ZipCode"].id = torch.tensor([55369])
        data["ZipCode"].is_seed = torch.tensor([True])
        data[("ItemInstance", "reverse_DESCRIBED_BY", "Item")].edge_index = torch.tensor([[3, 2, 1, 0],
                                                                                          [0, 0, 0, 1]])
        data[("Item", "DESCRIBED_BY", "ItemInstance")].edge_index = torch.tensor([[0, 0, 0, 1],
                                                                                  [2, 1, 3, 0]])
        data[("Customer", "LIVES_IN", "ZipCode")].edge_index = torch.tensor([[0, 1],
                                                                             [0, 0]])
        data[("Customer", "PURCHASED", "Item")].edge_index = torch.tensor([[1, 1, 1],
                                                                           [1, 0, 0]])
        data[("Customer", "PURCHASED", "Item")
             ].purchase_time = torch.tensor([3, 1, 4])

        vertex_start_attrs = {"Customer": "start_dt",
                              "Item": "start_ts", "ItemInstance": "start_dt"}
        vertex_end_attrs = {"Item": "end_ts", "ItemInstance": "end_dt"}
        edge_start_attrs = {("Customer", "PURCHASED", "Item"): "purchase_time"}
        edge_end_attrs = {("Customer", "PURCHASED", "Item"): "purchase_time"}
        feature_transforms = {
            ("ItemInstance", "reverse_DESCRIBED_BY", "Item"): ["x"]}
        transform = TemporalPyGTransform(vertex_start_attrs=vertex_start_attrs,
                                         vertex_end_attrs=vertex_end_attrs,
                                         edge_start_attrs=edge_start_attrs,
                                         edge_end_attrs=edge_end_attrs,
                                         start_dt=0,
                                         end_dt=6,
                                         feature_transforms=feature_transforms,
                                         timestep=1)

        transformed_data = transform(data)
        # price of second item should be 5
        self.assertEqual(transformed_data[0]["Item"]["ItemInstance_x"][1], 5)
        self.assertEqual(transformed_data[4]["Item"]["ItemInstance_x"][0], 6)
        self.assertEqual(transformed_data[5][(
            "Customer", "PURCHASED", "ITEM")], {})
        self.assertEqual(len(transformed_data), 6)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
