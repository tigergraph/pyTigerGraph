import json
import unittest

from pyTigerGraph.common.schema import _upsert_attrs

from pyTigerGraphUnitTestAsync import make_connection

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphSchemaAsync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.conn = await make_connection()

    async def test_01_getUDTs(self):
        res = await self.conn._getUDTs()
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))
        self.assertTrue(
            res[0]["name"] == "tuple1_all_types" or res[0]["name"] == "tuple2_simple")
        tuple2_simple = res[0] if (
            res[0]["name"] == "tuple2_simple") else res[1]
        self.assertIn('fields', tuple2_simple)
        fields = tuple2_simple['fields']
        self.assertTrue(fields[0]['fieldName'] == 'field1')
        self.assertTrue(fields[0]['fieldType'] == 'INT')
        self.assertTrue(fields[1]['fieldName'] == 'field2')
        self.assertTrue(fields[1]['fieldType'] == 'STRING')
        self.assertTrue(fields[2]['fieldName'] == 'field3')
        self.assertTrue(fields[2]['fieldType'] == 'DATETIME')

    def test_02_upsertAttrs(self):
        tests = [
            ({"attr_name": "attr_value"}, {"attr_name": {"value": "attr_value"}}),
            ({"attr_name1": "attr_value1", "attr_name2": "attr_value2"},
                {"attr_name1": {"value": "attr_value1"}, "attr_name2": {"value": "attr_value2"}}),
            ({"attr_name": ("attr_value", "operator")},
                {"attr_name": {"value": "attr_value", "op": "operator"}}),
            ({"attr_name1": ("attr_value1", "+"), "attr_name2": ("attr_value2", "-")},
                {"attr_name1": {"value": "attr_value1", "op": "+"},
                 "attr_name2": {"value": "attr_value2", "op": "-"}}),
            ("a string", {}),
            ({"attr_name"}, {}),
            (1, {}),
            ({}, {})
        ]

        for t in tests:
            res = _upsert_attrs(t[0])
            self.assertEqual(t[1], res)

    async def test_03_getSchema(self):
        res = await self.conn.getSchema()
        items = [
            ("GraphName"),
            ("VertexTypes", "Name", [
                "vertex1_all_types",
                "vertex2_primary_key",
                "vertex3_primary_key_composite",
                "vertex4",
                "vertex5",
                "vertex6",
                "vertex7"
            ]),
            ("EdgeTypes", "Name", [
                "edge1_undirected",
                "edge2_directed",
                "edge3_directed_with_reverse",
                "edge4_many_to_many",
                "edge5_all_to_all",
                "edge6_loop"
            ]),
            ("UDTs", "name", [
                "tuple1_all_types",
                "tuple2_simple"
            ])
        ]
        self.assertEqual(len(items), len(res))
        for i in items:
            if i == "GraphName":
                self.assertEqual(self.conn.graphname, res[i])
            else:
                self.assertIn(i[0], res)
                t = res[i[0]]
                self.assertIsInstance(t, list)
                self.assertEqual(len(i[2]), len(t))
                for tt in t:
                    self.assertIn(i[1], tt)
                    self.assertIn(tt[i[1]], i[2])

    async def test_04_upsertData(self):
        data = {
            "vertices": {
                "vertex4": {
                    "4000": {
                        "a01": {
                            "value": 4000
                        }
                    },
                    "4001": {
                        "a01": {
                            "value": 4001
                        }
                    }
                },
                "vertex5": {
                    "5000": {},
                    "5001": {}
                }
            },
            "edges": {
                "vertex4": {
                    "4000": {
                        "edge2_directed": {
                            "vertex5": {
                                "5000": {
                                    "a01": {
                                        "value": 40005000
                                    }
                                },
                                "5001": {
                                    "a01": {
                                        "value": 40005001
                                    }
                                }
                            }
                        }
                    },
                    "4001": {
                        "edge3_directed_with_reverse": {
                            "vertex5": {
                                "5000": {
                                    "a01": {
                                        "value": 40005000
                                    }
                                },
                            }
                        }
                    }
                }
            }
        }
        res = await self.conn.upsertData(data)
        self.assertEqual({"accepted_vertices": 4, "accepted_edges": 3}, res)

        res = await self.conn.delVertices("vertex4", where="a01>1000")
        self.assertEqual(2, res)

        res = await self.conn.delVerticesById("vertex5", [5000, 5001])
        self.assertEqual(2, res)

        """
               v4     v5       
        7000   🔵️———🔵️
                 ╲ ╱
                  ╳
                 ╱ ╲
        7001   🔵️   🔵️
        """
        data = {
            "vertices": {
                "vertex4": {
                    "7000": {
                        "a01": {
                            "value": 7000
                        }
                    },
                    "7001": {
                        "a01": {
                            "value": 7000
                        }
                    }
                },
                "vertex5": {
                    "7000": {},
                    "7001": {}
                }
            },
            "edges": {
                "vertex4": {
                    "7000": {
                        "edge2_directed": {
                            "vertex5": {
                                "7000": {
                                    "a01": {
                                        "value": 7000
                                    }
                                },
                                "7001": {
                                    "a01": {
                                        "value": 7000
                                    }
                                }
                            }
                        }
                    },
                    "7001": {
                        "edge2_directed": {
                            "vertex5": {
                                "7000": {
                                    "a01": {
                                        "value": 7000
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        res = await self.conn.upsertData(data, atomic=True, ackAll=True)
        self.assertEqual({"accepted_vertices": 4, "accepted_edges": 3}, res)

        """
               v4     v5       
        7000   🔴️———🔵️
                 ╲ ╱
                  ╳
                 ╱ ╲
        7001   🔴️   🔵

        7002   🟢
        """
        data = {
            "vertices": {
                "vertex4": {
                    "7000": {
                        "a01": {
                            "value": 7010
                        }
                    },
                    "7001": {
                        "a01": {
                            "value": 7010
                        }
                    },
                    "7002": {
                        "a01": {
                            "value": 7010
                        }
                    }
                }
            }
        }
        exp = {
            "accepted_vertices": 1,
            "skipped_vertices": 2,
            "vertices_already_exist": [
                {"v_type": "vertex4", "v_id": "7000"},
                {"v_type": "vertex4", "v_id": "7001"}
            ],
            "accepted_edges": 0
        }
        res = await self.conn.upsertData(data, newVertexOnly=True)
        self.assertEqual(exp, res)

        """
               v4     v5       
        7000   🔵️———🔵
                 ╲ ╱
                  ╳
                 ╱ ╲
        7001   🔵️   🔵️
                   ╱
                  ╱
                 ╱
        7002   🔵⋯⋯⋯🔴

        7003   🔴⋯⋯⋯🔴
        """
        data = {
            "edges": {
                "vertex4": {
                    "7002": {
                        "edge2_directed": {
                            "vertex5": {
                                "7001": {
                                    "a01": {
                                        "value": 7000
                                    }
                                },
                                "7002": {
                                    "a01": {
                                        "value": 7000
                                    }
                                }
                            }
                        }
                    },
                    "7003": {
                        "edge2_directed": {
                            "vertex5": {
                                "7003": {
                                    "a01": {
                                        "value": 7000
                                    }
                                },
                            }
                        }
                    }
                }
            }
        }
        exp = {
            "accepted_vertices": 0,
            "accepted_edges": 1,
            "skipped_edges": 2,
            "edge_vertices_not_exist": [
                {"v_type": "vertex5", "v_id": "7002"},
                {"v_type": "vertex4", "v_id": "7003"},
                {"v_type": "vertex5", "v_id": "7003"}
            ]
        }
        res = await self.conn.upsertData(data, vertexMustExist=True)
        self.assertEqual(exp, res)

        """
               v4     v5       
        7000   🟢️———🔵
                 ╲ ╱
                  ╳
                 ╱ ╲
        7001   🟢️   🔵
                   ╱
                  ╱
                 ╱
        7002   🔵

        7003   🔴
        """
        data = {
            "vertices": {
                "vertex4": {
                    "7000": {
                        "a01": {
                            "value": 7020
                        }
                    },
                    "7001": {
                        "a01": {
                            "value": 7020
                        }
                    },
                    "7003": {
                        "a01": {
                            "value": 7020
                        }
                    }
                }
            }
        }
        exp = {
            "accepted_vertices": 2,
            "skipped_vertices": 1,
            "vertices_not_exist": [
                {"v_type": "vertex4", "v_id": "7003"}
            ],
            "accepted_edges": 0
        }
        res = await self.conn.upsertData(data, updateVertexOnly=True)
        self.assertEqual(exp, res)

        res = await self.conn.delVertices("vertex4", where="a01>=7000,a01<8000")
        self.assertEqual(3, res)

        res = await self.conn.delVerticesById("vertex5", [7000, 7001])
        self.assertEqual(2, res)

    async def test_05_getEndpoints(self):
        res = await self.conn.getEndpoints()
        self.assertIsInstance(res, dict)
        self.assertIn("GET /endpoints/{graph_name}", res)

        res = await self.conn.getEndpoints(dynamic=True)
        self.assertEqual(4, len(res))

    async def test_createGlobalVertices(self):
        """Test createGlobalVertices function with GSQL commands."""
        # Test single GSQL command
        gsql_command = "CREATE VERTEX TestVertex1 (PRIMARY_ID id UINT, name STRING)"
        res = await self.conn.createGlobalVertices(gsql_command)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test multiple GSQL commands
        gsql_commands = [
            "CREATE VERTEX TestVertex2 (PRIMARY_ID id UINT, name STRING)",
            "CREATE VERTEX TestVertex3 (PRIMARY_ID id UINT, age UINT)"
        ]
        res = await self.conn.createGlobalVertices(gsql_commands)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test invalid input
        with self.assertRaises(Exception):
            await self.conn.createGlobalVertices("INVALID COMMAND")

    async def test_createGlobalVerticesJson(self):
        """Test createGlobalVerticesJson function with JSON configuration."""
        # Test single vertex config
        vertex_config = {
            "Config": {
                "STATS": "OUTDEGREE_BY_EDGETYPE"
            },
            "Attributes": [
                {
                    "AttributeType": {
                        "Name": "STRING"
                    },
                    "AttributeName": "name"
                }
            ],
            "PrimaryId": {
                "AttributeType": {
                    "Name": "UINT"
                },
                "AttributeName": "user_id"
            },
            "Name": "TestJsonVertex1"
        }
        res = await self.conn.createGlobalVerticesJson(vertex_config)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test multiple vertex configs
        vertices_config = [
            {
                "Config": {
                    "STATS": "OUTDEGREE_BY_EDGETYPE"
                },
                "Attributes": [
                    {
                        "AttributeType": {
                            "Name": "STRING"
                        },
                        "AttributeName": "name"
                    }
                ],
                "PrimaryId": {
                    "AttributeType": {
                        "Name": "UINT"
                    },
                    "AttributeName": "user_id"
                },
                "Name": "TestJsonVertex2"
            },
            {
                "Config": {
                    "STATS": "OUTDEGREE_BY_EDGETYPE"
                },
                "Attributes": [
                    {
                        "AttributeType": {
                            "Name": "STRING"
                        },
                        "AttributeName": "name"
                    }
                ],
                "PrimaryId": {
                    "AttributeType": {
                        "Name": "UINT"
                    },
                    "AttributeName": "user_id"
                },
                "Name": "TestJsonVertex3"
            }
        ]
        res = await self.conn.createGlobalVerticesJson(vertices_config)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test invalid input - missing required fields
        invalid_config = {
            "Name": "InvalidVertex"
            # Missing PrimaryId and Attributes
        }
        with self.assertRaises(Exception):
            await self.conn.createGlobalVerticesJson(invalid_config)

    async def test_addGlobalVerticesToGraph(self):
        """Test addGlobalVerticesToGraph function."""
        # Test single vertex name
        res = await self.conn.addGlobalVerticesToGraph("TestVertex1")
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test multiple vertex names
        res = await self.conn.addGlobalVerticesToGraph(["TestVertex2", "TestVertex3"])
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test with specific target graph
        res = await self.conn.addGlobalVerticesToGraph(["TestVertex1"], target_graph=self.conn.graphname)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test invalid input
        with self.assertRaises(Exception):
            await self.conn.addGlobalVerticesToGraph(123)  # Should be string or list

    async def test_rebuildGraphEngine(self):
        """Test rebuildGraphEngine function."""
        # Test basic rebuild
        res = await self.conn.rebuildGraphEngine()
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test with parameters (use existing vertex type from testserver.gsql)
        res = await self.conn.rebuildGraphEngine(
            threadnum=2,
            vertextype="vertex4",
            path="/tmp/test_rebuild",
            force=True
        )
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)

        # Test with segid parameter
        res = await self.conn.rebuildGraphEngine(segid=1)
        self.assertIsInstance(res, dict)
        self.assertIn("error", res)
        self.assertIn("message", res)


if __name__ == '__main__':
    unittest.main()
