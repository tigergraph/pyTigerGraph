# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Tool metadata for enhanced LLM guidance."""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from enum import Enum


class ToolCategory(str, Enum):
    """Categories for organizing tools."""
    SCHEMA = "schema"
    DATA = "data"
    QUERY = "query"
    VECTOR = "vector"
    LOADING = "loading"
    DISCOVERY = "discovery"
    UTILITY = "utility"


class ToolMetadata(BaseModel):
    """Enhanced metadata for tools to help LLMs understand usage patterns."""
    category: ToolCategory
    prerequisites: List[str] = []
    related_tools: List[str] = []
    common_next_steps: List[str] = []
    use_cases: List[str] = []
    complexity: str = "basic"  # basic, intermediate, advanced
    examples: List[Dict[str, Any]] = []
    keywords: List[str] = []  # For discovery


# Define metadata for each tool
TOOL_METADATA: Dict[str, ToolMetadata] = {
    # Schema Operations
    "tigergraph__describe_graph": ToolMetadata(
        category=ToolCategory.SCHEMA,
        prerequisites=[],
        related_tools=["tigergraph__get_graph_schema", "tigergraph__get_graph_metadata"],
        common_next_steps=["tigergraph__add_node", "tigergraph__add_edge", "tigergraph__run_query"],
        use_cases=[
            "Understanding the structure of a graph before writing queries",
            "Discovering available vertex and edge types",
            "Learning the attributes of each vertex/edge type",
            "First step in any graph interaction workflow"
        ],
        complexity="basic",
        keywords=["schema", "structure", "describe", "understand", "explore"],
        examples=[
            {
                "description": "Get schema for default graph",
                "parameters": {}
            },
            {
                "description": "Get schema for specific graph",
                "parameters": {"graph_name": "SocialGraph"}
            }
        ]
    ),
    
    "tigergraph__list_graphs": ToolMetadata(
        category=ToolCategory.SCHEMA,
        prerequisites=[],
        related_tools=["tigergraph__describe_graph", "tigergraph__create_graph"],
        common_next_steps=["tigergraph__describe_graph"],
        use_cases=[
            "Discovering what graphs exist in the database",
            "First step when connecting to a new TigerGraph instance",
            "Verifying a graph was created successfully"
        ],
        complexity="basic",
        keywords=["list", "graphs", "discover", "available"],
        examples=[{"description": "List all graphs", "parameters": {}}]
    ),
    
    "tigergraph__create_graph": ToolMetadata(
        category=ToolCategory.SCHEMA,
        prerequisites=[],
        related_tools=["tigergraph__list_graphs", "tigergraph__describe_graph"],
        common_next_steps=["tigergraph__describe_graph", "tigergraph__add_node"],
        use_cases=[
            "Creating a new graph from scratch",
            "Setting up a graph with specific vertex and edge types",
            "Initializing a new project or data model"
        ],
        complexity="intermediate",
        keywords=["create", "new", "graph", "initialize", "setup"],
        examples=[
            {
                "description": "Create a social network graph",
                "parameters": {
                    "graph_name": "SocialGraph",
                    "vertex_types": [
                        {
                            "name": "Person",
                            "attributes": [
                                {"name": "name", "type": "STRING"},
                                {"name": "age", "type": "INT"}
                            ]
                        }
                    ],
                    "edge_types": [
                        {
                            "name": "FOLLOWS",
                            "from_vertex": "Person",
                            "to_vertex": "Person"
                        }
                    ]
                }
            }
        ]
    ),
    
    "tigergraph__get_graph_schema": ToolMetadata(
        category=ToolCategory.SCHEMA,
        prerequisites=[],
        related_tools=["tigergraph__describe_graph"],
        common_next_steps=["tigergraph__add_node", "tigergraph__run_query"],
        use_cases=[
            "Getting raw JSON schema for programmatic processing",
            "Detailed schema inspection for advanced use cases"
        ],
        complexity="intermediate",
        keywords=["schema", "json", "raw", "detailed"],
        examples=[{"description": "Get raw schema", "parameters": {}}]
    ),
    
    # Node Operations
    "tigergraph__add_node": ToolMetadata(
        category=ToolCategory.DATA,
        prerequisites=["tigergraph__describe_graph"],
        related_tools=["tigergraph__add_nodes", "tigergraph__get_node", "tigergraph__delete_node"],
        common_next_steps=["tigergraph__get_node", "tigergraph__add_edge", "tigergraph__get_node_edges"],
        use_cases=[
            "Creating a single vertex in the graph",
            "Updating an existing vertex's attributes",
            "Adding individual entities (users, products, etc.)"
        ],
        complexity="basic",
        keywords=["add", "create", "insert", "node", "vertex", "single"],
        examples=[
            {
                "description": "Add a person node",
                "parameters": {
                    "vertex_type": "Person",
                    "vertex_id": "user123",
                    "attributes": {"name": "Alice", "age": 30, "city": "San Francisco"}
                }
            },
            {
                "description": "Add a product node",
                "parameters": {
                    "vertex_type": "Product",
                    "vertex_id": "prod456",
                    "attributes": {"name": "Laptop", "price": 999.99, "category": "Electronics"}
                }
            }
        ]
    ),
    
    "tigergraph__add_nodes": ToolMetadata(
        category=ToolCategory.DATA,
        prerequisites=["tigergraph__describe_graph"],
        related_tools=["tigergraph__add_node", "tigergraph__get_nodes"],
        common_next_steps=["tigergraph__get_vertex_count", "tigergraph__add_edges"],
        use_cases=[
            "Batch loading multiple vertices efficiently",
            "Importing data from CSV or JSON",
            "Initial data population"
        ],
        complexity="basic",
        keywords=["add", "create", "insert", "batch", "multiple", "bulk", "nodes", "vertices"],
        examples=[
            {
                "description": "Add multiple person nodes",
                "parameters": {
                    "vertex_type": "Person",
                    "vertices": [
                        {"id": "user1", "name": "Alice", "age": 30},
                        {"id": "user2", "name": "Bob", "age": 25},
                        {"id": "user3", "name": "Carol", "age": 35}
                    ]
                }
            }
        ]
    ),
    
    "tigergraph__get_node": ToolMetadata(
        category=ToolCategory.DATA,
        prerequisites=[],
        related_tools=["tigergraph__get_nodes", "tigergraph__has_node"],
        common_next_steps=["tigergraph__get_node_edges", "tigergraph__delete_node"],
        use_cases=[
            "Retrieving a specific vertex by ID",
            "Verifying a vertex was created",
            "Checking vertex attributes"
        ],
        complexity="basic",
        keywords=["get", "retrieve", "fetch", "read", "node", "vertex", "single"],
        examples=[
            {
                "description": "Get a person node",
                "parameters": {
                    "vertex_type": "Person",
                    "vertex_id": "user123"
                }
            }
        ]
    ),
    
    "tigergraph__get_nodes": ToolMetadata(
        category=ToolCategory.DATA,
        prerequisites=[],
        related_tools=["tigergraph__get_node", "tigergraph__get_vertex_count"],
        common_next_steps=["tigergraph__get_edges"],
        use_cases=[
            "Retrieving multiple vertices of a type",
            "Exploring graph data",
            "Data export and analysis"
        ],
        complexity="basic",
        keywords=["get", "retrieve", "fetch", "list", "multiple", "nodes", "vertices"],
        examples=[
            {
                "description": "Get all person nodes (limited)",
                "parameters": {
                    "vertex_type": "Person",
                    "limit": 100
                }
            }
        ]
    ),
    
    # Edge Operations
    "tigergraph__add_edge": ToolMetadata(
        category=ToolCategory.DATA,
        prerequisites=["tigergraph__add_node", "tigergraph__describe_graph"],
        related_tools=["tigergraph__add_edges", "tigergraph__get_edge"],
        common_next_steps=["tigergraph__get_node_edges", "tigergraph__get_neighbors"],
        use_cases=[
            "Creating a relationship between two vertices",
            "Connecting entities in the graph",
            "Building graph structure"
        ],
        complexity="basic",
        keywords=["add", "create", "connect", "relationship", "edge", "link"],
        examples=[
            {
                "description": "Create a friendship edge",
                "parameters": {
                    "edge_type": "FOLLOWS",
                    "from_vertex_type": "Person",
                    "from_vertex_id": "user1",
                    "to_vertex_type": "Person",
                    "to_vertex_id": "user2",
                    "attributes": {"since": "2024-01-15"}
                }
            }
        ]
    ),
    
    "tigergraph__add_edges": ToolMetadata(
        category=ToolCategory.DATA,
        prerequisites=["tigergraph__add_nodes", "tigergraph__describe_graph"],
        related_tools=["tigergraph__add_edge"],
        common_next_steps=["tigergraph__get_edge_count"],
        use_cases=[
            "Batch loading multiple edges",
            "Building graph structure efficiently",
            "Importing relationship data"
        ],
        complexity="basic",
        keywords=["add", "create", "batch", "multiple", "edges", "relationships", "bulk"],
        examples=[]
    ),
    
    # Query Operations
    "tigergraph__run_query": ToolMetadata(
        category=ToolCategory.QUERY,
        prerequisites=["tigergraph__describe_graph"],
        related_tools=["tigergraph__run_installed_query", "tigergraph__get_neighbors"],
        common_next_steps=[],
        use_cases=[
            "Ad-hoc querying without installing",
            "Testing queries before installation",
            "Simple data retrieval operations",
            "Running openCypher or GSQL queries"
        ],
        complexity="intermediate",
        keywords=["query", "search", "find", "select", "interpret", "gsql", "cypher"],
        examples=[
            {
                "description": "Simple GSQL query",
                "parameters": {
                    "query_text": "INTERPRET QUERY () FOR GRAPH MyGraph { SELECT v FROM Person:v LIMIT 5; PRINT v; }"
                }
            },
            {
                "description": "openCypher query",
                "parameters": {
                    "query_text": "INTERPRET OPENCYPHER QUERY () FOR GRAPH MyGraph { MATCH (n:Person) RETURN n LIMIT 5 }"
                }
            }
        ]
    ),
    
    "tigergraph__get_neighbors": ToolMetadata(
        category=ToolCategory.QUERY,
        prerequisites=[],
        related_tools=["tigergraph__get_node_edges", "tigergraph__run_query"],
        common_next_steps=[],
        use_cases=[
            "Finding vertices connected to a given vertex",
            "1-hop graph traversal",
            "Discovering relationships"
        ],
        complexity="basic",
        keywords=["neighbors", "connected", "adjacent", "traverse", "related"],
        examples=[
            {
                "description": "Get friends of a person",
                "parameters": {
                    "vertex_type": "Person",
                    "vertex_id": "user1",
                    "edge_type": "FOLLOWS"
                }
            }
        ]
    ),
    
    # Vector Operations
    "tigergraph__add_vector_attribute": ToolMetadata(
        category=ToolCategory.VECTOR,
        prerequisites=["tigergraph__describe_graph"],
        related_tools=["tigergraph__drop_vector_attribute", "tigergraph__get_vector_index_status"],
        common_next_steps=["tigergraph__get_vector_index_status", "tigergraph__upsert_vectors"],
        use_cases=[
            "Adding vector/embedding support to existing vertex types",
            "Setting up semantic search capabilities",
            "Enabling similarity-based queries"
        ],
        complexity="intermediate",
        keywords=["vector", "embedding", "add", "attribute", "similarity", "semantic"],
        examples=[
            {
                "description": "Add embedding attribute for documents",
                "parameters": {
                    "vertex_type": "Document",
                    "vector_name": "embedding",
                    "dimension": 384,
                    "metric": "COSINE"
                }
            },
            {
                "description": "Add embedding for products (higher dimension)",
                "parameters": {
                    "vertex_type": "Product",
                    "vector_name": "feature_vector",
                    "dimension": 1536,
                    "metric": "L2"
                }
            }
        ]
    ),
    
    "tigergraph__upsert_vectors": ToolMetadata(
        category=ToolCategory.VECTOR,
        prerequisites=["tigergraph__add_vector_attribute", "tigergraph__get_vector_index_status"],
        related_tools=["tigergraph__search_top_k_similarity", "tigergraph__fetch_vector"],
        common_next_steps=["tigergraph__get_vector_index_status", "tigergraph__search_top_k_similarity"],
        use_cases=[
            "Loading embedding vectors into the graph",
            "Updating vector data for vertices",
            "Populating semantic search index"
        ],
        complexity="intermediate",
        keywords=["vector", "embedding", "upsert", "load", "insert", "update"],
        examples=[
            {
                "description": "Upsert document embeddings",
                "parameters": {
                    "vertex_type": "Document",
                    "vector_attribute": "embedding",
                    "vectors": [
                        {
                            "vertex_id": "doc1",
                            "vector": [0.1, 0.2, 0.3],
                            "attributes": {"title": "Document 1"}
                        }
                    ]
                }
            }
        ]
    ),
    
    "tigergraph__search_top_k_similarity": ToolMetadata(
        category=ToolCategory.VECTOR,
        prerequisites=["tigergraph__upsert_vectors", "tigergraph__get_vector_index_status"],
        related_tools=["tigergraph__fetch_vector"],
        common_next_steps=[],
        use_cases=[
            "Finding similar documents or items",
            "Semantic search operations",
            "Recommendation based on similarity"
        ],
        complexity="intermediate",
        keywords=["vector", "search", "similarity", "nearest", "semantic", "find", "similar"],
        examples=[
            {
                "description": "Find similar documents",
                "parameters": {
                    "vertex_type": "Document",
                    "vector_attribute": "embedding",
                    "query_vector": [0.1, 0.2, 0.3],
                    "top_k": 10
                }
            }
        ]
    ),
    
    # Loading Operations
    "tigergraph__create_loading_job": ToolMetadata(
        category=ToolCategory.LOADING,
        prerequisites=["tigergraph__describe_graph"],
        related_tools=["tigergraph__run_loading_job_with_file", "tigergraph__run_loading_job_with_data"],
        common_next_steps=["tigergraph__run_loading_job_with_file", "tigergraph__get_loading_jobs"],
        use_cases=[
            "Setting up data ingestion from CSV/JSON files",
            "Defining how file columns map to vertex/edge attributes",
            "Preparing for bulk data loading"
        ],
        complexity="advanced",
        keywords=["loading", "job", "create", "define", "ingest", "import"],
        examples=[]
    ),
    
    "tigergraph__run_loading_job_with_file": ToolMetadata(
        category=ToolCategory.LOADING,
        prerequisites=["tigergraph__create_loading_job"],
        related_tools=["tigergraph__run_loading_job_with_data", "tigergraph__get_loading_job_status"],
        common_next_steps=["tigergraph__get_loading_job_status", "tigergraph__get_vertex_count"],
        use_cases=[
            "Loading data from CSV or JSON files",
            "Bulk import of graph data",
            "ETL operations"
        ],
        complexity="intermediate",
        keywords=["loading", "job", "run", "file", "import", "bulk"],
        examples=[]
    ),
    
    # Statistics
    "tigergraph__get_vertex_count": ToolMetadata(
        category=ToolCategory.UTILITY,
        prerequisites=[],
        related_tools=["tigergraph__get_edge_count", "tigergraph__get_nodes"],
        common_next_steps=[],
        use_cases=[
            "Verifying data was loaded",
            "Monitoring graph size",
            "Data validation"
        ],
        complexity="basic",
        keywords=["count", "statistics", "size", "vertex", "node", "total"],
        examples=[
            {
                "description": "Count all vertices",
                "parameters": {}
            },
            {
                "description": "Count specific vertex type",
                "parameters": {"vertex_type": "Person"}
            }
        ]
    ),
    
    "tigergraph__get_edge_count": ToolMetadata(
        category=ToolCategory.UTILITY,
        prerequisites=[],
        related_tools=["tigergraph__get_vertex_count"],
        common_next_steps=[],
        use_cases=[
            "Verifying relationships were created",
            "Monitoring graph connectivity",
            "Data validation"
        ],
        complexity="basic",
        keywords=["count", "statistics", "size", "edge", "relationship", "total"],
        examples=[]
    ),
}


def get_tool_metadata(tool_name: str) -> Optional[ToolMetadata]:
    """Get metadata for a specific tool."""
    return TOOL_METADATA.get(tool_name)


def get_tools_by_category(category: ToolCategory) -> List[str]:
    """Get all tool names in a specific category."""
    return [
        tool_name for tool_name, metadata in TOOL_METADATA.items()
        if metadata.category == category
    ]


def search_tools_by_keywords(keywords: List[str]) -> List[str]:
    """Search for tools matching any of the provided keywords."""
    matching_tools = []
    keywords_lower = [k.lower() for k in keywords]
    
    for tool_name, metadata in TOOL_METADATA.items():
        # Check if any keyword matches
        for keyword in keywords_lower:
            if any(keyword in mk.lower() for mk in metadata.keywords):
                matching_tools.append(tool_name)
                break
            # Also check in use cases
            if any(keyword in uc.lower() for uc in metadata.use_cases):
                matching_tools.append(tool_name)
                break
    
    return matching_tools
