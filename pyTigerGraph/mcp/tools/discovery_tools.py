# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Discovery and navigation tools for LLMs.

These tools help LLMs discover the right tools for their tasks and understand
common workflows.
"""

import json
from typing import List, Optional
from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..tool_metadata import TOOL_METADATA, ToolCategory, search_tools_by_keywords, get_tools_by_category
from ..response_formatter import format_success, format_list_response


class ToolDiscoveryInput(BaseModel):
    """Input for discovering relevant tools."""
    task_description: str = Field(
        ..., 
        description=(
            "Describe what you want to accomplish in natural language.\n"
            "Examples:\n"
            "  - 'add multiple users to the graph'\n"
            "  - 'find similar documents using embeddings'\n"
            "  - 'understand the graph structure'\n"
            "  - 'load data from a CSV file'"
        )
    )
    category: Optional[str] = Field(
        None, 
        description=(
            "Filter by category: 'schema', 'data', 'query', 'vector', 'loading', 'utility'.\n"
            "Leave empty to search all categories."
        )
    )
    limit: int = Field(
        5,
        description="Maximum number of tools to return (default: 5)"
    )


class GetWorkflowInput(BaseModel):
    """Input for getting workflow templates."""
    workflow_type: str = Field(
        ..., 
        description=(
            "Type of workflow to retrieve:\n"
            "  - 'create_graph': Set up a new graph with schema\n"
            "  - 'load_data': Import data into an existing graph\n"
            "  - 'query_data': Query and analyze graph data\n"
            "  - 'vector_search': Set up and use vector similarity search\n"
            "  - 'graph_analysis': Analyze graph structure and statistics\n"
            "  - 'setup_connection': Initial connection setup and verification"
        )
    )


class GetToolInfoInput(BaseModel):
    """Input for getting detailed information about a specific tool."""
    tool_name: str = Field(
        ...,
        description=(
            "Name of the tool to get information about.\n"
            "Example: 'tigergraph__add_node' or 'tigergraph__search_top_k_similarity'"
        )
    )


# Tool definitions
discover_tools_tool = Tool(
    name=TigerGraphToolName.DISCOVER_TOOLS,
    description=(
        "Discover which TigerGraph tools are relevant for your task.\n\n"
        "**Use this tool when:**\n"
        "  - You're unsure which tool to use for your goal\n"
        "  - You want to explore available capabilities\n"
        "  - You need suggestions for accomplishing a task\n\n"
        "**Returns:**\n"
        "  - List of recommended tools with descriptions\n"
        "  - Use cases and complexity ratings\n"
        "  - Prerequisites and related tools\n"
        "  - Example parameters\n\n"
        "**Example:**\n"
        "  task_description: 'I want to add multiple users to the graph'"
    ),
    inputSchema=ToolDiscoveryInput.model_json_schema(),
)

get_workflow_tool = Tool(
    name=TigerGraphToolName.GET_WORKFLOW,
    description=(
        "Get a step-by-step workflow template for common TigerGraph tasks.\n\n"
        "**Use this tool when:**\n"
        "  - You need to complete a complex multi-step task\n"
        "  - You want to follow best practices\n"
        "  - You're new to TigerGraph and need guidance\n\n"
        "**Returns:**\n"
        "  - Ordered list of tools to use\n"
        "  - Example parameters for each step\n"
        "  - Explanations of what each step accomplishes\n\n"
        "**Available workflows:** create_graph, load_data, query_data, vector_search, graph_analysis, setup_connection"
    ),
    inputSchema=GetWorkflowInput.model_json_schema(),
)

get_tool_info_tool = Tool(
    name=TigerGraphToolName.GET_TOOL_INFO,
    description=(
        "Get detailed information about a specific TigerGraph tool.\n\n"
        "**Use this tool when:**\n"
        "  - You want to understand a tool's capabilities\n"
        "  - You need examples of how to use a tool\n"
        "  - You want to know prerequisites or related tools\n\n"
        "**Returns:**\n"
        "  - Detailed tool description\n"
        "  - Use cases and examples\n"
        "  - Prerequisites and related tools\n"
        "  - Common next steps"
    ),
    inputSchema=GetToolInfoInput.model_json_schema(),
)


# Workflow templates
WORKFLOWS = {
    "setup_connection": {
        "name": "Setup and Verify Connection",
        "description": "Initial setup to verify connection and explore available graphs",
        "steps": [
            {
                "step": 1,
                "tool": "tigergraph__list_graphs",
                "description": "List all available graphs to see what exists",
                "parameters": {},
                "rationale": "First, discover what graphs are available in your TigerGraph instance"
            },
            {
                "step": 2,
                "tool": "tigergraph__show_graph_details",
                "description": "Get detailed schema of a specific graph",
                "parameters": {"graph_name": "<select from step 1>"},
                "rationale": "Understand the structure, vertex types, and edge types of the graph you'll work with"
            },
            {
                "step": 3,
                "tool": "tigergraph__get_vertex_count",
                "description": "Check how much data exists in the graph",
                "parameters": {},
                "rationale": "Verify the graph has data and get a sense of scale"
            }
        ]
    },
    
    "create_graph": {
        "name": "Create a New Graph",
        "description": "Complete workflow for creating a new graph from scratch",
        "steps": [
            {
                "step": 1,
                "tool": "tigergraph__list_graphs",
                "description": "Check existing graphs to avoid naming conflicts",
                "parameters": {},
                "rationale": "Ensure your graph name is unique"
            },
            {
                "step": 2,
                "tool": "tigergraph__create_graph",
                "description": "Create graph with vertex and edge type definitions",
                "parameters": {
                    "graph_name": "MyGraph",
                    "vertex_types": [
                        {
                            "name": "Person",
                            "attributes": [
                                {"name": "name", "type": "STRING"},
                                {"name": "age", "type": "INT"},
                                {"name": "email", "type": "STRING"}
                            ]
                        },
                        {
                            "name": "Company",
                            "attributes": [
                                {"name": "name", "type": "STRING"},
                                {"name": "industry", "type": "STRING"}
                            ]
                        }
                    ],
                    "edge_types": [
                        {
                            "name": "WORKS_AT",
                            "from_vertex": "Person",
                            "to_vertex": "Company",
                            "attributes": [
                                {"name": "since", "type": "INT"}
                            ]
                        },
                        {
                            "name": "KNOWS",
                            "from_vertex": "Person",
                            "to_vertex": "Person"
                        }
                    ]
                },
                "rationale": "Define the schema that represents your domain model"
            },
            {
                "step": 3,
                "tool": "tigergraph__show_graph_details",
                "description": "Verify the schema was created correctly",
                "parameters": {"graph_name": "MyGraph"},
                "rationale": "Confirm the graph structure matches your design"
            }
        ]
    },
    
    "load_data": {
        "name": "Load Data into Existing Graph",
        "description": "Import vertices and edges into a graph that already has a schema",
        "steps": [
            {
                "step": 1,
                "tool": "tigergraph__show_graph_details",
                "description": "Understand the graph schema before loading data",
                "parameters": {},
                "rationale": "Know what vertex/edge types exist and their required attributes"
            },
            {
                "step": 2,
                "tool": "tigergraph__add_nodes",
                "description": "Add vertices in batch (more efficient than one-by-one)",
                "parameters": {
                    "vertex_type": "Person",
                    "vertices": [
                        {"id": "p1", "name": "Alice", "age": 30, "email": "alice@example.com"},
                        {"id": "p2", "name": "Bob", "age": 25, "email": "bob@example.com"},
                        {"id": "p3", "name": "Carol", "age": 35, "email": "carol@example.com"}
                    ]
                },
                "rationale": "Load vertices before edges (edges require vertices to exist)"
            },
            {
                "step": 3,
                "tool": "tigergraph__add_edges",
                "description": "Add edges to connect vertices",
                "parameters": {
                    "edge_type": "KNOWS",
                    "edges": [
                        {
                            "from_type": "Person",
                            "from_id": "p1",
                            "to_type": "Person",
                            "to_id": "p2"
                        },
                        {
                            "from_type": "Person",
                            "from_id": "p2",
                            "to_type": "Person",
                            "to_id": "p3"
                        }
                    ]
                },
                "rationale": "Create relationships between entities"
            },
            {
                "step": 4,
                "tool": "tigergraph__get_vertex_count",
                "description": "Verify vertices were loaded correctly",
                "parameters": {"vertex_type": "Person"},
                "rationale": "Data validation - ensure expected number of vertices exist"
            },
            {
                "step": 5,
                "tool": "tigergraph__get_edge_count",
                "description": "Verify edges were created",
                "parameters": {"edge_type": "KNOWS"},
                "rationale": "Data validation - ensure relationships were established"
            }
        ]
    },
    
    "query_data": {
        "name": "Query and Analyze Graph Data",
        "description": "Run queries to extract insights from the graph",
        "steps": [
            {
                "step": 1,
                "tool": "tigergraph__show_graph_details",
                "description": "Review schema to understand what can be queried",
                "parameters": {},
                "rationale": "Know the vertex/edge types and attributes available for querying"
            },
            {
                "step": 2,
                "tool": "tigergraph__get_node",
                "description": "Retrieve a specific vertex to examine its data",
                "parameters": {
                    "vertex_type": "Person",
                    "vertex_id": "p1"
                },
                "rationale": "Simple data retrieval for a known vertex"
            },
            {
                "step": 3,
                "tool": "tigergraph__get_neighbors",
                "description": "Find vertices connected to a specific vertex",
                "parameters": {
                    "vertex_type": "Person",
                    "vertex_id": "p1",
                    "edge_type": "KNOWS"
                },
                "rationale": "1-hop traversal to discover relationships"
            },
            {
                "step": 4,
                "tool": "tigergraph__run_query",
                "description": "Run a custom GSQL or Cypher query for complex analysis",
                "parameters": {
                    "query_text": "INTERPRET QUERY () FOR GRAPH MyGraph { SELECT v FROM Person:v WHERE v.age > 25 LIMIT 10; PRINT v; }"
                },
                "rationale": "Complex queries for sophisticated analysis"
            }
        ]
    },
    
    "vector_search": {
        "name": "Vector Similarity Search Setup and Usage",
        "description": "Set up semantic search with embeddings and perform similarity queries",
        "steps": [
            {
                "step": 1,
                "tool": "tigergraph__show_graph_details",
                "description": "Check existing vertex types",
                "parameters": {},
                "rationale": "Identify which vertex type should have vector attributes"
            },
            {
                "step": 2,
                "tool": "tigergraph__add_vector_attribute",
                "description": "Add vector/embedding attribute to a vertex type",
                "parameters": {
                    "vertex_type": "Document",
                    "vector_name": "embedding",
                    "dimension": 384,
                    "metric": "COSINE"
                },
                "rationale": "Enable semantic search by adding vector storage capability"
            },
            {
                "step": 3,
                "tool": "tigergraph__get_vector_index_status",
                "description": "Wait for the vector index to be ready",
                "parameters": {},
                "rationale": "Vector index must be built before searching (can take time for large graphs)"
            },
            {
                "step": 4,
                "tool": "tigergraph__upsert_vectors",
                "description": "Load embedding vectors into vertices",
                "parameters": {
                    "vertex_type": "Document",
                    "vector_attribute": "embedding",
                    "vectors": [
                        {
                            "vertex_id": "doc1",
                            "vector": [0.1, 0.2, 0.3],  # 384 dimensions
                            "attributes": {"title": "Document 1", "content": "..."}
                        }
                    ]
                },
                "rationale": "Populate the graph with embedding data"
            },
            {
                "step": 5,
                "tool": "tigergraph__get_vector_index_status",
                "description": "Verify index is ready after loading data",
                "parameters": {},
                "rationale": "Ensure index has processed new vectors"
            },
            {
                "step": 6,
                "tool": "tigergraph__search_top_k_similarity",
                "description": "Perform similarity search with a query vector",
                "parameters": {
                    "vertex_type": "Document",
                    "vector_attribute": "embedding",
                    "query_vector": [0.1, 0.2, 0.3],  # Same dimension as stored vectors
                    "top_k": 10
                },
                "rationale": "Find most similar documents to the query"
            }
        ]
    },
    
    "graph_analysis": {
        "name": "Graph Analysis and Statistics",
        "description": "Analyze graph structure, connectivity, and statistics",
        "steps": [
            {
                "step": 1,
                "tool": "tigergraph__show_graph_details",
                "description": "Get schema and structure overview",
                "parameters": {},
                "rationale": "Understand the graph composition"
            },
            {
                "step": 2,
                "tool": "tigergraph__get_vertex_count",
                "description": "Count vertices by type",
                "parameters": {},
                "rationale": "Understand the size and distribution of data"
            },
            {
                "step": 3,
                "tool": "tigergraph__get_edge_count",
                "description": "Count edges by type",
                "parameters": {},
                "rationale": "Understand connectivity patterns"
            },
            {
                "step": 4,
                "tool": "tigergraph__get_node_degree",
                "description": "Analyze connectivity of specific vertices",
                "parameters": {
                    "vertex_type": "Person",
                    "vertex_id": "p1"
                },
                "rationale": "Find highly connected nodes (hubs)"
            },
            {
                "step": 5,
                "tool": "tigergraph__run_query",
                "description": "Run custom analytics queries",
                "parameters": {
                    "query_text": "INTERPRET QUERY () FOR GRAPH MyGraph { /* Custom analysis */ }"
                },
                "rationale": "Perform sophisticated graph algorithms and analysis"
            }
        ]
    }
}


# Tool implementations
async def discover_tools(
    task_description: str,
    category: Optional[str] = None,
    limit: int = 5,
) -> List[TextContent]:
    """Discover relevant tools based on task description."""
    
    # Extract keywords from task description
    task_lower = task_description.lower()
    keywords = task_lower.split()
    
    # Search for matching tools
    matching_tools = search_tools_by_keywords(keywords)
    
    # Filter by category if specified
    if category:
        try:
            cat = ToolCategory(category.lower())
            matching_tools = [
                tool for tool in matching_tools
                if tool in TOOL_METADATA and TOOL_METADATA[tool].category == cat
            ]
        except ValueError:
            pass  # Invalid category, ignore filter
    
    # Remove duplicates and limit results
    seen = set()
    unique_tools = []
    for tool in matching_tools:
        if tool not in seen and tool in TOOL_METADATA:
            seen.add(tool)
            unique_tools.append(tool)
            if len(unique_tools) >= limit:
                break
    
    # If no matches, suggest discovery strategies
    if not unique_tools:
        return format_success(
            operation="discover_tools",
            summary=f"Error: No specific tools found for '{task_description}'",
            suggestions=[
                "Try rephrasing your task with different keywords",
                "Use 'tigergraph__get_workflow' to see common workflow patterns",
                "Use 'tigergraph__show_graph_details' to understand what's available",
                "Browse tools by category: schema, data, query, vector, loading, utility"
            ],
            metadata={"task": task_description, "category": category}
        )
    
    # Build detailed response
    recommended_tools = []
    for tool_name in unique_tools:
        metadata = TOOL_METADATA[tool_name]
        recommended_tools.append({
            "tool_name": tool_name,
            "category": metadata.category,
            "complexity": metadata.complexity,
            "use_cases": metadata.use_cases[:3],  # Limit to top 3 use cases
            "prerequisites": metadata.prerequisites,
            "related_tools": metadata.related_tools[:3],
            "examples": metadata.examples[:1] if metadata.examples else []
        })
    
    suggestions = [
        f"Use 'tigergraph__get_tool_info' with tool_name='{unique_tools[0]}' for more details",
        "Check 'prerequisites' before using a tool",
        "Explore 'related_tools' for alternative or complementary operations"
    ]
    
    if len(unique_tools) < len(matching_tools):
        suggestions.append(f"Increase 'limit' parameter to see more tools (found {len(matching_tools)} total)")
    
    return format_success(
        operation="discover_tools",
        summary=f"Found {len(unique_tools)} relevant tools for '{task_description}'",
        data={
            "task": task_description,
            "recommended_tools": recommended_tools
        },
        suggestions=suggestions,
        metadata={
            "total_matches": len(matching_tools),
            "returned": len(unique_tools)
        }
    )


async def get_workflow(workflow_type: str) -> List[TextContent]:
    """Get a workflow template for common tasks."""
    
    if workflow_type not in WORKFLOWS:
        available = list(WORKFLOWS.keys())
        return format_success(
            operation="get_workflow",
            summary=f"Error: Unknown workflow type '{workflow_type}'",
            data={"available_workflows": available},
            suggestions=[
                f"Try one of these workflows: {', '.join(available)}",
                "Use 'discover_tools' to find tools for specific tasks"
            ]
        )
    
    workflow = WORKFLOWS[workflow_type]
    
    return format_success(
        operation="get_workflow",
        summary=f"Success: Workflow: {workflow['name']}",
        data={
            "workflow_type": workflow_type,
            "name": workflow['name'],
            "description": workflow['description'],
            "steps": workflow['steps'],
            "total_steps": len(workflow['steps'])
        },
        suggestions=[
            "Execute each step in order",
            "Adapt parameters to your specific use case",
            "Verify each step completed successfully before proceeding",
            "Use 'tigergraph__get_tool_info' to learn more about each tool"
        ],
        metadata={"workflow_type": workflow_type}
    )


async def get_tool_info(tool_name: str) -> List[TextContent]:
    """Get detailed information about a specific tool."""
    
    if tool_name not in TOOL_METADATA:
        return format_success(
            operation="get_tool_info",
            summary=f"Error: Tool '{tool_name}' not found",
            suggestions=[
                "Check the tool name spelling",
                "Use 'discover_tools' to find available tools",
                "Tool names follow the pattern: 'tigergraph__<operation>'"
            ]
        )
    
    metadata = TOOL_METADATA[tool_name]
    
    return format_success(
        operation="get_tool_info",
        summary=f"Success: Tool Information: {tool_name}",
        data={
            "tool_name": tool_name,
            "category": metadata.category,
            "complexity": metadata.complexity,
            "use_cases": metadata.use_cases,
            "prerequisites": metadata.prerequisites,
            "related_tools": metadata.related_tools,
            "common_next_steps": metadata.common_next_steps,
            "examples": metadata.examples,
            "keywords": metadata.keywords
        },
        suggestions=[
            f"Try the tool with the provided examples",
            f"Check prerequisites: {', '.join(metadata.prerequisites) if metadata.prerequisites else 'None'}",
            f"Explore related tools: {', '.join(metadata.related_tools[:3]) if metadata.related_tools else 'None'}"
        ]
    )
