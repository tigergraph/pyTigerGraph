# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""GSQL operation tools for MCP."""

import os
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field
from mcp.types import Tool, TextContent

from ..tool_names import TigerGraphToolName
from ..connection_manager import get_connection
from pyTigerGraph.common.exception import TigerGraphException


# Default LLM provider and model
DEFAULT_LLM_PROVIDER = "openai"
DEFAULT_LLM_MODEL = "gpt-4o"


def get_llm_config() -> Tuple[str, str]:
    """Get LLM provider and model from environment variables.
    
    Configuration priority:
    1. LLM_MODEL - If contains ':', split into 'provider:model'
    2. LLM_MODEL + LLM_PROVIDER - Use LLM_MODEL as model name and LLM_PROVIDER as provider
    3. Defaults - Falls back to 'openai' provider and 'gpt-4o' model
    
    Environment variables:
        LLM_MODEL: Model name, optionally with provider prefix (e.g., 'gpt-4o' or 'openai:gpt-4o')
        LLM_PROVIDER: Provider name (e.g., 'openai', 'anthropic', 'bedrock_converse')
    
    Returns:
        Tuple of (provider, model)
    
    Raises:
        ValueError: If configuration is invalid
    """
    llm_model = os.getenv("LLM_MODEL", "").strip()
    llm_provider = os.getenv("LLM_PROVIDER", "").strip()
    
    # Case 1: LLM_MODEL contains ":" - parse as provider:model
    if llm_model and ":" in llm_model:
        parts = llm_model.split(":", 1)
        provider = parts[0].strip()
        model = parts[1].strip()
        
        if not provider or not model:
            raise ValueError(
                f"Invalid LLM_MODEL format: '{llm_model}'. "
                f"When using 'provider:model' format, both must be specified."
            )
        return provider, model
    
    # Case 2: LLM_MODEL specified without ":" - need LLM_PROVIDER
    if llm_model:
        if llm_provider:
            return llm_provider, llm_model
        else:
            # No provider specified, use default provider
            return DEFAULT_LLM_PROVIDER, llm_model
    
    # Case 3: No LLM_MODEL - use defaults
    if llm_provider:
        # Provider specified but no model - use default model with specified provider
        return llm_provider, DEFAULT_LLM_MODEL
    
    # Case 4: Nothing specified - use all defaults
    return DEFAULT_LLM_PROVIDER, DEFAULT_LLM_MODEL


class GSQLToolInput(BaseModel):
    """Input schema for running GSQL command."""
    graph_name: Optional[str] = Field(None, description="Name of the graph. If not provided, uses default connection.")
    command: str = Field(..., description="GSQL command to execute.")


class GenerateGSQLToolInput(BaseModel):
    """Input schema for generating GSQL from natural language."""
    query_description: str = Field(
        ..., 
        description="A natural language description of what data you want to retrieve. Examples: 'Find all users who purchased more than 5 items', 'Count vertices by type', 'Find shortest path between two nodes'"
    )
    graph_name: Optional[str] = Field(
        None, 
        description="Name of the graph. If provided, the schema will be fetched to generate more accurate GSQL."
    )


class GenerateCypherToolInput(BaseModel):
    """Input schema for generating Cypher from natural language."""
    query_description: str = Field(
        ..., 
        description="A natural language description of what data you want to retrieve. Examples: 'Find all users who purchased more than 5 items', 'Find friends of friends', 'Match patterns in the graph'"
    )
    graph_name: str = Field(
        ..., 
        description="Name of the graph. Required for Cypher queries as they need to be wrapped in INTERPRET OPENCYPHER QUERY for the specific graph."
    )


gsql_tool = Tool(
    name=TigerGraphToolName.GSQL,
    description=(
        "Execute a GSQL command on TigerGraph. "
        "Use this for administrative tasks (e.g., creating users, granting roles) or schema modifications (e.g., CREATE VERTEX). "
        "Do NOT use this for running data queries (SELECT statements) - use run_query instead. "
        "Example: `CREATE USER alice WITH PASSWORD 'password'` or `LS`."
    ),
    inputSchema=GSQLToolInput.model_json_schema(),
)


generate_gsql_query_tool = Tool(
    name=TigerGraphToolName.GENERATE_GSQL,
    description=(
        "Generate a GSQL query from a natural language description using an LLM. "
        "Use this tool when you need to create a GSQL query but are unsure of the exact syntax. "
        "The generated query can then be executed using the gsql tool. "
        "For best results, provide the graph_name so the schema can be used to generate accurate queries. "
        "Configure the LLM via env vars: LLM_MODEL (e.g., 'gpt-4o' or 'openai:gpt-4o') and optionally LLM_PROVIDER."
    ),
    inputSchema=GenerateGSQLToolInput.model_json_schema(),
)


generate_cypher_query_tool = Tool(
    name=TigerGraphToolName.GENERATE_CYPHER,
    description=(
        "Generate an openCypher query from a natural language description using an LLM. "
        "Use this tool when you prefer Cypher syntax over GSQL. "
        "The generated query will be wrapped in TigerGraph's INTERPRET OPENCYPHER QUERY format. "
        "graph_name is required as the query needs to specify the target graph. "
        "Configure the LLM via env vars: LLM_MODEL (e.g., 'gpt-4o' or 'openai:gpt-4o') and optionally LLM_PROVIDER."
    ),
    inputSchema=GenerateCypherToolInput.model_json_schema(),
)


# Default GSQL generation prompt template
GSQL_GENERATION_PROMPT = """You are a GSQL query expert for TigerGraph. Generate a valid GSQL query based on the user's natural language request.

{schema_section}

## Query Construction Tips

1. **Start broad**: Use category filters to find relevant entity/event types
2. **Add specificity**: Use pattern matching (LIKE) for company-specific queries  
3. **Follow relationships**: Traverse Has_Action edges to find connections
4. **Filter actions**: Check relationship type to get precise information
5. **Get context**: Always consider retrieving document chunks for supporting evidence
6. **Structure output**: Use MapAccum for grouped results, SetAccum for unique lists

## GSQL Query Examples

### Count vertices by type
```gsql
SumAccum<INT> @@count;
Result = SELECT v FROM VertexType:v
ACCUM @@count += 1;
PRINT @@count;
```

### Count by attribute value
```gsql
MapAccum<STRING, INT> @@catCount;
Result = SELECT v FROM VertexType:v
ACCUM @@catCount += (v.attribute -> 1);
PRINT @@catCount;
```

### Find connected vertices (1-hop)
```gsql
SetAccum<STRING> @@results;
Result = SELECT t FROM SourceType:s -(EdgeType:e)- TargetType:t
WHERE s.id == "some_id"
ACCUM @@results += t.id;
PRINT @@results;
```

### Filter with conditions
```gsql
SetAccum<STRING> @@filtered;
Result = SELECT v FROM VertexType:v
WHERE v.attribute == "value" AND v.count > 10
ACCUM @@filtered += v.id
LIMIT 100;
PRINT @@filtered;
```

### Pattern matching with LIKE
```gsql
SetAccum<STRING> @@matched;
Result = SELECT v FROM VertexType:v
WHERE v.name LIKE "%pattern%"
ACCUM @@matched += v.id;
PRINT @@matched;
```

### Multi-hop traversal
```gsql
SetAccum<STRING> @@results;
Result = SELECT v3 FROM Type1:v1 -(Edge1)- Type2:v2 -(Edge2)- Type3:v3
WHERE v1.id == "start_id"
ACCUM @@results += v3.id
LIMIT 50;
PRINT @@results;
```

## Rules
1. Always use appropriate accumulators (SumAccum, SetAccum, MapAccum, ListAccum, etc.)
2. Use proper GSQL syntax with SELECT, FROM, WHERE, ACCUM clauses
3. End queries with PRINT statement
4. Use LIMIT for potentially large result sets
5. Only output the GSQL query, no explanations or markdown code blocks

Generate a GSQL query for the following request:
"""


# Default Cypher generation prompt template (adapted for TigerGraph openCypher limitations)
CYPHER_GENERATION_PROMPT = """You are an openCypher query expert for TigerGraph. Generate a valid openCypher query based on the user's natural language request.

{schema_section}

## TigerGraph openCypher Guidelines

1. Only include attributes that are found in the schema. Never include any attributes that are not found in the schema.
2. Always make sure the attributes used exist in the vertex type or edge type referenced.
3. Use as few vertex types, edge types, and attributes as possible.
4. Always use double quotes for strings instead of single quotes.
5. Always convert strings to lower case using toLower() function for string comparison in WHERE clause.
6. Never use directed edge pattern. Always use UNDIRECTED pattern: (a)-[:EDGE]-(b) instead of (a)-[:EDGE]->(b).
7. Always ensure the edge used starts from and ends with correct vertex types matching the schema.
8. Use meaningful alias names connected by underscore. Avoid using lowercase versions of vertex/edge types as aliases (e.g., use "p" or "src" instead of "person" for Person type).
9. Always add ASC or DESC for ORDER BY based on data type.
10. Include the entity from the WHERE clause in the final RETURN result.

## Supported Clauses
- MATCH / OPTIONAL MATCH / MANDATORY MATCH: Match patterns in the graph
- WHERE: Filter results
- RETURN / WITH: Project query results, alias fields, chain query parts
- ORDER BY / SKIP / LIMIT: Control output order, offset, and size
- DELETE / DETACH DELETE: Delete nodes/edges

## Supported Operators
- Mathematical: +, -, *, /, %, ^ (exponent)
- Comparison: =, <, <=, >, >=, <>, IS NULL, IS NOT NULL
- Boolean: AND, OR, NOT, XOR
- String/List: CONTAINS, STARTS WITH, ENDS WITH, IN, DISTINCT, [ ] (subscript), . (property access)

## Supported Functions
- Aggregation: count(), sum(), avg(), min(), max(), stDev(), stDevP()
- Math: abs(), sqrt(), log(), exp(), sin(), cos(), tan(), radians(), degrees()
- String: left(), right(), substring(), replace(), trim(), toLower(), toUpper(), split()
- List: head(), last(), size(), range(), coalesce(), tail()
- Others: id(), elementId(), labels(), properties(), timestamp()

## UNSUPPORTED Features (DO NOT USE)
- Clauses: CREATE, MERGE, REMOVE, SET, UNION, UNION ALL, UNWIND, CALL
- Functions: collect(), exists(), keys(), nodes(), relationships(), length(), percentileCont(), percentileDisc(), startNode(), endNode(), reverse()
- Path variables (e.g., p = (...)) are NOT supported
- Disconnected MATCH fragments are NOT supported
- WITH clause must group by exactly one vertex variable

## openCypher Query Examples

### Find all nodes of a type
```cypher
MATCH (p:Person)
RETURN p
LIMIT 10
```

### Find nodes with specific property (use toLower for string comparison)
```cypher
MATCH (p:Person)
WHERE toLower(p.name) = toLower("John")
RETURN p
```

### Find connected nodes (use UNDIRECTED edge pattern)
```cypher
MATCH (p:Person)-[:KNOWS]-(f:Person)
WHERE toLower(p.name) = toLower("Alice")
RETURN p.name AS source_name, f.name AS friend_name
```

### Count nodes by property
```cypher
MATCH (p:Person)
RETURN p.city AS city_name, count(*) AS total_count
ORDER BY total_count DESC
```

### Pattern matching with conditions
```cypher
MATCH (p:Person)-[r:PURCHASED]-(prod:Product)
WHERE r.amount > 100
RETURN p.name AS buyer_name, prod.name AS product_name, r.amount AS purchase_amount
ORDER BY purchase_amount DESC
LIMIT 20
```

### Aggregation queries
```cypher
MATCH (p:Person)-[:PURCHASED]-(prod:Product)
RETURN p.name AS buyer_name, sum(prod.price) AS total_spent
ORDER BY total_spent DESC
```

## Rules
1. ONLY output the Cypher query body, no explanations or markdown code blocks
2. Do NOT include USE GRAPH or INTERPRET OPENCYPHER QUERY wrapper - that will be added automatically
3. Always validate the syntax before responding
4. Use UNDIRECTED edge patterns only
5. Use toLower() for all string comparisons

Generate an openCypher query for the following request:
"""


async def gsql(
    command: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Execute a GSQL command."""
    try:
        conn = get_connection(graph_name=graph_name)
        result = await conn.gsql(command)
        message = f"Success: GSQL command executed successfully:\n{result}"
    except Exception as e:
        message = f"Failed to execute GSQL command due to: {str(e)}"
    return [TextContent(type="text", text=message)]


async def generate_gsql(
    query_description: str,
    graph_name: Optional[str] = None,
) -> List[TextContent]:
    """Generate a GSQL query from natural language description using LangChain's init_chat_model.
    
    Supports multiple LLM providers through environment variables:
    - LLM_MODEL: Model name, optionally with provider prefix (e.g., 'gpt-4o' or 'openai:gpt-4o')
    - LLM_PROVIDER: Provider name if not specified in LLM_MODEL (e.g., 'openai', 'anthropic', 'bedrock_converse')
    
    Args:
        query_description: Natural language description of the query to generate.
        graph_name: Optional graph name to fetch schema for better query generation.
    
    Returns:
        List containing the generated GSQL query.
    """
    try:
        # Try to import langchain's init_chat_model
        try:
            from langchain.chat_models import init_chat_model
        except ImportError:
            return [TextContent(
                type="text", 
                text="Error: LangChain package is not installed. Please install it with: pip install langchain"
            )]
        
        # Get LLM provider and model configuration from environment
        try:
            provider, model = get_llm_config()
        except ValueError as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        # Initialize the LLM using langchain's init_chat_model
        try:
            llm = init_chat_model(model, model_provider=provider)
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Failed to initialize LLM with provider '{provider}' and model '{model}': {str(e)}\n\n"
                     f"Make sure you have the required dependencies installed and API keys configured.\n"
                     f"Common providers: openai, anthropic, bedrock_converse, azure_openai, google_genai, ollama\n\n"
                     f"Configuration options:\n"
                     f"  - LLM_MODEL=provider:model (e.g., 'openai:gpt-4o')\n"
                     f"  - LLM_MODEL=model + LLM_PROVIDER=provider (e.g., LLM_MODEL='gpt-4o', LLM_PROVIDER='openai')"
            )]
        
        # Try to get schema if graph_name is provided
        schema_section = "## Graph Schema\n\nNo schema information available. Generate a generic GSQL query based on the request."
        if graph_name:
            try:
                conn = get_connection(graph_name=graph_name)
                schema = await conn.describe_graph()
                if schema:
                    schema_section = f"## Graph Schema\n\n{schema}"
            except Exception as e:
                schema_section = f"## Graph Schema\n\nCould not fetch schema: {str(e)}. Generate a generic GSQL query."
        
        # Build the prompt
        prompt = GSQL_GENERATION_PROMPT.format(schema_section=schema_section)
        
        # Create messages for the chat model
        from langchain_core.messages import SystemMessage, HumanMessage
        messages = [
            SystemMessage(content=prompt),
            HumanMessage(content=query_description)
        ]
        
        # Invoke the LLM
        response = llm.invoke(messages)
        gsql_query = response.content.strip()
        
        # Clean up the response - remove markdown code blocks if present
        if gsql_query.startswith("```gsql"):
            gsql_query = gsql_query[7:]
        elif gsql_query.startswith("```"):
            gsql_query = gsql_query[3:]
        if gsql_query.endswith("```"):
            gsql_query = gsql_query[:-3]
        
        gsql_query = gsql_query.strip()
        
        message = f"Success: Generated GSQL query (using {provider}:{model}):\n\n```gsql\n{gsql_query}\n```\n\nYou can execute this query using the gsql tool."
        return [TextContent(type="text", text=message)]
        
    except Exception as e:
        return [TextContent(type="text", text=f"Failed to generate GSQL query due to: {str(e)}")]


async def generate_cypher(
    query_description: str,
    graph_name: str,
) -> List[TextContent]:
    """Generate an openCypher query from natural language description using LangChain's init_chat_model.
    
    The generated query will be wrapped in TigerGraph's INTERPRET OPENCYPHER QUERY format.
    
    Supports multiple LLM providers through environment variables:
    - LLM_MODEL: Model name, optionally with provider prefix (e.g., 'gpt-4o' or 'openai:gpt-4o')
    - LLM_PROVIDER: Provider name if not specified in LLM_MODEL (e.g., 'openai', 'anthropic', 'bedrock_converse')
    
    Args:
        query_description: Natural language description of the query to generate.
        graph_name: Name of the graph (required for INTERPRET OPENCYPHER QUERY wrapper).
    
    Returns:
        List containing the generated Cypher query wrapped for TigerGraph execution.
    """
    try:
        # Try to import langchain's init_chat_model
        try:
            from langchain.chat_models import init_chat_model
        except ImportError:
            return [TextContent(
                type="text", 
                text="Error: LangChain package is not installed. Please install it with: pip install langchain"
            )]
        
        # Get LLM provider and model configuration from environment
        try:
            provider, model = get_llm_config()
        except ValueError as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
        
        # Initialize the LLM using langchain's init_chat_model
        try:
            llm = init_chat_model(model, model_provider=provider)
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Failed to initialize LLM with provider '{provider}' and model '{model}': {str(e)}\n\n"
                     f"Make sure you have the required dependencies installed and API keys configured.\n"
                     f"Common providers: openai, anthropic, bedrock_converse, azure_openai, google_genai, ollama\n\n"
                     f"Configuration options:\n"
                     f"  - LLM_MODEL=provider:model (e.g., 'openai:gpt-4o')\n"
                     f"  - LLM_MODEL=model + LLM_PROVIDER=provider (e.g., LLM_MODEL='gpt-4o', LLM_PROVIDER='openai')"
            )]
        
        # Get schema for the graph
        schema_section = "## Graph Schema\n\nNo schema information available. Generate a generic Cypher query based on the request."
        try:
            conn = get_connection(graph_name=graph_name)
            schema = await conn.describe_graph()
            if schema:
                schema_section = f"## Graph Schema\n\n{schema}"
        except Exception as e:
            schema_section = f"## Graph Schema\n\nCould not fetch schema: {str(e)}. Generate a generic Cypher query."
        
        # Build the prompt
        prompt = CYPHER_GENERATION_PROMPT.format(schema_section=schema_section)
        
        # Create messages for the chat model
        from langchain_core.messages import SystemMessage, HumanMessage
        messages = [
            SystemMessage(content=prompt),
            HumanMessage(content=query_description)
        ]
        
        # Invoke the LLM
        response = llm.invoke(messages)
        cypher_query = response.content.strip()
        
        # Clean up the response - remove markdown code blocks if present
        if cypher_query.startswith("```cypher"):
            cypher_query = cypher_query[9:]
        elif cypher_query.startswith("```"):
            cypher_query = cypher_query[3:]
        if cypher_query.endswith("```"):
            cypher_query = cypher_query[:-3]
        
        cypher_query = cypher_query.strip()
        
        # Wrap in TigerGraph's INTERPRET OPENCYPHER QUERY format
        wrapped_query = f"USE GRAPH {graph_name}\nINTERPRET OPENCYPHER QUERY () {{\n{cypher_query}\n}}"
        
        message = (
            f"Success: Generated openCypher query (using {provider}:{model}):\n\n"
            f"```cypher\n{wrapped_query}\n```\n\n"
            f"You can execute this query using the gsql tool."
        )
        return [TextContent(type="text", text=message)]
        
    except Exception as e:
        return [TextContent(type="text", text=f"Failed to generate Cypher query due to: {str(e)}")]

