# Copyright 2025 TigerGraph Inc.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file or https://www.apache.org/licenses/LICENSE-2.0
#
# Permission is granted to use, copy, modify, and distribute this software
# under the License. The software is provided "AS IS", without warranty.

"""Structured response formatting for MCP tools.

This module provides utilities for creating consistent, LLM-friendly responses
from MCP tools. It ensures responses are both machine-readable and human-friendly.
"""

import json
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel
from mcp.types import TextContent


class ToolResponse(BaseModel):
    """Structured response format for all MCP tools.
    
    This format provides:
    - Clear success/failure indication
    - Structured data for parsing
    - Human-readable summary
    - Contextual suggestions for next steps
    - Rich metadata
    """
    success: bool
    operation: str
    data: Optional[Dict[str, Any]] = None
    summary: str
    metadata: Optional[Dict[str, Any]] = None
    suggestions: Optional[List[str]] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    timestamp: str = None

    def __init__(self, **data):
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        super().__init__(**data)


def format_response(
    success: bool,
    operation: str,
    summary: str,
    data: Optional[Dict[str, Any]] = None,
    suggestions: Optional[List[str]] = None,
    error: Optional[str] = None,
    error_code: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[TextContent]:
    """Create a structured response for MCP tools.
    
    Args:
        success: Whether the operation succeeded
        operation: Name of the operation (tool name without prefix)
        summary: Human-readable summary message
        data: Structured result data
        suggestions: List of suggested next steps or actions
        error: Error message if success=False
        error_code: Optional error code for categorization
        metadata: Additional context (graph_name, timing, etc.)
    
    Returns:
        List of TextContent with both JSON and formatted text
    
    Example:
        >>> format_response(
        ...     success=True,
        ...     operation="add_node",
        ...     summary="Node added successfully",
        ...     data={"vertex_id": "user1", "vertex_type": "Person"},
        ...     suggestions=["Use 'get_node' to verify", "Use 'add_edge' to connect"]
        ... )
    """
    
    response = ToolResponse(
        success=success,
        operation=operation,
        summary=summary,
        data=data,
        suggestions=suggestions,
        error=error,
        error_code=error_code,
        metadata=metadata
    )
    
    # Create structured JSON output
    json_output = response.model_dump_json(indent=2, exclude_none=True)
    
    # Create human-readable format
    text_parts = [f"**{summary}**"]
    
    # Add data section
    if data:
        text_parts.append(f"\n**Data:**\n```json\n{json.dumps(data, indent=2, default=str)}\n```")
    
    # Add suggestions
    if suggestions and len(suggestions) > 0:
        text_parts.append("\n**💡 Suggestions:**")
        for i, suggestion in enumerate(suggestions, 1):
            text_parts.append(f"{i}. {suggestion}")
    
    # Add error details
    if error:
        text_parts.append(f"\n**❌ Error Details:**\n{error}")
        if error_code:
            text_parts.append(f"\n**Error Code:** {error_code}")
    
    # Add metadata footer
    if metadata:
        text_parts.append(f"\n**Metadata:** {json.dumps(metadata, default=str)}")
    
    text_output = "\n".join(text_parts)
    
    # Combine both formats
    full_output = f"```json\n{json_output}\n```\n\n{text_output}"
    
    return [TextContent(type="text", text=full_output)]


def format_success(
    operation: str,
    summary: str,
    data: Optional[Dict[str, Any]] = None,
    suggestions: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[TextContent]:
    """Convenience method for successful operations."""
    return format_response(
        success=True,
        operation=operation,
        summary=summary,
        data=data,
        suggestions=suggestions,
        metadata=metadata
    )


def format_error(
    operation: str,
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
    suggestions: Optional[List[str]] = None,
) -> List[TextContent]:
    """Format an error response with contextual recovery hints.
    
    Args:
        operation: Name of the failed operation
        error: The exception that occurred
        context: Context information (parameters, state, etc.)
        suggestions: Optional manual suggestions (auto-generated if not provided)
    
    Returns:
        Formatted error response with recovery hints
    """
    
    error_str = str(error)
    error_lower = error_str.lower()
    
    # Auto-generate suggestions based on error type if not provided
    if suggestions is None:
        suggestions = []
        
        # Schema/type errors
        if any(term in error_lower for term in ["vertex type", "edge type", "type not found"]):
            suggestions.extend([
                "The specified type may not exist in the schema",
                "Call 'show_graph_details' to see available vertex and edge types",
                "Call 'list_graphs' to ensure you're using the correct graph"
            ])
        
        # Attribute errors
        elif any(term in error_lower for term in ["attribute", "column", "field"]):
            suggestions.extend([
                "One or more attributes may not match the schema definition",
                "Call 'show_graph_details' to see required attributes and their types",
                "Check that attribute names are spelled correctly"
            ])
        
        # Connection errors
        elif any(term in error_lower for term in ["connection", "timeout", "unreachable"]):
            suggestions.extend([
                "Unable to connect to TigerGraph server",
                "Verify TG_HOST environment variable is correct",
                "Check network connectivity and firewall settings",
                "Ensure TigerGraph server is running"
            ])
        
        # Authentication errors
        elif any(term in error_lower for term in ["auth", "token", "permission", "forbidden"]):
            suggestions.extend([
                "Authentication failed - check credentials",
                "Verify TG_USERNAME and TG_PASSWORD environment variables",
                "For TigerGraph Cloud, ensure TG_API_TOKEN is set",
                "Check if user has required permissions for this operation"
            ])
        
        # Query errors
        elif any(term in error_lower for term in ["syntax", "parse", "query"]):
            suggestions.extend([
                "Query syntax error detected",
                "For GSQL: Use 'INTERPRET QUERY () FOR GRAPH <name> { ... }'",
                "For Cypher: Use 'INTERPRET OPENCYPHER QUERY () FOR GRAPH <name> { ... }'",
                "Call 'show_graph_details' to understand the schema before writing queries"
            ])
        
        # Vector errors
        elif any(term in error_lower for term in ["vector", "dimension", "embedding"]):
            suggestions.extend([
                "Vector operation error",
                "Ensure vector dimensions match the attribute definition",
                "Call 'get_vector_index_status' to check if index is ready",
                "Verify vector attribute exists with 'show_graph_details'"
            ])
        
        # Generic suggestions
        if len(suggestions) == 0:
            suggestions.extend([
                "Check the error message for specific details",
                "Call 'show_graph_details' to understand the current graph structure",
                "Verify all required parameters are provided correctly"
            ])
    
    # Determine error code
    error_code = None
    if "connection" in error_lower or "timeout" in error_lower:
        error_code = "CONNECTION_ERROR"
    elif "auth" in error_lower or "permission" in error_lower:
        error_code = "AUTHENTICATION_ERROR"
    elif "type" in error_lower:
        error_code = "SCHEMA_ERROR"
    elif "attribute" in error_lower:
        error_code = "ATTRIBUTE_ERROR"
    elif "syntax" in error_lower or "parse" in error_lower:
        error_code = "SYNTAX_ERROR"
    else:
        error_code = "OPERATION_ERROR"
    
    return format_response(
        success=False,
        operation=operation,
        summary=f"❌ Failed to {operation.replace('_', ' ')}",
        error=error_str,
        error_code=error_code,
        metadata=context,
        suggestions=suggestions
    )


def gsql_has_error(result_str: str) -> bool:
    """Check whether a GSQL result string indicates a failure.

    ``conn.gsql()`` does **not** raise an exception when a GSQL command fails;
    instead, the error message is returned as a plain string.  This helper
    inspects the result for well-known error patterns so callers can
    distinguish success from failure.
    """
    error_patterns = [
        "Encountered \"",
        "SEMANTIC ERROR",
        "Syntax Error",
        "Failed to create",
        "does not exist",
        "is not a valid",
        "already exists",
        "Invalid syntax",
    ]
    return any(p in result_str for p in error_patterns)


def format_list_response(
    operation: str,
    items: List[Any],
    item_type: str = "items",
    summary_template: Optional[str] = None,
    suggestions: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[TextContent]:
    """Format a response containing a list of items.
    
    Args:
        operation: Name of the operation
        items: List of items to return
        item_type: Type of items (for summary message)
        summary_template: Optional custom summary (use {count} and {type} placeholders)
        suggestions: Optional suggestions
        metadata: Optional metadata
    
    Returns:
        Formatted response
    """
    
    count = len(items)
    
    if summary_template:
        summary = summary_template.format(count=count, type=item_type)
    else:
        summary = f"✅ Found {count} {item_type}"
    
    return format_success(
        operation=operation,
        summary=summary,
        data={
            "count": count,
            item_type: items
        },
        suggestions=suggestions,
        metadata=metadata
    )
