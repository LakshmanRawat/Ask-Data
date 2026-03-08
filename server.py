from mcp.server.fastmcp import FastMCP
import duckdb
import json
from datetime import date, datetime, time
from decimal import Decimal

mcp = FastMCP("duckdb-agent")

DB_PATH = "data.duckdb"


def serialize_value(value):
    """Convert non-JSON-serializable values to strings"""
    if isinstance(value, (date, datetime)):
        return str(value)
    elif isinstance(value, time):
        return str(value)
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, (bytes, bytearray)):
        return value.decode('utf-8', errors='replace')
    else:
        return value


@mcp.tool()
def run_sql(query: str) -> str:
    """
    Run SQL query on DuckDB and return results as JSON string.
    
    Args:
        query: SQL query to execute
        
    Returns:
        JSON string with query results
    """
    try:
        conn = duckdb.connect(DB_PATH)
        
        # Execute query
        result = conn.execute(query).fetchall()
        
        # Get column names
        columns = [desc[0] for desc in conn.description] if conn.description else []
        
        conn.close()
        
        # Format results as list of dictionaries, serializing non-JSON types
        if columns:
            formatted_results = [
                {col: serialize_value(val) for col, val in zip(columns, row)}
                for row in result
            ]
        else:
            formatted_results = [
                [serialize_value(val) for val in row]
                for row in result
            ]
        
        return json.dumps({
            "success": True,
            "columns": columns,
            "data": formatted_results,
            "row_count": len(result)
        })
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })


@mcp.tool()
def list_tables() -> str:
    """
    List all tables in the database.
    
    Returns:
        JSON string with list of table names
    """
    try:
        conn = duckdb.connect(DB_PATH)
        
        result = conn.execute("SHOW TABLES").fetchall()
        
        conn.close()
        
        # Extract table names
        tables = [row[0] if isinstance(row, (list, tuple)) else row for row in result]
        
        return json.dumps({
            "success": True,
            "tables": tables
        })
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })


@mcp.tool()
def get_table_schema(table_name: str) -> str:
    """
    Get the schema (column names and types) for a specific table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        JSON string with table schema information
    """
    try:
        conn = duckdb.connect(DB_PATH)
        
        # Get table schema
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        
        conn.close()
        
        # Format schema
        schema = [{"column": row[0], "type": row[1], "null": row[2], "default": row[3], "key": row[4]} 
                  for row in result]
        
        return json.dumps({
            "success": True,
            "table": table_name,
            "schema": schema
        })
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })


if __name__ == "__main__":
    mcp.run()