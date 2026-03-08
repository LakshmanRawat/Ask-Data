import asyncio
import ollama
from mcp import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters
import json


class DuckDBAgent:
    """
    An AI agent that uses Ollama for natural language understanding
    and MCP to query DuckDB database.
    """
    
    def __init__(self, model="llama3", db_server_command="python3", db_server_args=None):
        self.model = model
        self.db_server_command = db_server_command
        self.db_server_args = db_server_args or ["server.py"]
        self.session = None
        self.read = None
        self.write = None
        
    async def initialize(self):
        """Initialize the MCP client connection"""
        server_params = StdioServerParameters(
            command=self.db_server_command,
            args=self.db_server_args
        )
        self.stdio_context = stdio_client(server_params)
        self.read, self.write = await self.stdio_context.__aenter__()
        self.session_context = ClientSession(self.read, self.write)
        self.session = await self.session_context.__aenter__()
        await self.session.initialize()
        
    async def close(self):
        """Close the MCP client connection"""
        if hasattr(self, 'session_context') and self.session_context:
            await self.session_context.__aexit__(None, None, None)
        if hasattr(self, 'stdio_context') and self.stdio_context:
            await self.stdio_context.__aexit__(None, None, None)
    
    async def get_schema_info(self):
        """Get database schema information to help with SQL generation"""
        try:
            # Get list of tables
            tables_result = await self.session.call_tool("list_tables", {})
            
            # Parse JSON response
            if hasattr(tables_result, 'content') and tables_result.content:
                result_text = tables_result.content[0].text
                result_data = json.loads(result_text)
                tables = result_data.get('tables', [])
            else:
                tables = []
            
            schema_info = f"Available tables: {', '.join(tables) if tables else 'None'}\n"
            
            return schema_info
        except Exception as e:
            return f"Could not fetch schema: {str(e)}"
    
    def generate_sql(self, user_query: str, schema_info: str = "") -> str:
        """
        Use Ollama to convert natural language query to SQL
        """
        system_prompt = """You are a SQL expert. Convert the user's natural language query into a valid SQL query for DuckDB.

Rules:
1. Only return the SQL query, nothing else
2. Use proper SQL syntax for DuckDB
3. If the query is ambiguous, make reasonable assumptions
4. For date queries, use DATE format: 'YYYY-MM-DD'
5. Return only the SQL query without markdown formatting or explanations

Available schema information:
{schema_info}

Example:
User: "Show me all orders"
SQL: SELECT * FROM orders

User: "What is the total amount for Alice?"
SQL: SELECT SUM(amount) as total FROM orders WHERE customer = 'Alice'
"""
        
        prompt = system_prompt.format(schema_info=schema_info)
        
        try:
            response = ollama.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user_query}
                ]
            )
            
            sql = response["message"]["content"].strip()
            
            # Clean up SQL if it's wrapped in markdown code blocks
            if sql.startswith("```"):
                lines = sql.split("\n")
                sql = "\n".join(lines[1:-1]) if len(lines) > 2 else sql
            if sql.startswith("```sql"):
                lines = sql.split("\n")
                sql = "\n".join(lines[1:-1]) if len(lines) > 2 else sql
            
            return sql.strip()
        except Exception as e:
            raise Exception(f"Error generating SQL: {str(e)}")
    
    def explain_results(self, query: str, results: str) -> str:
        """
        Use Ollama to explain query results in natural language
        """
        prompt = f"""The user asked: "{query}"

The database returned these results:
{results}

Provide a clear, concise explanation of these results in natural language. Be conversational and helpful."""
        
        try:
            response = ollama.chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}]
            )
            return response["message"]["content"]
        except Exception as e:
            return f"Results: {results}\n(Note: Could not generate explanation: {str(e)})"
    
    async def query(self, user_query: str, explain: bool = True) -> dict:
        """
        Main method to process a natural language query
        
        Args:
            user_query: Natural language question about the data
            explain: Whether to generate a natural language explanation of results
            
        Returns:
            Dictionary with SQL query, results, and explanation
        """
        try:
            # Get schema information to help with SQL generation
            schema_info = await self.get_schema_info()
            
            # Generate SQL from natural language
            sql_query = self.generate_sql(user_query, schema_info)
            
            # Execute SQL via MCP
            result = await self.session.call_tool("run_sql", {"query": sql_query})
            
            # Parse JSON response
            if hasattr(result, 'content') and result.content:
                result_text = result.content[0].text
                try:
                    result_data = json.loads(result_text)
                    if result_data.get('success'):
                        # Format results nicely
                        data = result_data.get('data', [])
                        columns = result_data.get('columns', [])
                        row_count = result_data.get('row_count', 0)
                        
                        if columns and data:
                            # Create a formatted table-like output
                            results_text = f"Found {row_count} row(s):\n"
                            results_text += json.dumps(data, indent=2)
                        else:
                            results_text = json.dumps(data, indent=2) if data else "No rows returned"
                    else:
                        results_text = f"Error: {result_data.get('error', 'Unknown error')}"
                except json.JSONDecodeError:
                    results_text = result_text
            else:
                results_text = str(result)
            
            # Generate explanation if requested
            explanation = None
            if explain:
                explanation = self.explain_results(user_query, results_text)
            
            return {
                "query": user_query,
                "sql": sql_query,
                "results": results_text,
                "explanation": explanation
            }
            
        except Exception as e:
            return {
                "query": user_query,
                "error": str(e),
                "sql": sql_query if 'sql_query' in locals() else None
            }
    
    async def chat(self, user_query: str) -> str:
        """
        Simplified chat interface that returns just the explanation
        """
        result = await self.query(user_query, explain=True)
        
        if "error" in result:
            return f"Sorry, I encountered an error: {result['error']}"
        
        return result.get("explanation", result.get("results", "No results"))


async def main():
    """Example usage"""
    agent = DuckDBAgent()
    
    try:
        await agent.initialize()
        
        # Example queries
        queries = [
            "Show me all orders",
            "What is the total amount for Alice?",
            "How many orders are there?",
        ]
        
        for query in queries:
            print(f"\n{'='*60}")
            print(f"Query: {query}")
            print(f"{'='*60}")
            
            result = await agent.query(query)
            
            print(f"\nGenerated SQL: {result.get('sql')}")
            print(f"\nResults: {result.get('results')}")
            if result.get('explanation'):
                print(f"\nExplanation: {result.get('explanation')}")
            
    finally:
        await agent.close()


if __name__ == "__main__":
    asyncio.run(main())
