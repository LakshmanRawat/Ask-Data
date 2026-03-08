# DuckDB + Ollama + MCP Agent

An AI-powered data querying agent that combines:
- **Ollama**: For natural language understanding and SQL generation
- **MCP (Model Context Protocol)**: For tool integration
- **DuckDB**: For fast analytical database queries

## Features

- 🗣️ Natural language queries - Ask questions in plain English
- 🤖 AI-powered SQL generation - Automatically converts questions to SQL
- 📊 Intelligent result explanation - Get human-readable explanations of query results
- 🔍 Schema introspection - Automatically discovers database structure

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Ensure Ollama is running with a model:**
   ```bash
   # Make sure Ollama is installed and running
   ollama pull llama3  # or your preferred model
   ```

3. **Create the database (if not already created):**
   ```bash
   python create_db.py
   ```

## Usage

### Interactive Chat Interface

Run the interactive chat interface to query your database:

```bash
python chat.py
```

Example queries:
- "Show me all orders"
- "What is the total amount for Alice?"
- "How many orders are there?"
- "Show me orders from March 2024"

To see the generated SQL queries, run with `--show-sql`:
```bash
python chat.py --show-sql
```

### Programmatic Usage

You can also use the agent programmatically:

```python
import asyncio
from agent import DuckDBAgent

async def main():
    agent = DuckDBAgent()
    
    try:
        await agent.initialize()
        
        # Query the database
        result = await agent.query("What is the total revenue?")
        
        print(f"SQL: {result['sql']}")
        print(f"Results: {result['results']}")
        print(f"Explanation: {result['explanation']}")
        
    finally:
        await agent.close()

asyncio.run(main())
```

## Architecture

### Core Files

- **`server.py`**: MCP server that exposes DuckDB tools (run_sql, list_tables, get_table_schema)
- **`agent.py`**: Main agent class that integrates Ollama with MCP client
- **`chat.py`**: Interactive command-line interface
- **`create_db.py`**: Database initialization script (run once to set up)
- **`requirements.txt`**: Python dependencies
- **`data.duckdb`**: DuckDB database file (created by create_db.py)

## How It Works (Simple Explanation)

Think of it like asking a smart assistant to look something up:

1. **You ask a question** in plain English: "Show me all orders"
2. **The agent checks** what tables exist in the database
3. **AI translates** your question into SQL (database language): `SELECT * FROM orders`
4. **The agent runs** the SQL query on the database
5. **AI explains** the results back to you in plain English
6. **You get** a friendly, conversational answer

**Example:**
```
You: "What is the total amount for Alice?"
🤖: "Alice has a total of $350 across her 2 orders"
```

> 📖 **Want more details?** See [HOW_IT_WORKS.md](HOW_IT_WORKS.md) for a complete simple explanation with analogies and examples.

## Configuration

You can customize the Ollama model by modifying the `DuckDBAgent` initialization:

```python
agent = DuckDBAgent(model="llama3.2")  # Use a different model
```
