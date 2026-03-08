#!/usr/bin/env python3
"""
Interactive chat interface for the DuckDB + Ollama agent.
Run this script to have a conversational interface with your database.
"""

import asyncio
import sys
from agent import DuckDBAgent


async def interactive_chat():
    """Interactive chat loop"""
    agent = DuckDBAgent()
    
    try:
        print("Initializing DuckDB agent...")
        await agent.initialize()
        print("✓ Agent ready! Type your questions about the data.")
        print("Type 'exit' or 'quit' to end the conversation.\n")
        
        while True:
            try:
                # Get user input
                user_query = input("\nYou: ").strip()
                
                if user_query.lower() in ['exit', 'quit', 'q']:
                    print("\nGoodbye!")
                    break
                
                if not user_query:
                    continue
                
                # Process query
                print("\nThinking...")
                result = await agent.query(user_query, explain=True)
                
                if "error" in result:
                    print(f"\n❌ Error: {result['error']}")
                    if result.get('sql'):
                        print(f"Generated SQL: {result['sql']}")
                else:
                    print(f"\n🤖 Agent:")
                    if result.get('explanation'):
                        print(result['explanation'])
                    else:
                        print(result.get('results', 'No results'))
                    
                    # Optionally show SQL (can be toggled)
                    if '--show-sql' in sys.argv:
                        print(f"\n📝 SQL: {result.get('sql')}")
                
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"\n❌ Unexpected error: {str(e)}")
                
    finally:
        await agent.close()


if __name__ == "__main__":
    asyncio.run(interactive_chat())
