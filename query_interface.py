# query_interface.py

import json
import os
import requests
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()
console = Console()

class Neo4jConnector:
    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")
        
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            self.driver.verify_connectivity()
            console.print("Connected to Neo4j.")
        except Exception as e:
            console.print(f"Failed to connect to Neo4j: {e}")
            raise

    def close(self):
        self.driver.close()

    def get_schema(self):
        with self.driver.session(database="neo4j") as session:
            node_labels_query = "MATCH (n:ENTITY) RETURN DISTINCT n.type AS label"
            relationship_types_query = "MATCH ()-[r:RELATIONSHIP]->() RETURN DISTINCT r.type AS type"
            node_labels = [record["label"] for record in session.run(node_labels_query)]
            relationship_types = [record["type"] for record in session.run(relationship_types_query)]
            
            return {"node_labels": node_labels, "relationship_types": relationship_types}

    def run_query(self, query: str):
        with self.driver.session(database="neo4j") as session:
            try:
                result = session.run(query)
                return [dict(record) for record in result]
            except Exception as e:
                console.print(f"Query failed: {e}")
                return None

# Gemini API translator
class GeminiQueryTranslator:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={self.api_key}"

    def translate(self, schema: dict, question: str):
        json_schema = {
            "type": "OBJECT",
            "properties": {
                "query": {"type": "STRING", "description": "Cypher query to run."},
                "explanation": {"type": "STRING", "description": "Explanation of query."}
            },
            "required": ["query", "explanation"]
        }

        prompt = f"""
        You are an expert Neo4j Cypher query translator. Your task is to convert a user's natural language question into a valid Cypher query based on the provided graph schema.

        **Graph Schema:**
        - Node Labels: {schema['node_labels']}
        - Relationship Types: {schema['relationship_types']}

        **Schema Details:**
        - All nodes have the label 'ENTITY'. The specific type of entity (like 'PERSON' or 'ORGANIZATION') is stored in a property called 'type'.
        - All nodes have an 'id' property which is the name of the entity (e.g., 'Google' or 'Elon Musk').
        - All relationships have the label 'RELATIONSHIP'. The specific type of relationship (like 'EMPLOYER' or 'CEO_OF') is stored in a property called 'type'.

        **User's Question:**
        "{question}"

        Based on the schema and the user's question, generate a Cypher query to find the answer. The query should be compatible with the structured output format requested.
        """

        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": json_schema
            }
        }
        
        try:
            response = requests.post(self.api_url, json=payload, headers={'Content-Type': 'application/json'})
            response.raise_for_status()
            response_json = response.json()
            generated_text = response_json['candidates'][0]['content']['parts'][0]['text']
            return json.loads(generated_text)
        except requests.RequestException as e:
            console.print(f"API Request Failed: {e}")
            console.print("Response body:", response.text)
            return None
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            console.print(f"Failed to parse API response: {e}")
            console.print("Received payload:", response_json)
            return None

def display_results(results: list):
    if not results:
        console.print("No results found")
        return
        
    table = Table(show_header=True, header_style="purple")
    for key in results[0].keys():
        table.add_column(key)
    
    for row in results:
        table.add_row(*(str(value) for value in row.values()))
        
    console.print(table)

if __name__ == "__main__":
    try:
        db_connector = Neo4jConnector()
        query_translator = GeminiQueryTranslator()

        console.print("Fetching graph schema")
        graph_schema = db_connector.get_schema()
        console.print("Schema loaded")
        console.print("Type 'exit' to quit.")

        while True:
            question = console.input("Ask your knowledge graph a question > ")
            if question.lower() == 'exit':
                break

            console.print(f"\nTranslating your question...")
            structured_response = query_translator.translate(graph_schema, question)
            
            if not structured_response or "query" not in structured_response:
                console.print("Could not generate valid query.\n")
                continue
                
            cypher_query = structured_response["query"]
            explanation = structured_response.get("explanation", "No explanation provided.")

            console.print(f"Generated Query: {cypher_query}")
            console.print(f"Explanation: {explanation} \n")
            
            results = db_connector.run_query(cypher_query)
            
            if results is not None:
                display_results(results)
            
            print("-" * 50)

    except (ValueError, Exception) as e:
        console.print(f"\nError: {e}")
    finally:
        if 'db_connector' in locals():
            db_connector.close()
        console.print("\nConnection closed. Spend some time studying")