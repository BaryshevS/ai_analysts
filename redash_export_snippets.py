import os
import json
import requests
import csv
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_redash_query_snippets():
    # Read the Redash API token from environment variables
    redash_token = os.getenv('REDASH_TOKEN')
    redash_host = os.getenv('REDASH_HOST', 'https://superanalytics.ru')  # Default to superanalytics.ru if not set

    if not redash_token:
        raise ValueError("REDASH_TOKEN environment variable is not set")

    # Redash API endpoint for querying snippets
    url = f"{redash_host}/api/query_snippets"

    headers = {
        'Authorization': f'Key {redash_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Print the response structure for debugging
        print(f"API Response Type: {type(data)}")
        print(f"API Response Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dictionary'}")
        print(f"First few items: {data[:3] if isinstance(data, list) and len(data) > 0 else data}")

        # Convert to list of dictionaries for easier handling
        snippets_list = []
        # Check if data is a list (direct response) or dict (with results key)
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and 'results' in data:
            items = data.get('results', [])
        else:
            items = []

        for item in items:
            snippet = {
                'id': item.get('id') if isinstance(item, dict) else None,
                'trigger': item.get('trigger') if isinstance(item, dict) else None,
                'description': item.get('description') if isinstance(item, dict) else None,
                'snippet': item.get('snippet') if isinstance(item, dict) else str(item),
                'created_at': item.get('created_at') if isinstance(item, dict) else None
            }
            snippets_list.append(snippet)

        return snippets_list
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Redash API: {e}")
        return []

# Main execution
if __name__ == "__main__":
    # Get query snippets from Redash
    snippets = get_redash_query_snippets()

    # Save to JSON file
    with open('redash_snippets.json', 'w', encoding='utf-8') as f:
        json.dump(snippets, f, ensure_ascii=False, indent=2)

    print(f"Successfully saved {len(snippets)} query snippets to redash_snippets.json")

    # Also update the CSV file with the current time
    # For now, just save the JSON version
    with open('redash_snippets.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Trigger', 'Description', 'Snippet', '', 'Created At']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for snippet in snippets:
            writer.writerow({
                'Trigger': snippet['trigger'],
                'Description': snippet['description'],
                'Snippet': snippet['snippet'],
                '': '',
                'Created At': snippet['created_at']
            })

    print("Updated both JSON and CSV files")