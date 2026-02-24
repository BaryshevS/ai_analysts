import os
import json
import requests
import csv
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_redash_queries():
    # Read the Redash API token from environment variables
    redash_token = os.getenv('REDASH_TOKEN')
    redash_host = os.getenv('REDASH_HOST', 'https://superanalytics.ru')  # Default to superanalytics.ru if not set

    if not redash_token:
        raise ValueError("REDASH_TOKEN environment variable is not set")

    # Redash API endpoint for querying queries
    url = f"{redash_host}/api/queries"

    headers = {
        'Authorization': f'Key {redash_token}',
        'Content-Type': 'application/json'
    }

    all_queries = []
    page = 1
    page_size = 50  # Number of items per page

    try:
        while True:
            # Add pagination parameters to the URL
            paginated_url = f"{url}?page={page}&page_size={page_size}"

            response = requests.get(paginated_url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Extract queries from the response
            if isinstance(data, dict) and 'results' in data:
                queries = data.get('results', [])
                count = data.get('count', 0)
                page_size_response = data.get('page_size', page_size)

                # Add queries to our collection
                for query in queries:
                    query_data = {
                        'id': query.get('id'),
                        'name': query.get('name'),
                        'description': query.get('description', ''),
                        'query': query.get('query', ''),
                        'data_source_id': query.get('data_source_id'),
                        'created_at': query.get('created_at'),
                        'updated_at': query.get('updated_at'),
                        'is_archived': query.get('is_archived', False),
                        'is_draft': query.get('is_draft', False)
                    }
                    all_queries.append(query_data)

                # Check if we have more pages
                total_pages = (count + page_size_response - 1) // page_size_response
                if page >= total_pages:
                    break

                page += 1
            else:
                # If we don't get the expected structure, break the loop
                break

        return all_queries
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Redash API: {e}")
        return []

# Main execution
if __name__ == "__main__":
    # Get queries from Redash
    queries = get_redash_queries()

    # Save to JSON file
    with open('redash_queries.json', 'w', encoding='utf-8') as f:
        json.dump(queries, f, ensure_ascii=False, indent=2)

    print(f"Successfully saved {len(queries)} queries to redash_queries.json")

    # Also save to CSV file
    with open('redash_queries.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['ID', 'Name', 'Description', 'Query', 'Data Source ID', 'Created At', 'Updated At', 'Is Archived', 'Is Draft']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for query in queries:
            writer.writerow({
                'ID': query['id'],
                'Name': query['name'],
                'Description': query['description'],
                'Query': query['query'][:1000] + '...' if len(query['query']) > 1000 else query['query'],  # Truncate long queries
                'Data Source ID': query['data_source_id'],
                'Created At': query['created_at'],
                'Updated At': query['updated_at'],
                'Is Archived': query['is_archived'],
                'Is Draft': query['is_draft']
            })

    print("Updated both JSON and CSV files")