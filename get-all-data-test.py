import requests
import pandas as pd
import os
import time
import concurrent.futures
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')
my_api_key = os.getenv('API_KEY')
if my_api_key is None:
    print("No API key found!!!")
    exit()

# Base URL
base_url = "https://api.openalex.org/authors"
params = "per-page=200&select=id,display_name,orcid,works_count,affiliations,x_concepts"
headers = {"User-Agent": "MyApp/1.0 (rlee379@gatech.edu)"}

# Function to get co-authors
def get_coauthors(author_id, name):
    works_url = f"https://api.openalex.org/works?filter=author.id:{author_id}&per-page=200&api_key={my_api_key}"
    coauthors = set()

    try:
        response = requests.get(works_url, headers=headers)
        if response.status_code == 200:
            works_data = response.json()
            for work in works_data.get("results", []):
                for authorship in work.get("authorships", []):
                    coauthor_name = authorship.get("author", {}).get("display_name")
                    if coauthor_name and coauthor_name != name:
                        coauthors.add(coauthor_name)
        else:
            print(f"Error fetching coauthors for {name} ({author_id}): {response.status_code}")
    except Exception as e:
        print(f"Exception while fetching coauthors for {name}: {e}")

    return ", ".join(coauthors) if coauthors else "N/A"

# Pagination
rows = []
cursor = '*'

while cursor:
    url = f"{base_url}?{params}&cursor={cursor}&api_key={my_api_key}"
    response = requests.get(url, headers=headers)

    if response.status_code == 429:
        print("Rate limited. Sleeping for 60 seconds.")
        time.sleep(60)
        continue

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    results = data.get("results", [])

    # Use multithreading to fetch co-authors faster
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for author in results:
            author_id = author.get("id")
            name = author.get("display_name", "Unknown")

            # Handle ORCID safely
            orcid = author.get("orcid")
            orcid = orcid.split("/")[-1] if orcid else "N/A"

            # Extract institutions and concepts
            institutions = ", ".join(set([affiliation['institution']['display_name'] for affiliation in author.get("affiliations", []) if 'institution' in affiliation]))
            concepts = ", ".join(set([concept['display_name'] for concept in author.get("x_concepts", [])]))

            # Submit coauthor fetching tasks
            futures.append(executor.submit(get_coauthors, author_id, name))

            # Store data while awaiting coauthor results
            rows.append({
                'Name': name,
                'Orcid': orcid,
                'Institutions': institutions,
                'Concepts': concepts,
                'Coauthors': None  # Placeholder for coauthors, will be updated
            })

        # Collect coauthor results
        for i, future in enumerate(futures):
            rows[i]['Coauthors'] = future.result()

    # Get next cursor
    cursor = data['meta'].get('next_cursor', None)
    if not cursor:
        break

# Save data
print(f"Retrieved {len(rows)} authors")
df = pd.DataFrame(rows)
df.to_csv('all_authors_data.csv', index=False)
print("Data saved successfully.")
