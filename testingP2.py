import requests
import pandas as pd
import os
from dotenv import load_dotenv

# %% [markdown]
# # Data Ingestion and Processing

# %% [markdown]
# ### Set up API

# %%
# Load environment variables
load_dotenv('.env')
my_api_key = os.getenv('API_KEY')
if my_api_key is None:
    print("No API key found!!!")
    exit()
else:
    print("API key is set!")

# %% [markdown]
# ### Get Data

# %%
# Base URL for OpenAlex API authors endpoint
base_url = "https://api.openalex.org/authors"

# %%
# Parameters for pagination and selecting data
params = "per-page=200&select=id,display_name,orcid,works_count,affiliations,x_concepts"

# %%
# Headers with User-Agent
headers = {"User-Agent": "MyApp/1.0 (rlee379@gatech.edu)"}

# %%
# List to hold data for DataFrame
rows = []

# %%
# Pagination
cursor = '*'
while cursor:
    # Construct the URL with the current cursor for pagination
    url = f"{base_url}?{params}&cursor={cursor}&api_key={my_api_key}"
    print(f"Requesting data with URL: {url}")
    
    # Send the GET request to the API
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break

    # Parse the JSON data from the response
    data = response.json()
    
    # If no results, break the loop
    if 'results' not in data or not data['results']:
        print("No results found or error in fetching data.")
        break

    results = data['results']

    # Process each author in the current batch of results
    for author in results:
        author_id = author.get("id")
        name = author.get("display_name", "Unknown")
        orcid = author.get("orcid")
        orcid = orcid.split("/")[-1] if orcid else "N/A"

        # Extract institutions
        institutions = [affiliation['institution']['display_name'] for affiliation in author.get("affiliations", [])]
        institutions_str = ", ".join(set(institutions)) if institutions else "N/A"

        # Extract concepts
        concepts = [concept['display_name'] for concept in author.get("x_concepts", [])]
        concepts_str = ", ".join(set(concepts)) if concepts else "N/A"

        # Fetch works for the author and their coauthors
        if author_id:
            works_url = f"https://api.openalex.org/works?filter=author.id:{author_id}&per-page=200&api_key={my_api_key}"
            works_response = requests.get(works_url, headers=headers)
            if works_response.status_code == 200:
                works_data = works_response.json()
                coauthors = set()
                for work in works_data.get("results", []):
                    authorships = work.get("authorships", [])
                    for authorship in authorships:
                        coauthor_name = authorship.get("author", {}).get("display_name")
                        if coauthor_name and coauthor_name != name:
                            coauthors.add(coauthor_name)
                coauthors_str = ", ".join(coauthors) if coauthors else "N/A"
            else:
                coauthors_str = "N/A"
        else:
            coauthors_str = "N/A"

        # Add the processed author data to the rows list
        rows.append({
            'Name': name,
            'Orcid': orcid,
            'Institutions': institutions_str,
            'Concepts': concepts_str,
            'Coauthors': coauthors_str
        })

    # Move to the next page if a next_cursor exists; if not, stop the loop
    cursor = data['meta'].get('next_cursor', None)
    if not cursor:
        print("No next cursor. Ending pagination.")
        break

# %% [markdown]
# ### Create DataFrame

# %%
# Create DataFrame from the rows list
df = pd.DataFrame(rows)

# %%
# Save DataFrame to CSV
df.to_csv('all_authors_data.csv', index=False)

# %% [markdown]
# ### Refine Dataset

# %% [markdown]
# #### Split into known and unknown ORCID

# %%
# Refine Dataset
authors_with_orcid = df[df['Orcid'].notna() & (df['Orcid'] != "N/A")]
authors_without_orcid = df[df['Orcid'].isna() | (df['Orcid'] == "N/A")]

# Save to CSV
authors_with_orcid.to_csv('authors_with_orcid.csv', index=False)
authors_without_orcid.to_csv('authors_without_orcid.csv', index=False)

# %% [markdown]
# ### Display the refined datasets

# %%
authors_with_orcid

# %%
authors_without_orcid
