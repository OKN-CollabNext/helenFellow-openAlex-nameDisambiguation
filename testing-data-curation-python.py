# %%
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
#params = "per-page=200&select=display_name,orcid,works_count,affiliations,x_concepts"

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
while cursor and len(rows) < 1:
    url = f"{base_url}?{params}&cursor={cursor}&api_key={my_api_key}"
    print(f"Requesting data with URL: {url}")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    results = data.get("results", [])

    for author in results:
        author_id = author.get("id")
        name = author.get("display_name", "Unknown")
        # print(f"Author ID for {name}: {author_id}")
        orcid = author.get("orcid")
        orcid = orcid.split("/")[-1] if orcid else "N/A"

        # Extract institutions
        institutions = [affiliation['institution']['display_name'] for affiliation in author.get("affiliations", [])]
        institutions_str = ", ".join(set(institutions)) if institutions else "N/A"

        # Extract concepts
        concepts = [concept['display_name'] for concept in author.get("x_concepts", [])]
        concepts_str = ", ".join(set(concepts)) if concepts else "N/A"

        # Request author's works to get coauthors
        if author_id:
            works_url = f"https://api.openalex.org/works?filter=author.id:{author_id}&per-page=200&api_key={my_api_key}"
            print(f"Fetching works for author {name} from URL: {works_url}")
            works_response = requests.get(works_url, headers=headers)
            if works_response.status_code == 200:
                works_data = works_response.json()
                # print(f"Works data fetched for {name}: {works_data}")
                coauthors = set()
                for work in works_data.get("results", []):
                    authorships = work.get("authorships", [])
                    # print(f"Authorships in work: {authorships}")
                    for authorship in authorships:
                        coauthor_name = authorship.get("author", {}).get("display_name")
                        if coauthor_name and coauthor_name != name:
                            coauthors.add(coauthor_name)
                            # print(f"Found coauthor: {coauthor_name}")
                coauthors_str = ", ".join(coauthors) if coauthors else "N/A"
                print(f"Coauthors for {name}: {coauthors_str}")
            else:
                print(f"Error fetching works: {works_response.status_code} - {works_response.text}")
                coauthors_str = "N/A"
        else:
            coauthors_str = "N/A"

        rows.append({
            'Name': name,
            'Orcid': orcid,
            'Institutions': institutions_str,
            'Concepts': concepts_str,
            'Coauthors': coauthors_str
        })

    cursor = data['meta'].get('next_cursor', False)



# %%
# Create DataFrame
df = pd.DataFrame(rows)

# %%
df

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

authors_with_orcid.to_csv('authors_with_orcid.csv', index=False)
authors_without_orcid.to_csv('authors_without_orcid.csv', index=False)

# %%
authors_with_orcid

# %%
authors_without_orcid


