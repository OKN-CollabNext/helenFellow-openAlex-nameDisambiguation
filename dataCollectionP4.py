import csv

# List to hold data for DataFrame
rows = []

# Pagination
cursor = '*'
start_entry = 1000  # Start from the 1000th entry
end_entry = 2000    # Go up to the 2000th entry
current_count = 0

while cursor and current_count < end_entry:
    url = f"{base_url}?{params}&cursor={cursor}&api_key={my_api_key}"
    print(f"Requesting data with URL: {url}")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        break

    data = response.json()
    results = data.get("results", [])

    for author in results:
        if current_count < start_entry:  # Skip entries before the 1000th
            current_count += 1
            continue
        
        if current_count >= end_entry:
            break

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

        # Request author's works to get coauthors
        coauthors_str = "N/A"
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

        rows.append({
            'Name': name,
            'ORCID': orcid,
            'Institutions': institutions_str,
            'Concepts': concepts_str,
            'Coauthors': coauthors_str
        })
        current_count += 1

    cursor = data.get("meta", {}).get("next_cursor")  # Set cursor for next request if available

# Write the collected data to a CSV file
with open('authors_data.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['Name', 'ORCID', 'Institutions', 'Concepts', 'Coauthors'])
    writer.writeheader()  # Write the header row
    writer.writerows(rows)  # Write all the collected rows

print("Data saved to 'authors_data.csv'")
