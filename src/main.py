from pathlib import Path
import requests
import pandas as pd

def retrieve_311_tickets(): 
    api_base_url = "https://phl.carto.com/api/v2/sql"
    query = """
    SELECT * 
    FROM public_cases_fc
    WHERE requested_datetime >= '2025-01-01' AND
    requested_datetime < '2026-01-01' AND
    agency_responsible = 'License & Inspections'
    """

    query_parameters = {"format": "csv",
                        "skipfields": "cartodb_id,the_geom,the_geom_webmercator",
                        "q": query}


    response = requests.get(api_base_url, params=query_parameters, stream=True)

    script_location = Path(__file__).resolve().parent
    data_file_path = script_location.parent / "data" / "public_cases_fc_2025.csv"
    with open(data_file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return None

def find_opa_account_nums():
    script_location = Path(__file__).resolve().parent
    data_file_path = script_location.parent / "data" / "public_cases_fc_2025.csv"
    new_data_file_path = script_location.parent / "data" / "public_cases_fc_2025_opa.csv"

    try:
        df = pd.read_csv(data_file_path)
        address_cache = {}
        df['opa_account_num'] = df.apply(receive_opa_account_num_from_row, axis=1, cache=address_cache)
        df.to_csv(new_data_file_path, index=False)
    except FileNotFoundError:
        print("Error: 311 service request file not found.")

def receive_opa_account_num_from_row(row, cache):
    address = row["address"]
    if address == "":
        return ""
    if address in cache:
        return cache[address]
    opa_account_num = receive_opa_account_num_from_address(address)
    cache[address] = opa_account_num
    return opa_account_num

def receive_opa_account_num_from_address(address):
    if address == "":
        return ""
    api_base_url = "https://api.phila.gov/ais/v2/search/"

    response = requests.get(api_base_url + address)
    if response.status_code != 200:
        return ""
    
    json = response.json()
    if json["search_type"] != "address":
        return ""
    
    if len(json["features"]) >= 1:
        return json["features"][0]["properties"]["opa_account_num"]
    return ""

def main():
    retrieve_311_tickets()
    find_opa_account_nums()

if __name__ == "__main__":
    main()
