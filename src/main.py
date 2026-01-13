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

def retrieve_violations():
    api_base_url = "https://phl.carto.com/api/v2/sql"
    query = """
    SELECT objectid, opa_account_num, casecreateddate, casestatus, violationnumber, violationcodetitle, violationdate, violationstatus, violationresolutiondate
    FROM violations
    WHERE violationdate >= '2025-01-01'
    """

    query_parameters = {"format": "csv",
                        "skipfields": "cartodb_id,the_geom,the_geom_webmercator",
                        "q": query}
    
    response = requests.get(api_base_url, params=query_parameters, stream=True)

    script_location = Path(__file__).resolve().parent
    data_file_path = script_location.parent / "data" / "violations.csv"
    with open(data_file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

def join_requests_and_violations():
    script_location = Path(__file__).resolve().parent
    service_requests_path = script_location.parent / "data" / "public_cases_fc_2025_opa.csv"
    violations_path = script_location.parent / "data" / "violations.csv"
    joined_data_path = script_location.parent / "data" / "joined.csv"

    try:
        service_requests_df = pd.read_csv(service_requests_path)
        violations_df = pd.read_csv(violations_path)
        
        merged_df = pd.merge(service_requests_df, violations_df, on='opa_account_num', how="inner")

        # make sure datetime columns are converted to datetime format for accurate comparison
        merged_df["requested_datetime"] = pd.to_datetime(merged_df["requested_datetime"])
        merged_df["violationdate"] = pd.to_datetime(merged_df["violationdate"])

        # filter dataframe to get rid of rows where violation was created before the service request
        merged_df = merged_df[merged_df["requested_datetime"] <= merged_df["violationdate"]]  

        merged_df.to_csv(joined_data_path, index=False)
    except FileNotFoundError:
        print("Error: file not found.")



def main():
    retrieve_311_tickets()
    find_opa_account_nums()
    retrieve_violations()
    join_requests_and_violations()

if __name__ == "__main__":
    main()
