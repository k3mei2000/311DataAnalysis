from pathlib import Path
import requests
from ratelimit import limits, sleep_and_retry
import pandas as pd
from matplotlib import pyplot as plt

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

# Prevent calling the AIS api more than 10 times per second.
@sleep_and_retry
@limits(calls=10, period=1)
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

def perform_data_analysis(): 
    script_location = Path(__file__).resolve().parent
    cases_file_path = script_location.parent / "data" / "public_cases_fc_2025.csv"
    joined_file_path = script_location.parent / "data" / "joined.csv"
    try:
        cases_df = pd.read_csv(cases_file_path)
        joined_df = pd.read_csv(joined_file_path)

        # 1. How many service requests since the beginning of the year has 311 associated with "License & Inspections" as the agency_responsible?
        # We just need to count the number of rows in public_cases_fc_2025.csv.
        # The query we used already found all service requests in 2025 that have "License & Inspections" as agency_responsible.
        total_service_requests = len(cases_df)
        print(f"{total_service_requests} service requests were found with License & Inspections responsible since the beginning of the year 2025.")

        # 2. What percentage of these service requests have not been closed? (i.e. L&I has not finished inspecting them)
        # We count the number of rows where the status is not "Closed".
        total_open_service_requests = len(cases_df[cases_df["status"] != "Closed"])
        open_percentage = total_open_service_requests / total_service_requests * 100
        print(f"{total_open_service_requests} service requests, or {open_percentage:.2f}%, have not been closed.")

        # 3. What percentage of these service requests have resulted in the issuance of a code violation?
        # We count the number of distinct service requests in the joined dataset.
        total_violation_service_requests = joined_df["objectid_x"].nunique()
        violation_percentage = total_violation_service_requests / total_service_requests * 100
        print(f"{total_violation_service_requests} service requests, or {violation_percentage:.2f}%, have resulted in the issuance of a code violation.")

        # Create text file with findings
        output_text_file_path = script_location.parent / "output" / "findings.txt"
        with open(output_text_file_path, "w") as f:
            f.write(f"{total_service_requests} service requests were found with License & Inspections responsible since the beginning of the year 2025.\n")
            f.write(f"{total_open_service_requests} service requests, or {open_percentage:.2f}%, have not been closed.\n")
            f.write(f"{total_violation_service_requests} service requests, or {violation_percentage:.2f}%, have resulted in the issuance of a code violation.\n")

        # Create data visualization of findings 
        status_plot_file_path = script_location.parent / "output" / "status.png"
        open_closed_vis_df = pd.DataFrame({
            "Status": ["Not Closed", "Closed"],
            "Percentages": [open_percentage, 100-open_percentage]
        })
        open_closed_vis_df.plot(kind="pie", 
                                y="Percentages", 
                                title="L&I Service Request Statuses", 
                                labels=open_closed_vis_df["Status"], 
                                autopct="%1.1f%%", 
                                colors=["Red","Blue"], 
                                legend=None)
        plt.savefig(status_plot_file_path)

        violation_plot_file_path = script_location.parent / "output" / "violation.png"
        violation_vis_df = pd.DataFrame({
            "ViolationStatus": ["Violation Found", "No Violation Found"],
            "Percentages": [violation_percentage, 100-violation_percentage]
        })
        violation_vis_df.plot(kind="pie", 
                              y="Percentages", 
                              title="Service Requests Resulting in Violations", 
                              labels=violation_vis_df["ViolationStatus"], 
                              autopct="%1.1f%%", 
                              colors=["Red","Blue"], 
                              legend=None)
        plt.savefig(violation_plot_file_path)
       
    except FileNotFoundError as e:
        print(f"Error: {e}")


def main():
    # Data retrieval functions
    retrieve_311_tickets()
    find_opa_account_nums()
    retrieve_violations()
    join_requests_and_violations()

    # Data analysis functions
    perform_data_analysis() 

if __name__ == "__main__":
    main()
