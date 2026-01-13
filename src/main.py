from pathlib import Path
import requests

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

def main():
    retrieve_311_tickets()

if __name__ == "__main__":
    main()
