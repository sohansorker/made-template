import os
import zipfile
import requests
import pandas as pd
import sqlite3
from io import BytesIO

# Set the directory to store the data
data_folder = "./data"
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# List of Latin American countries to filter the data
latin_american_countries = [
    "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Costa Rica", "Cuba", "Dominican Republic", "Ecuador", 
    "El Salvador", "Guatemala", "Honduras", "Mexico", "Nicaragua", "Panama", "Paraguay", "Peru", "Suriname", "Uruguay", "Venezuela"
]

# URLs for downloading the datasets
electricity_production_url = "https://api.worldbank.org/v2/en/indicator/EG.USE.ELEC.KH.PC?downloadformat=csv"
gdp_growth_url = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv"


def fetch_and_extract_data(url, output_directory):
    """
    Fetches the data from the given URL and extracts the CSV files from the downloaded ZIP archive.
    
    Parameters:
    - url: The API endpoint from which the data will be fetched.
    - output_directory: The directory where the data will be extracted.

    Returns:
    - A list of CSV file paths.
    """
    response = requests.get(url)
    if response.status_code == 200:
        # Extract the zip file from the response content
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            zip_file.extractall(output_directory)
            print(f"Extracted files to {output_directory}")
            # Filter CSV files and exclude metadata files
            csv_files = [f for f in zip_file.namelist() if f.endswith(".csv") and "Metadata" not in f]
            return [os.path.join(output_directory, file) for file in csv_files]
    else:
        print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        return []


def process_data(file_path, selected_countries, years_range):
    """
    Processes the downloaded data by reshaping it into a long format and filtering for the specified countries.

    Parameters:
    - file_path: Path to the CSV file to be processed.
    - selected_countries: List of countries to include in the data.
    - years_range: List of years for which data is required.

    Returns:
    - A reshaped DataFrame with the processed data.
    """
    # Load the CSV file and skip metadata rows
    data_frame = pd.read_csv(file_path, skiprows=4)
    print(f"Data loaded from: {file_path}")
    
    # Ensure that all required columns are present
    required_columns = ["Country Name", "Country Code"] + years_range
    missing_columns = [col for col in required_columns if col not in data_frame.columns]
    if missing_columns:
        print(f"Warning: Missing columns {missing_columns} in the dataset.")
        return pd.DataFrame()
    
    # Filter data for the selected countries
    filtered_data = data_frame[data_frame["Country Name"].isin(selected_countries)][["Country Name", "Country Code", "Indicator Name","Indicator Code"] + years_range]
    
    # Reshape the data into a long format (one row per country-year combination)
    reshaped_data = filtered_data.melt(
        id_vars=["Country Name", "Country Code", "Indicator Name","Indicator Code"],
        var_name="Year",
        value_name="Value"
    )
    return reshaped_data


def save_data_to_sqlite(data_frame, table_name, db_file_path):
    """
    Saves the processed data to an SQLite database.

    Parameters:
    - data_frame: The DataFrame containing the processed data.
    - table_name: The name of the table in the database.
    - db_file_path: Path to the SQLite database file.
    """
    if data_frame.empty:
        print(f"Skipping empty data for table '{table_name}'.")
        return
    
    with sqlite3.connect(db_file_path) as connection:
        data_frame.to_sql(table_name, connection, if_exists="replace", index=False)
    print(f"Data saved to the '{table_name}' table in the database at {db_file_path}.")


# Fetch and extract electricity production data
electricity_data_files = fetch_and_extract_data(electricity_production_url, data_folder)

# Fetch and extract GDP growth data
gdp_data_files = fetch_and_extract_data(gdp_growth_url, data_folder)

# Define the years for analysis
analysis_years = [str(year) for year in range(2015, 2022)]

# Process and save the electricity production data
if electricity_data_files:
    electricity_processed = process_data(
        electricity_data_files[0], latin_american_countries, analysis_years
    )
    if not electricity_processed.empty:
        electricity_processed.to_csv(
            os.path.join(data_folder, "electricity_production_processed.csv"), index=False
        )
        print("Processed electricity production data saved as CSV.")
        save_data_to_sqlite(
            electricity_processed,
            "electricity_production",
            os.path.join(data_folder, "cleaned_data.db")
        )

# Process and save the GDP growth data
if gdp_data_files:
    gdp_processed = process_data(
        gdp_data_files[0], latin_american_countries, analysis_years
    )
    if not gdp_processed.empty:
        gdp_processed.to_csv(
            os.path.join(data_folder, "gdp_growth_processed.csv"), index=False
        )
        print("Processed GDP growth data saved as CSV.")
        save_data_to_sqlite(
            gdp_processed,
            "gdp_growth",
            os.path.join(data_folder, "cleaned_data.db")
        )
