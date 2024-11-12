import os
import zipfile
import requests
import pandas as pd
from io import BytesIO

# Define the URLs of the datasets
url_gdp_data_zip = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv"
url_re_data_csv = "https://pxweb.irena.org:443/sq/a8e3bb11-9ee1-49b6-ac0d-18a477043c83"

# Define the directories
data_directory = '../data'  # Ensure this is the correct relative path to the data directory
if not os.path.exists(data_directory):
    os.makedirs(data_directory)

# Function to download and extract the CSV from the ZIP
def download_and_extract_zip(url, destination_folder):
    response = requests.get(url)
    if response.status_code == 200:
        # Open the ZIP file from the response content
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            # Extract all files to the destination folder
            zip_ref.extractall(destination_folder)
            print(f"Extracted {len(zip_ref.namelist())} files to {destination_folder}")
    else:
        print(f"Failed to download ZIP data from {url}. Status code: {response.status_code}")

# Function to download a direct CSV file
def download_csv(url, destination_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(destination_path, 'wb') as file:
            file.write(response.content)
        print(f"CSV file downloaded and saved to {destination_path}")
    else:
        print(f"Failed to download CSV from {url}. Status code: {response.status_code}")

# Function to read and clean the CSV file
def process_csv(file_path):
    try:
        # Open and inspect the first few lines of the file
        with open(file_path, 'r', encoding='utf-8') as f:
            for _ in range(5):  # Check first 5 lines
                print(f.readline().strip())
        
        # Attempt to read the CSV file with options to handle errors
        df = pd.read_csv(file_path, on_bad_lines='skip', delimiter=',', encoding='utf-8')  # Try comma delimiter
        print(f"Data from {file_path} loaded successfully.")
        return df
    except Exception as e:
        print(f"Error processing CSV file {file_path}: {e}")
        return None

# Download and process the GDP dataset (ZIP file)
download_and_extract_zip(url_gdp_data_zip, data_directory)
# Assuming the zip contains a CSV file with a known name (fix this based on actual extracted name)
gdp_csv_files = os.listdir(data_directory)  # List files in the data directory
gdp_csv_file = next((file for file in gdp_csv_files if file.endswith('.csv')), None)

if gdp_csv_file:
    gdp_file_path = os.path.join(data_directory, gdp_csv_file)
    df_gdp = process_csv(gdp_file_path)
else:
    print("No CSV file found in GDP ZIP extraction.")

# Download and process the RE dataset (direct CSV file)
csv_re_file = os.path.join(data_directory, "RESHARE.csv")
download_csv(url_re_data_csv, csv_re_file)
df_re = process_csv(csv_re_file)

# Save the processed data back as CSV
gdp_output_csv = os.path.join(data_directory, "gdp_data_processed.csv")
re_output_csv = os.path.join(data_directory, "renewable_energy_data_processed.csv")

# Save DataFrames as CSV files if they were processed successfully
if df_gdp is not None:
    df_gdp.to_csv(gdp_output_csv, index=False)
    print(f"Processed GDP data saved to {gdp_output_csv}")
else:
    print("GDP data processing failed.")

if df_re is not None:
    df_re.to_csv(re_output_csv, index=False)
    print(f"Processed RE data saved to {re_output_csv}")
else:
    print("RE data processing failed.")
