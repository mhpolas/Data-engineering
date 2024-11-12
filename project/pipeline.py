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
def process_csv(file_path, skip_rows=0):
    try:
        # Try reading the file with 'utf-8' encoding first, if it fails, fall back to 'latin1'
        df = pd.read_csv(file_path, skiprows=skip_rows, encoding='utf-8')
        print(f"Data from {file_path} loaded successfully with UTF-8 encoding.")
        return df
    except UnicodeDecodeError:
        # Fallback to latin1 encoding if utf-8 fails
        print(f"UTF-8 decoding failed, trying with latin1 encoding for {file_path}.")
        df = pd.read_csv(file_path, skiprows=skip_rows, encoding='latin1')
        print(f"Data from {file_path} loaded successfully with latin1 encoding.")
        return df
    except Exception as e:
        print(f"Error processing CSV file {file_path}: {e}")
        return None

# Download and process the GDP dataset (ZIP file)
download_and_extract_zip(url_gdp_data_zip, data_directory)

# The exact file you're looking for
gdp_csv_file = "API_NY.GDP.MKTP.KD.ZG_DS2_en_csv_v2_10065.csv"
gdp_file_path = os.path.join(data_directory, gdp_csv_file)

# Process the GDP data, skipping the first 4 rows
if os.path.exists(gdp_file_path):
    df_gdp = process_csv(gdp_file_path, skip_rows=4)
else:
    print(f"File {gdp_csv_file} not found in the extracted files.")

# Download and process the RE dataset (direct CSV file)
csv_re_file = os.path.join(data_directory, "RESHARE.csv")
download_csv(url_re_data_csv, csv_re_file)
df_re = process_csv(csv_re_file,skip_rows=0)

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
