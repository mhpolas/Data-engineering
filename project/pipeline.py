
import os
import zipfile
import requests
import pandas as pd
from io import BytesIO

# Define the URLs of the datasets
url_gdp_data_zip = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv"
url_re_data_csv = "https://pxweb.irena.org:443/sq/a8e3bb11-9ee1-49b6-ac0d-18a477043c83"

# Define the directories
data_directory = './data'
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
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(file_path)
    # Add any data cleaning or transformation steps here (e.g., renaming columns, handling missing values)
    print(f"Data from {file_path} loaded successfully.")
    return df

# Download and process the GDP dataset (ZIP file)
download_and_extract_zip(url_gdp_data_zip, data_directory)
# Assuming the zip contains a CSV file with a known name
csv_gdp_file = os.path.join(data_directory, "API_NY.GDP.MKTP.KD.ZG_DS2_en_csv_v2_10065.csv")
df_gdp = process_csv(csv_gdp_file)

# Download and process the RE dataset (direct CSV file)
csv_re_file = os.path.join(data_directory, "RE_DATA.csv")
download_csv(url_re_data_csv, csv_re_file)
df_re = process_csv(csv_re_file)

# Save the processed data to SQLite (or another format like CSV)
sqlite_file = os.path.join(data_directory, "processed_data.db")
conn = pd.SQLiteConnection(sqlite_file)
df_gdp.to_sql('gdp_data', conn, if_exists='replace', index=False)
df_re.to_sql('renewable_energy_data', conn, if_exists='replace', index=False)

# Close the connection
conn.close()

print(f"Data saved to {sqlite_file}")
