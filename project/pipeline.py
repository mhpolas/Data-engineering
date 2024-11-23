import os
import zipfile
import requests
import pandas as pd
import sqlite3
from io import BytesIO

# Define the URLs of the datasets
url_gdp_data_zip = "https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv"
url_re_data_csv = "https://pxweb.irena.org:443/sq/a8e3bb11-9ee1-49b6-ac0d-18a477043c83"

# Define the directories
data_directory = '../data'  
if not os.path.exists(data_directory):
    os.makedirs(data_directory)

# List of Latin American countries
latin_american_countries = [
    "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Costa Rica",
    "Cuba", "Dominican Republic", "Ecuador", "El Salvador", "Guatemala",
    "Honduras", "Mexico", "Nicaragua", "Panama", "Paraguay", "Peru",
    "Uruguay", "Venezuela"
]

# Function to download and extract the CSV from the ZIP
def download_and_extract_zip(url, destination_folder):
    response = requests.get(url)
    if response.status_code == 200:
        # Open the ZIP file from the response content
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            # Extract all files to the destination folder
            zip_ref.extractall(destination_folder)
            print(f"Extracted {len(zip_ref.namelist())} files to {destination_folder}")
            # Find CSV files excluding those with 'Metadata' in the name
            csv_files = [file for file in zip_ref.namelist() if file.endswith('.csv') and 'Metadata' not in file]
            if csv_files:
                return os.path.join(destination_folder, csv_files[0])  # Return the first valid CSV file found
            else:
                print("No valid CSV files found in the ZIP archive.")
                return None
    else:
        print(f"Failed to download ZIP data from {url}. Status code: {response.status_code}")
        return None

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
        df = pd.read_csv(file_path, skiprows=skip_rows, header=0, encoding='utf-8')
        print(f"Data from {file_path} loaded successfully with UTF-8 encoding.")
        return df
    except UnicodeDecodeError:
        # Fallback to latin1 encoding if utf-8 fails
        print(f"UTF-8 decoding failed, trying with latin1 encoding for {file_path}.")
        df = pd.read_csv(file_path, skiprows=skip_rows, header=0, encoding='latin1')
        print(f"Data from {file_path} loaded successfully with latin1 encoding.")
        return df
    except Exception as e:
        print(f"Error processing CSV file {file_path}: {e}")
        return None

# Function to filter and clean GDP data
def clean_gdp_data(df):
    filtered = df[df['Country Name'].isin(latin_american_countries)]
    filtered = filtered[["Country Name", "Country Code", "Indicator Name", "Indicator Code", 
                          "2018", "2019", "2020", "2021", "2022"]]
    return filtered.melt(
        id_vars=["Country Name", "Country Code", "Indicator Name", "Indicator Code"],
        var_name="Year", value_name="GDP Growth (%)"
    )

# Function to filter and clean RE data
def clean_re_data(df):
    return df[(df["Region/country/area"].isin(latin_american_countries)) & 
              (df["Year"].between(2018, 2022))]

# Function to export DataFrame to SQLite database
def export_to_sqlite(df, table_name, db_name):
    try:
        # Create a connection to SQLite database (it will create the database file if it doesn't exist)
        conn = sqlite3.connect(db_name)
        # Write the DataFrame to SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        print(f"Data saved to SQLite table '{table_name}' in {db_name}.")
    except Exception as e:
        print(f"Error exporting to SQLite: {e}")
    finally:
        # Close the SQLite connection
        conn.close()

# Download and process the GDP dataset (ZIP file)
gdp_file_path = download_and_extract_zip(url_gdp_data_zip, data_directory)

if gdp_file_path:
    df_gdp = process_csv(gdp_file_path, skip_rows=4)
    if df_gdp is not None:
        df_gdp_cleaned = clean_gdp_data(df_gdp)
else:
    print("GDP data processing failed because the CSV file was not found in the ZIP archive.")

# Download and process the RE dataset (direct CSV file)
csv_re_file = os.path.join(data_directory, "RESHARE.csv")
download_csv(url_re_data_csv, csv_re_file)
df_re = process_csv(csv_re_file)

if df_re is not None:
    df_re_cleaned = clean_re_data(df_re)

# Save the cleaned data back as CSV
gdp_output_csv = os.path.join(data_directory, "gdp_data_cleaned.csv")
re_output_csv = os.path.join(data_directory, "re_data_cleaned.csv")

if df_gdp_cleaned is not None:
    df_gdp_cleaned.to_csv(gdp_output_csv, index=False)
    print(f"Cleaned GDP data saved to {gdp_output_csv}")

if df_re_cleaned is not None:
    df_re_cleaned.to_csv(re_output_csv, index=False)
    print(f"Cleaned RE data saved to {re_output_csv}")

# Export cleaned data to SQLite
sqlite_db_file = os.path.join(data_directory, "data_cleaned.db")

if df_gdp_cleaned is not None:
    export_to_sqlite(df_gdp_cleaned, 'gdp_data', sqlite_db_file)

if df_re_cleaned is not None:
    export_to_sqlite(df_re_cleaned, 'renewable_energy_data', sqlite_db_file)
