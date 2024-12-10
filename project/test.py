import os
import subprocess
import pandas as pd

# Paths to the expected output files
EXPECTED_OUTPUT_FILES = [
    '../data/re_data_cleaned.csv',
    '../data/gdp_data_cleaned.csv',
    '../data/data_cleaned.db'
]

# Function to run the ETL pipeline and capture its output
def run_etl_pipeline():
    print("Running ETL pipeline...")

    # Use subprocess to execute the pipeline
    process = subprocess.Popen(['python', './project/pipeline.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        print(f"ETL pipeline failed with error: {stderr.decode()}")
        return False

    print(f"ETL pipeline ran successfully. Output: {stdout.decode()}")
    return True

# Function to validate the existence and content of files
def validate_output_files():
    print("Validating output files...")

    # Initialize an empty list for errors
    errors = []

    # Check if the output files exist and if they are non-empty
    for file in EXPECTED_OUTPUT_FILES:
        if not os.path.exists(file):
            errors.append(f"File missing: {file}")
        elif os.path.getsize(file) == 0:
            errors.append(f"File is empty: {file}")
    
    if errors: 
        for error in errors:
            print(error)
        return False
    
    # Include data file names in success messages
    print(f"Success: All files exist and are non-empty:")
    for file in EXPECTED_OUTPUT_FILES:
        print(f"  - {file}")
    
    return True

# Function to check for missing values in the cleaned data files
def check_missing_values():
    print("Checking data Files for missing values...")

    try:
        # Read cleaned data into pandas DataFrames
        re_data = pd.read_csv(EXPECTED_OUTPUT_FILES[0])
        gdp_data = pd.read_csv(EXPECTED_OUTPUT_FILES[1])
    except Exception as e:
        print(f"Error while loading data: {e}")
        return False

    # Check if there are missing values in any of the DataFrames
    missing_values_re_data = re_data.isnull().sum().sum()
    missing_values_gdp_data = gdp_data.isnull().sum().sum()

    if missing_values_re_data > 0:
        print(f"Missing values found in {EXPECTED_OUTPUT_FILES[0]}.")
        return False
    if missing_values_gdp_data > 0:
        print(f"Missing values found in {EXPECTED_OUTPUT_FILES[1]}.")
        return False

    # Success messages with data file names included
    print("No missing values found in the following data files:")
    print(f"  - {EXPECTED_OUTPUT_FILES[0]}")
    print(f"  - {EXPECTED_OUTPUT_FILES[1]}")

    return True

# Main function to run all tests sequentially
def run_tests():
    # First, run the ETL pipeline and validate if it worked
    if not run_etl_pipeline():
        print("ETL pipeline test failed.")
        return

    # Then, validate the output files
    if not validate_output_files():
        print("File validation test failed.")
        return

    # Finally, check for missing values in the cleaned data
    if not check_missing_values():
        print("Missing value test failed.")
        return

    # Final success message
    print("All tests passed successfully!")

if __name__ == "__main__":
    run_tests()
