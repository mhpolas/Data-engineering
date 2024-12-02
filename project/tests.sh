#!/bin/bash

# Variables for directories and files
DATA_DIR="../data"
OUTPUT_FILE_GDP="${DATA_DIR}/gdp_data_cleaned.csv"
OUTPUT_FILE_RE="${DATA_DIR}/re_data_cleaned.csv"
SCRIPT_PATH="pipeline.py"

# Clean up any previous test artifacts
echo "Cleaning up previous test outputs..."
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# Run the data pipeline script
echo "Executing the data pipeline script..."
python3 "$SCRIPT_PATH"

# Check if the GDP output file exists
if [ -f "$OUTPUT_FILE_GDP" ]; then
    echo "Test passed: GDP output file '$OUTPUT_FILE_GDP' exists."
else
    echo "Test failed: GDP output file '$OUTPUT_FILE_GDP' does not exist."
    exit 1
fi

# Check if the Renewable Energy (RE) output file exists
if [ -f "$OUTPUT_FILE_RE" ]; then
    echo "Test passed: RE output file '$OUTPUT_FILE_RE' exists."
else
    echo "Test failed: RE output file '$OUTPUT_FILE_RE' does not exist."
    exit 1
fi

# Optional: Validate the content of the output files (basic checks)
echo "Validating the content of the GDP output file..."
if head -n 5 "$OUTPUT_FILE_GDP"; then
    echo "Content validation for GDP output file passed."
else
    echo "Content validation for GDP output file failed."
    exit 1
fi

echo "Validating the content of the RE output file..."
if head -n 5 "$OUTPUT_FILE_RE"; then
    echo "Content validation for RE output file passed."
else
    echo "Content validation for RE output file failed."
    exit 1
fi

echo "All tests passed successfully."
exit 0
