# Insurance_data_comparison_and_update
## Description
Banki.ru is a website that provides annual reports and data related to insurance companies, their contracts, and various other financial details. These reports are essential for tracking the performance and activities of insurance companies each year. This script automates the process of comparing these annual data files. It compares two CSV files containing insurance contract data from different periods, identifies common and unique contract entries, and detects any changes between them. The script then updates a database with the relevant data, ensuring accuracy and consistency in financial and insurance reporting.

## Functional Description
The script performs the following steps:
1. Retrieves contract data files from two different periods.
2. Compares the 'contract_id' column between the two files.
3. Identifies common 'contract_id' entries present in both files.
4. Detects contracts only present in the newer file.
5. Compares the data for these common contracts to find any changes.
6. Renames columns in the data to align with the required database schema.
7. Inserts new data entries into a database.
8. Deletes outdated records based on identified contract changes.
9. Updates the database with the modified data.

## How It Works
1. The program lists available folders and lets the user select two folders containing the contract data files.
2. It loads and processes two CSV files from different periods, splitting the data into manageable chunks.
3. The 'contract_id' column is extracted from both datasets and compared to identify the common and unique entries.
4. The script checks for changes in common contract entries between the two datasets and creates a new data frame containing only the changes.
5. The script renames the columns in the data for consistency with the database schema.
6. The relevant data is uploaded to the database, updating the records and deleting outdated entries.

## Input Structure
To run the program, the following parameters need to be provided:
1. File paths: Paths to the two CSV files containing contract data.
2. Database credentials: Username, Password, Database DSN (Data Source Name)
3. The program is designed to work with a specific table in the database where the contract data is stored.

## Technical Requirements
To run the program, the following are required:
1. Python 3.x
2. Installed libraries: pandas, sqlalchemy, ctypes, IPython, time, gc
3. Database with the relevant table for inserting and updating contract data.

## Usage
1. Modify the file paths and database credentials in the script.
2. Run the script. It will:
   - Load contract data files.
   - Identify common and unique contract entries.
   - Detect any changes in the data.
   - Update the database with the new and modified contract data.

## Example Output
1. Files successfully loaded and compared.
2. Changes between the two datasets detected and displayed.
3. Database updated with new and modified contract entries.

## Conclusion
This tool simplifies the comparison of annual contract data files, detects changes between the datasets, and ensures that the database remains up-to-date with accurate and consistent information from Banki.ru's insurance reports.
