import os
import time
import warnings
from IPython.display import display, clear_output
import gc
import ctypes
import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, select, or_, and_, Table, MetaData, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base

# folders = [f for f in os.listdir() if os.path.isdir(f) and not f.endswith('.zip') and not f.endswith('.rar')]

# print("Select a folder:")
# for i, folder in enumerate(folders):
#     print(f"{i+1}. {folder}")
# while True:
#     try:
#         index1 = int(input("Enter the index of the first folder: ")) - 1
#         if index1 < 0 or index1 >= len(folders):
#             print("Invalid index. Please try again.")
#         else:
#             break
#     except ValueError:
#         print("Invalid input. Please enter a number.")

# while True:
#     try:
#         index2 = int(input("Enter the index of the second folder: ")) - 1
#         if index2 < 0 or index2 >= len(folders):
#             print("Invalid index. Please try again.")
#         elif index2 == index1:
#             print("You cannot select the same folder twice. Please try again.")
#         else:
#             break
#     except ValueError:
#         print("Invalid input. Please enter a number.")

# folder1 = folders[index1]
# folder2 = folders[index2]

# for folder in [folder1, folder2]:
#     for root, dirs, files in os.walk(folder):
#         for file in files:
#             if "contracts" in file.lower():
#                 file_path = os.path.join(root, file)
#                 if folder == folder1:
#                     file23 = f'r"{file_path}"'
#                 else:
#                     file24 = f'r"{file_path}"'
#                 break

# print(f"File in folder {folder1}: {file23}")
# print(f"File in folder {folder2}: {file24}")
# time.sleep(8)
# clear_output()

file23 = r"SK_contracts_0109.csv"
file24 = r"SK_contracts_0109.csv"

print("\nWARNING:   The entire script runs for 3 - 4 hours")

# ctypes is used to access objects by memory address
class PyObject(ctypes.Structure):
    _fields_ = [("refcnt", ctypes.c_long)]

gc.disable()  # Disable cyclic GC

print("\n1/19: Let's start reading the first file. 15 minutes")
with pd.read_csv(r"SK_contracts_0109.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=';', encoding='cp1251', quoting=3, low_memory=False
                ) as reader:
    chunks = []
    for chunk in reader:
        chunks.append(chunk)
    df_doc_23 = pd.concat(chunks, ignore_index=True)

clear_output()
print("\n2/19: Reading completed")

del chunks
del chunk
del reader

print("\n3/19: Let's start reading the second file. 15 minutes")
with pd.read_csv(r"SK_contracts_0109.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=';', encoding='cp1251', quoting=3, low_memory=False
                ) as reader:
    chunks = []
    for chunk in reader:
        chunks.append(chunk)
    df_doc_24 = pd.concat(chunks, ignore_index=True)

clear_output()
print("4/19: Reading completed")

del chunks
del chunk
del reader

# Select the 'contract_id' column from both dataframes
contract_ids_23 = df_doc_23['contract_id'].tolist()
clear_output()
print("\n5/19: The id column from the first file has been read")
contract_ids_24 = df_doc_24['contract_id'].tolist()
print("\n6/19: The id column from the second file has been read")

in_23_in_24_contract_ids = list(set(contract_ids_24) & set(contract_ids_23))
clear_output()
print("\n7/19: The ids which are found in both old and new files")
only_in_24 = list(set(contract_ids_24) - set(in_23_in_24_contract_ids))
print("\n8/19: The ids which are only found in the new file")

# Create a new dataframe with rows that have 'contract_id' from only_in_24
only_in_24_df = df_doc_24[df_doc_24['contract_id'].isin(only_in_24)]
clear_output()
print("\n9/19: Contracts located only in the last file were found")

doc_24_not_in_only_in_24_df = df_doc_24[~df_doc_24.index.isin(only_in_24_df.index)]
clear_output()
print("\n10/19: Non-unique rows are found in the new file")
in_23_in_24_contract_ids_df = pd.DataFrame(in_23_in_24_contract_ids, columns=['contract_id'])
print("\n11/19: Rows which are found in both old and new files")

del contract_ids_23
del contract_ids_24
del in_23_in_24_contract_ids
del only_in_24

filter_df_23 = df_doc_23[df_doc_23['contract_id'].isin(in_23_in_24_contract_ids_df['contract_id'])]
filter_df_24 = df_doc_24[df_doc_24['contract_id'].isin(in_23_in_24_contract_ids_df['contract_id'])]
clear_output()
print("\n12/19: Data have been sorted")

merged_df = filter_df_23.merge(filter_df_24, on='contract_id', suffixes=('_23', '_24'))
clear_output()
print("\n13/19: Data compiled")

changed_rows_df = merged_df.loc[~(merged_df.filter(regex='_23$').values == merged_df.filter(regex='_24$').values).all(axis=1)]
clear_output()
print("\n14/19: Changed data found")

change_in_24_df = changed_rows_df[[col for col in changed_rows_df.columns if '_24' in col or 'contract_id' in col]]

change_in_24_df.columns = change_in_24_df.columns.str.replace('_24', '')

del change_in_24_df['cis_contract_id_23']
change_in_24_df = change_in_24_df.drop_duplicates()
clear_output()
print("\n15/19: DataFrame for changed rows in the new file created")

del doc_24_not_in_only_in_24_df
del in_23_in_24_contract_ids_df
del filter_df_23
del filter_df_24
del merged_df
del changed_rows_df 

column_mapping = {
    'insurer_id': 'INSURER_ID', 'short_name': 'INSURER_NAME', 'contract_series': 'CONTRACT_SERIES',
    'contract_number': 'CONTRACT_NUMBER', 'contract_id': 'CONTRACT_ID_AIS_OSAGO', 'addendum_id': 'ADDENDUM_ID_AIS_OSAGO',
    'cis_contract_id': 'CONTRACT_ID_SK', 'cis_addendum_id': 'ADDENDUM_ID_SK', 'business_object_type_code': 'BUSINESS_OBJECT_TYPE_CODE',
    'addendum_type_code': 'ADDENDUM_TYPE_CODE', 'closing_type_name': 'CLOSURE_METHOD', 'reinsured_indicator': 'REINSURED_INDICATOR', 'tpi': 'TPI',
    'tb': 'BASE_TARIF', 'max_tb': 'MAX_TB', 'premium_amount': 'PREMIUM_AMOUNT', 'termination_date': 'TERMINATION_DATE_TIME',
    'category': 'VEHICLE_CATEGORY', 'closing_date': 'CONTRACT_CLOSURE_DATE', 'effective_date': 'CONTRACT_START_DATE',
    'expiration_date': 'CONTRACT_END_DATE', 'vehicle_utilisation_period1_begin': 'VEHICLE_USAGE_PERIOD1_START',
    'vehicle_utilisation_period1_end': 'VEHICLE_USAGE_PERIOD1_END', 'vehicle_utilisation_period2_begin': 'VEHICLE_USAGE_PERIOD2_START',
    'vehicle_utilisation_period2_end': 'VEHICLE_USAGE_PERIOD2_END', 'vehicle_utilisation_period3_begin': 'VEHICLE_USAGE_PERIOD3_START',
    'vehicle_utilisation_period3_end': 'VEHICLE_USAGE_PERIOD3_END', 'status_name': 'CONTRACT_STATUS_DS', 'tpi_fias': 'TPI_FIAS',
    'tpi_kladr': 'TPI_KLADR', 'federal_district': 'FEDERAL_DISTRICT', 'first_load_date': 'FIRST_LOAD_DATE', 
    'vehicle_owner_type': 'VEHICLE_OWNER_TYPE', 'OnTimeCompliance': 'ON_TIME_COMPLIANCE', 'MissingFlag': 'MISSING_FLAG'
}

only_in_24_df = only_in_24_df.rename(columns=column_mapping)
change_in_24_df = change_in_24_df.rename(columns=column_mapping)
clear_output()
print("\n16/19: Columns names renamed")

del df_doc_23
del df_doc_24

only_in_24_df.to_csv('only_in_24_df.csv', index=False)
change_in_24_df.to_csv('chenge_in_24_df.csv', index=False)
print("Done")

# only_in_24_df = pd.read_csv('only_in_24_df.csv', low_memory=False)
import pandas as pd
change_in_24_df = pd.read_csv('chenge_in_24_df.csv', low_memory=False)
print("Done")

len(change_in_24_df)

import sys
import oracledb
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

# Database connection settings
username = 'KOSTYASHOV'
password = 'KOSTYASHOV'
dsn = 'DWHPROD'

# Create database connection
conection_string = f'oracle+cx_oracle://{username}:{password}@{dsn}'
engine = create_engine(conection_string)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

# Define table and class
Base = declarative_base()

class AlBabkinaKostyashovNastyReins12082024_3(Base):
    __tablename__ = 'al_babkina_kostyashov_nasty_reins12082024_3'
    __table_args__ = {'autoload_with': engine}
    id = Column(Integer, primary_key=True)

# Define table object
metadata = Base.metadata
al_babkina_kostyashov_nasty_reins12082024_3 = Table('al_babkina_kostyashov_nasty_reins12082024_3', metadata, autoload_with=engine)

# Delete rows in chunks
ids_to_delete = list(change_in_24_df['ID_ДОГОВОРА_АИС_ОСАГО'])
total_cycles = len(ids_to_delete) // 1000 + 1

with session.begin():
    for n in range(total_cycles):
        chunk = ids_to_delete[n*1000:(n+1)*1000]
        session.query(al_babkina_kostyashov_nasty_reins12082024_3).filter(
            al_babkina_kostyashov_nasty_reins12082024_3.c.ID_ДОГОВОРА_АИС_ОСАГО.in_(chunk)
        ).delete()
        clear_output()
        print(f"18/19: Выполнено {n+1} циклов из {total_cycles}")
    session.commit()

# Insert rows in chunks
chunksize = 30000
total_records = len(change_in_24_df)
total_cycles = -(-total_records // chunksize)

with session.begin():
    for n in range(total_cycles):
        start = n * chunksize
        end = min((n + 1) * chunksize, total_records)
        chunk = change_in_24_df.iloc[start:end]
        chunk.to_sql('al_babkina_kostyashov_nasty_reins12082024_3', engine, if_exists='append', index=False)
        clear_output()
        print(f"\n19/19: Completed {end} cycles out of {total_records}")
    session.commit()

print("\nThat's all!")
time.sleep(5)

username = 'username'
password = 'password'
dsn = 'dsn'

conection_string = f'oracle+cx_oracle://{username}:{password}@{dsn}'

# Create engine
engine = create_engine(conection_string)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

only_in_24_df.to_sql('al_babkina_kostyashov_nasty_reins12082024_2', engine, if_exists='append', index=False)
session.commit()
clear()
print("\n17/19: Lines that appeared in the last file are included")

ids_to_delete = list(change_in_24_df['ID_ДОГОВОРА_АИС_ОСАГО'])
total_cycles = len(ids_to_delete) // 1000 + 1

clear()
with session.begin():
    for n in range(total_cycles):
        chunk = ids_to_delete[n*1000:(n+1)*1000]
        ids_str = ",".join(f"'{id1}'" for id1 in chunk)
        delete_query = text(f"DELETE FROM al_babkina_kostyashov_nasty_reins12082024_2 WHERE ID_ДОГОВОРА_АИС_ОСАГО IN ({ids_str})")
        session.execute(delete_query)
        clear()
        display(f"\n18/19: Completed {n+1} cycles out of {total_cycles}")
    session.commit()

session.close()

chunksize = 30000
total_records = len(change_in_24_df)
total_cycles = -(-total_records // chunksize) 

clear()
with session.begin():
    for n in range(total_cycles):
        start = n * chunksize
        end = min((n + 1) * chunksize, total_records)
        chunk = change_in_24_df.iloc[start:end]
        chunk.to_sql('al_babkina_kostyashov_nasty_reins12082024_2', engine, if_exists='append', index=False)
        clear()
        print(f"\n19/19: Loaded {end} rows out of {total_records}")
    session.commit()

print("\nThat's all!")
time.sleep(5)

import sys
import oracledb
import pandas as pd
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb


# Create database connection
conection_string = f'oracle+cx_oracle://{username}:{password}@{dsn}'
engine = create_engine(conection_string)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

from sqlalchemy import text

query = text("TRUNCATE TABLE al_babkina_for_script_banki_ry")
session.execute(query)
session.commit()

print("Done")

chunksize = 30000
total_records = len(change_in_24_df)
total_cycles = -(-total_records // chunksize) 

clear_output()
with session.begin():
    for n in range(total_cycles):
        start = n * chunksize
        end = min((n + 1) * chunksize, total_records)
        chunk = change_in_24_df.iloc[start:end]
        chunk.to_sql('al_babkina_for_script_banki_ry', engine, if_exists='append', index=False)
        clear_output()
        print(f"\n19/19: Загружено {end} строк из {total_records}")
    session.commit()

print("\nThat's all!")
# change_in_24_df.to_sql('al_babkina_for_script_banki_ry', engine, if_exists='replace', index=False)

import sys
import oracledb
import pandas as pd
from sqlalchemy import create_engine, Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
 # Create database connection
conection_string = f'oracle+cx_oracle://{username}:{password}@{dsn}'
engine = create_engine(conection_string)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

# Update data in al_babkina_kostyashov_nasty_reins12082024_3 table
update_query = """
    UPDATE al_babkina_kostyashov_nasty_reins12082024_3 t1
    SET (t1.*) = (
        SELECT t2.*
        FROM al_babkina_for_script_banki_ry t2
        WHERE t1.ID_ДОГОВОРА_АИС_ОСАГО = t2.ID_ДОГОВОРА_АИС_ОСАГО
    )
"""
session.execute(update_query)
session.commit()


#######################################################################################################################################################################################

import gc
import ctypes
import time

# ctypes is used to access objects by memory address
class PyObject(ctypes.Structure):
    _fields_ = [("refcnt", ctypes.c_long)]

gc.disable()  # Disable cyclic garbage collection

import pandas as pd

print("Let's start reading 1 file. 15 minutes")
with pd.read_csv(r"СК договоры 0109.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=';', encoding='cp1251', quoting=3, low_memory=False
                ) as reader:
    chunks = []
    for chunk in reader:
        chunks.append(chunk)
    df_doc_23 = pd.concat(chunks, ignore_index=True)

print("Reading completed")

del chunks
del chunk
del reader
print(PyObject.from_address(id(df_doc_23)).refcnt)

print("Let's start reading 2 file. 15 minutes")
with pd.read_csv(r"СК договоры 0109.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=';', encoding='cp1251', quoting=3, low_memory=False
                ) as reader:
    chunks = []
    for chunk in reader:
        chunks.append(chunk)
    df_doc_24 = pd.concat(chunks, ignore_index=True)
    
print("Reading completed")

del chunks
del chunk
del reader
print(PyObject.from_address(id(df_doc_24)).refcnt)

# Select 'contract_id' column from both dataframes
contract_ids_23 = df_doc_23['contract_id'].tolist()
print("The id column from the 1 file has been read")
contract_ids_24 = df_doc_24['contract_id'].tolist()
print("The id column from the 2 file has been read")

in_23_in_24_contract_ids = list(set(contract_ids_24) & set(contract_ids_23))
print("The id which are found in 23 and 24")
only_in_24 = list(set(contract_ids_24) - set(in_23_in_24_contract_ids))
print("The id which are only found in 24")

# Create a new dataframe with rows that have 'contract_id' values from only_in_24
only_in_24_df = df_doc_24[df_doc_24['contract_id'].isin(only_in_24)]
print("Contracts located only in the last file were found")

doc_24_not_in_only_in_24_df = df_doc_24[~df_doc_24.index.isin(only_in_24_df.index)]
print("Not unique rows are found in 24")
in_23_in_24_contract_ids_df = pd.DataFrame(in_23_in_24_contract_ids, columns=['contract_id'])
print("Rows which are found in 23 and 24")

del contract_ids_23
del contract_ids_24
del in_23_in_24_contract_ids
del only_in_24
print(PyObject.from_address(id(only_in_24_df)).refcnt)

filter_df_23 = df_doc_23[df_doc_23['contract_id'].isin(in_23_in_24_contract_ids_df['contract_id'])]
filter_df_24 = df_doc_24[df_doc_24['contract_id'].isin(in_23_in_24_contract_ids_df['contract_id'])]
print("Data have been sorted")

merged_df = filter_df_23.merge(filter_df_24, on='contract_id', suffixes=('23', '24'))
print("Data completed")

changed_rows_df = merged_df.loc[~(merged_df.filter(regex='23$').values == merged_df.filter(regex='24$').values).all(axis=1)]
print("Changed data found")

change_in_24_df = changed_rows_df[[col for col in changed_rows_df.columns if '24' in col or 'contract_id' in col]]

change_in_24_df.columns = change_in_24_df.columns.str.replace('24', '')

del change_in_24_df['cis_contract_id_23']
change_in_24_df = change_in_24_df.drop_duplicates()

print("DataFrame for 24 year changed rows created")

del doc_24_not_in_only_in_24_df
del in_23_in_24_contract_ids_df
del filter_df_23
del filter_df_24
del merged_df
del changed_rows_df 
print(PyObject.from_address(id(change_in_24_df)).refcnt)

column_mapping = {
    'insurer_id': 'INSURER_COMPANY_ID', 'short_name': 'INSURER_NAME', 'contract_series': 'CONTRACT_SERIES',
    'contract_number': 'CONTRACT_NUMBER', 'contract_id': 'CONTRACT_ID_AIS_OSAGO', 'addendum_id': 'ADDENDUM_ID_AIS_OSAGO',
    'cis_contract_id': 'CONTRACT_ID_INSURER', 'cis_addendum_id': 'ADDENDUM_ID_INSURER', 'business_object_type_code': 'BUSINESS_OBJECT_TYPE',
    'addendum_type_code': 'ADDENDUM_TYPE', 'closing_type_name': 'CLOSING_METHOD', 'reinsured_indicator': 'REINSURED_FLAG', 'tpi': 'TPI',
    'tb': 'BASE_TARIFF', 'max_tb': 'MAX_TARIFF', 'premium_amount': 'INSURANCE_PREMIUM', 'termination_date': 'TERMINATION_DATE',
    'category': 'VEHICLE_CATEGORY', 'closing_date': 'CONTRACT_CLOSING_DATE', 'effective_date': 'CONTRACT_EFFECTIVE_DATE',
    'expiration_date': 'CONTRACT_EXPIRATION_DATE', 'vehicle_utilisation_period1_begin': 'START_DATE_1ST_PERIOD',
    'vehicle_utilisation_period1_end': 'END_DATE_1ST_PERIOD', 'vehicle_utilisation_period2_begin': 'START_DATE_2ND_PERIOD',
    'vehicle_utilisation_period2_end': 'END_DATE_2ND_PERIOD', 'vehicle_utilisation_period3_begin': 'START_DATE_3RD_PERIOD',
    'vehicle_utilisation_period3_end': 'END_DATE_3RD_PERIOD', 'status_name': 'CONTRACT_STATUS', 'tpi_fias': 'TPI_FIAS',
    'tpi_kladr': 'TPI_KLADR', 'federal_district': 'FEDERAL_DISTRICT', 'first_load_date': 'FIRST_LOAD_DATE',
    'vehicle_owner_type': 'VEHICLE_OWNER_TYPE', 'Соблюдение сроков': 'COMPLIANCE_WITH_TERMS', 'Отсутствие признака': 'ABSENCE_OF_FLAG'
}

only_in_24_df = only_in_24_df.rename(columns=column_mapping)
change_in_24_df = change_in_24_df.rename(columns=column_mapping)
print("Column names renamed")

only_in_24_df.to_csv('only_in_24_df.csv', index=False)
change_in_24_df.to_csv('change_in_24_df.csv', index=False)
print("Done")

del df_doc_23
del df_doc_24
print(PyObject.from_address(id(only_in_24_df)).refcnt)
print(PyObject.from_address(id(change_in_24_df)).refcnt)

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Connection string
username = 'username'
password = 'password'
dsn = 'dsn'

connection_string = f'oracle+oracledb://{username}:{password}@{dsn}'

# Create engine
engine = create_engine(connection_string)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

only_in_24_df.to_sql('al_babkina_kostyashov_nasty_reins12082024', engine, if_exists='append', index=False)
session.commit()
print("Done")

ids_to_delete = list(change_in_24_df['CONTRACT_ID_AIS_OSAGO'])
total_cycles = len(ids_to_delete) // 1000 + 1

with session.begin():
    for n in range(total_cycles):
        chunk = ids_to_delete[n*1000:(n+1)*1000]
        ids_str = ",".join(f"'{id1}'" for id1 in chunk)
        delete_query = text(f"DELETE FROM al_babkina_kostyashov_nasty_reins12082024 WHERE CONTRACT_ID_AIS_OSAGO IN ({ids_str})")
        session.execute(delete_query)
        clear_output()
        display(f"Completed {n+1} cycles out of {total_cycles}")
    session.commit()

session.close()
print("Done")

chunksize = 30000
total_records = len(change_in_24_df)
total_cycles = -(-total_records // chunksize) 

with session.begin():
    for n in range(total_cycles):
        start = n * chunksize
        end = min((n + 1) * chunksize, total_records)
        chunk = change_in_24_df.iloc[start:end]
        chunk.to_sql('al_babkina_kostyashov_nasty_reins12082024', engine, if_exists='append', index=False)
        clear_output()
        print(f"Completed {end} cycles out of {total_records}")
    session.commit()
    
##############################################################################################################################################################

import os
import time
import warnings
from IPython.display import display, clear_output
import gc
import ctypes
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

folders = [f for f in os.listdir() if os.path.isdir(f) and not f.endswith('.zip') and not f.endswith('.rar')]

print("Select a folder:")
for i, folder in enumerate(folders):
    print(f"{i+1}. {folder}")
while True:
    try:
        index1 = int(input("Enter the index of the first folder: ")) - 1
        if index1 < 0 or index1 >= len(folders):
            print("Invalid index. Please try again.")
        else:
            break
    except ValueError:
        print("Invalid input. Please enter a number.")

while True:
    try:
        index2 = int(input("Enter the index of the second folder: ")) - 1
        if index2 < 0 or index2 >= len(folders):
            print("Invalid index. Please try again.")
        elif index2 == index1:
            print("You cannot select the same folder twice. Please try again.")
        else:
            break
    except ValueError:
        print("Invalid input. Please enter a number.")

folder1 = folders[index1]
folder2 = folders[index2]

for folder in [folder1, folder2]:
    for root, dirs, files in os.walk(folder):
        for file in files:
            if "contracts" in file.lower():
                file_path = os.path.join(root, file)
                if folder == folder1:
                    file23 = f'r"{file_path}"'
                else:
                    file24 = f'r"{file_path}"'
                break

print(f"File in folder {folder1}: {file23}")
print(f"File in folder {folder2}: {file24}")
time.sleep(8)

print("ATTENTION: The entire script will run for 3-4 hours")

# ctypes is used for accessing objects by memory address
class PyObject(ctypes.Structure):
    _fields_ = [("refcnt", ctypes.c_long)]

gc.disable()  # disabling cyclic GC

print("1/19: Let's start reading the first file. 15 minutes")
with pd.read_csv(r"\\Fs17\reports$\current_reports_2017\MAX\for_transmission\for_Vania\! Reinsurance\2024\19082024\SK_contracts_0109.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=';', encoding='cp1251', quoting=3, low_memory=False
                ) as reader:
    chunks = []
    for chunk in reader:
        chunks.append(chunk)
    df_doc_23 = pd.concat(chunks, ignore_index=True)

clear_output()
print("2/19: Reading completed")

del chunks
del chunk
del reader

print("3/19: Let's start reading the second file. 15 minutes")
with pd.read_csv(r"SK_contracts_0109.csv", 
                 chunksize=10000, on_bad_lines='skip', encoding_errors='ignore', sep=';', encoding='cp1251', quoting=3, low_memory=False
                ) as reader:
    chunks = []
    for chunk in reader:
        chunks.append(chunk)
    df_doc_24 = pd.concat(chunks, ignore_index=True)

clear_output()
print("4/19: Reading completed")

del chunks
del chunk
del reader

# Select the 'contract_id' column from both dataframes
contract_ids_23 = df_doc_23['contract_id'].tolist()
clear_output()
print("5/19: The id column from the first file has been read")
contract_ids_24 = df_doc_24['contract_id'].tolist()
print("6/19: The id column from the second file has been read")

in_23_in_24_contract_ids = list(set(contract_ids_24) & set(contract_ids_23))
clear_output()
print("7/19: The ids found in both old and new files")
only_in_24 = list(set(contract_ids_24) - set(in_23_in_24_contract_ids))
print("8/19: The ids found only in the new file")

# Create a new dataframe with rows that have 'contract_id' values from only_in_24
only_in_24_df = df_doc_24[df_doc_24['contract_id'].isin(only_in_24)]
clear_output()
print("9/19: Contracts located only in the last file were found")

doc_24_not_in_only_in_24_df = df_doc_24[~df_doc_24.index.isin(only_in_24_df.index)]
clear_output()
print("10/19: Non-unique rows found in the new file")
in_23_in_24_contract_ids_df = pd.DataFrame(in_23_in_24_contract_ids, columns=['contract_id'])
print("11/19: Rows found in both old and new files")

del contract_ids_23
del contract_ids_24
del in_23_in_24_contract_ids
del only_in_24

filter_df_23 = df_doc_23[df_doc_23['contract_id'].isin(in_23_in_24_contract_ids_df['contract_id'])]
filter_df_24 = df_doc_24[df_doc_24['contract_id'].isin(in_23_in_24_contract_ids_df['contract_id'])]
clear_output()
print("12/19: Data sorted")

merged_df = filter_df_23.merge(filter_df_24, on='contract_id', suffixes=('_23', '_24'))
clear_output()
print("13/19: Data completed")

changed_rows_df = merged_df.loc[~(merged_df.filter(regex='_23$').values == merged_df.filter(regex='_24$').values).all(axis=1)]
clear_output()
print("14/19: Changed data found")

change_in_24_df = changed_rows_df[[col for col in changed_rows_df.columns if '_24' in col or 'contract_id' in col]]

change_in_24_df.columns = change_in_24_df.columns.str.replace('_24', '')

del change_in_24_df['cis_contract_id_23']
change_in_24_df = change_in_24_df.drop_duplicates()
clear_output()
print("15/19: DataFrame of changed rows in the new file created")

del doc_24_not_in_only_in_24_df
del in_23_in_24_contract_ids_df
del filter_df_23
del filter_df_24
del merged_df
del changed_rows_df

column_mapping = {
    'insurer_id': 'INSURANCE_COMPANY_ID', 'short_name': 'INSURANCE_COMPANY_NAME', 'contract_series': 'CONTRACT_SERIES',
    'contract_number': 'CONTRACT_NUMBER', 'contract_id': 'CONTRACT_ID_AIS_OSAGO', 'addendum_id': 'ADDENDUM_ID_AIS_OSAGO',
    'cis_contract_id': 'CONTRACT_ID_SK', 'cis_addendum_id': 'ADDENDUM_ID_SK', 'business_object_type_code': 'BUSINESS_OBJECT_TYPE',
    'addendum_type_code': 'ADDENDUM_TYPE', 'closing_type_name': 'CLOSING_METHOD', 'reinsured_indicator': 'REINSURED_INDICATOR', 'tpi': 'TPI',
    'tb': 'BASE_TARIFF', 'max_tb': 'MAX_BASE_TARIFF', 'premium_amount': 'INSURANCE_PREMIUM', 'termination_date': 'TERMINATION_DATE_AND_TIME',
    'category': 'VEHICLE_CATEGORY', 'closing_date': 'CONTRACT_CLOSING_DATE', 'effective_date': 'CONTRACT_START_DATE',
    'expiration_date': 'CONTRACT_END_DATE', 'vehicle_utilisation_period1_begin': 'START_DATE_1ST_PERIOD_IS',
    'vehicle_utilisation_period1_end': 'END_DATE_1ST_PERIOD_IS', 'vehicle_utilisation_period2_begin': 'START_DATE_2ND_PERIOD_IS',
    'vehicle_utilisation_period2_end': 'END_DATE_2ND_PERIOD_IS', 'vehicle_utilisation_period3_begin': 'START_DATE_3RD_PERIOD_IS',
    'vehicle_utilisation_period3_end': 'END_DATE_3RD_PERIOD_IS', 'status_name': 'CONTRACT_STATUS_DS', 'tpi_fias': 'TPI_FIAS',
    'tpi_kladr': 'TPI_KLADR', 'federal_district': 'FEDERAL_DISTRICT', 'first_load_date': 'FIRST_LOAD_DATE', 
    'vehicle_owner_type': 'VEHICLE_OWNER_TYPE', 'Compliance_time': 'TIME_COMPLIANCE', 'Absence_of_feature': 'ABSENCE_OF_FEATURE'
}

only_in_24_df = only_in_24_df.rename(columns=column_mapping)
change_in_24_df = change_in_24_df.rename(columns=column_mapping)
clear_output()
print("16/19: Columns names renamed")

del df_doc_23
del df_doc_24

# Connection string
username = 'KOSTYASHOV'
password = 'KOSTYASHOV'
dsn = 'DWHPROD'

connection_string = f'oracle+oracledb://{username}:{password}@{dsn}'

# Create engine
engine = create_engine(connection_string)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

only_in_24_df.to_sql('al_babkina_kostyashov_nasty_reins12082024', engine, if_exists='append', index=False)
session.commit()
clear_output()
print("17/19: Lines that appeared in the last file are included")

ids_to_delete = list(change_in_24_df['CONTRACT_ID_AIS_OSAGO'])
total_cycles = len(ids_to_delete) // 1000 + 1

clear_output()
with session.begin():
    for n in range(total_cycles):
        chunk = ids_to_delete[n*1000:(n+1)*1000]
        ids_str = ",".join(f"'{id1}'" for id1 in chunk)
        delete_query = text(f"DELETE FROM al_babkina_kostyashov_nasty_reins12082024 WHERE CONTRACT_ID_AIS_OSAGO IN ({ids_str})")
        session.execute(delete_query)
        clear_output()
        display(f"18/19: Completed {n+1} cycles out of {total_cycles}")
    session.commit()

session.close()
print("Done")

chunksize = 30000
total_records = len(change_in_24_df)
total_cycles = -(-total_records // chunksize) 

clear_output()
with session.begin():
    for n in range(total_cycles):
        start = n * chunksize
        end = min((n + 1) * chunksize, total_records)
        chunk = change_in_24_df.iloc[start:end]
        chunk.to_sql('al_babkina_kostyashov_nasty_reins12082024', engine, if_exists='append', index=False)
        clear_output()
        print(f"19/19: Completed {end} cycles out of {total_records}")
    session.commit()

print("That's all!")
time.sleep(5)
