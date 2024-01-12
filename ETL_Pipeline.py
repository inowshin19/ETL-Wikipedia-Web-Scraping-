# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 


# Code for ETL operations on Country-GDP data
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
#table_attribs_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
table_name = 'Largest_banks'
database_name = 'Banks.db'
csv_path = './Largest_banks_data.csv'
csv_path_exchange = './exchange_rate.csv'
log_file = 'code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ":" + message +'\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')

    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {
                "Name" : col[1].contents[2].text,
                "MC_USD_Billion" : float(col[2].contents[0].strip('\n'))
                }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    
    exchange_rate = pd.read_csv(csv_path)
    dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    

    df['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in df['MC_USD_Billion']]
    

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)
    

def run_query(query_statement, sql_connection):

    print("\n" + query_statement)
    run_query = pd.read_sql(query_statement, sql_connection)
    print(run_query)

    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
    

# Log Initialization

log_progress("Preliminaries complete. Initiating ETL process")

# Extraction Started
log_progress("Data extraction complete. Initiating Transformation process")
extracted_data = extract(url, table_attribs)

# Transformation
log_progress("Data transformation complete. Initiating Loading process")
transformed_data = transform(extracted_data, csv_path_exchange)

# load to csv
log_progress("Data saved to CSV file")
load_to_csv(transformed_data, csv_path)

# SQLITE connection

log_progress("SQL Connection initiated")

## Load to DB
sql_connection = sqlite3.connect('Banks.db')
load_to_db(transformed_data, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Run query
query_statement1 = 'SELECT * FROM Largest_banks'
query_statement2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query_statement3 = 'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement1, sql_connection)
run_query(query_statement2, sql_connection)
run_query(query_statement3, sql_connection)
log_progress("Process Complete")

## Close db connection
sql_connection.close()
log_progress("Server Connection closed")
