from token import RARROW
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from IPython.display import clear_output
from tqdm import tqdm 
import pandas_datareader.data as web
import psycopg
import json
from collections import namedtuple
from json import JSONEncoder
import os

input_data_path = '/home/ethan/Desktop/code/insider_trading/InsiderTrading/ticker_and_edgar_cik.csv'
connection = psycopg.connect(user="ethan", password="ethan", host="127.0.0.1", port="5432", dbname="insider_trading")

ticker_cik = pd.read_csv(input_data_path, delimiter=',')
sym_cik = ticker_cik.copy(deep=True)
sym_cik.set_index('Ticker', inplace=True)

"""
Ingests data from parameters.json. Data should follow format YYYY-MM-DD.
"""
def digest():
    try:
        with open('parameters.json', 'r') as file:
            data = json.load(file)
            return data
    except:
        print("coud not load parameters.json")
        os.exit()

#Looks up Edgar CIK Number
def symbol_to_cik(symbols):
    new_symbols = [i.lower() for i in symbols]
    cik = [sym_cik.loc[i, 'CIK'] for i in new_symbols]
    return cik
#Turns URL into Soup object
def to_soup(url):
    url_response = requests.get(url)
    webpage = url_response.content
    soup = BeautifulSoup(webpage, 'html.parser')
    return soup

def append_table(single_insider, connection, company):
    try:
        cursor = connection.cursor()
        postgres_insert_query = """INSERT INTO insiders (
                                        acquistion_or_disposition, 
                                        transaction_date, 
                                        deemed_execution_date, 
                                        reporting_owner, form, 
                                        transaction_type, 
                                        direct_or_indirect_ownership, 
                                        number_of_securities_transacted, 
                                        number_of_securities_owned, 
                                        line_number, 
                                        owner_cik, 
                                        security_name, 
                                        company) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        record_to_insert = (single_insider[0], 
                            single_insider[1], 
                            single_insider[2], 
                            single_insider[3], 
                            single_insider[4], 
                            single_insider[5], 
                            single_insider[6], 
                            single_insider[7], 
                            single_insider[8], 
                            single_insider[9], 
                            single_insider[10], 
                            single_insider[11], 
                            company)
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
        count = cursor.rowcount
        cursor.close()
    except Exception as e:
        pass

#Picks up the Insider Trades data
def insider_trading_all(symbol_list, end_date):
    symbols = symbol_list
    end = end_date
    with tqdm(total = len(symbols)) as pbar:
        for i in range(len(symbols)):
            try:
                lst = [symbols[i]]
                cik = symbol_to_cik(lst)
                page = 0
                beg_url = 'https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK='+str(cik[0])+'&type=&dateb=&owner=include&start='+str(page*80)
                urls = [beg_url]
                for url in urls:
                    soup = to_soup(url)
                    transaction_report = soup.find('table', {'id':'transaction-report'})

                    t_chil = [i for i in transaction_report.children]
                    t_cont = [i for i in t_chil if i != '\n']

                    data_rough = [i for lst in t_cont[1:] for i in lst.get_text().split('\n') if i != '' ]
                    data = [data_rough[i:i+12] for i in range(0,len(data_rough), 12)]
                    last_line = data[-1]
                    for i in data:
                        if (end > i[1]):
                            break
                        else:
                            if (i != last_line):
                                append_table(i, connection, lst[0])
                            else:
                                page += 1
                                urls.append('https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK='+str(cik[0])+'&type=&dateb=&owner=include&start='+str(page*80))
                pbar.update(1)
            except Exception as e:
                print("***unexpected error***")
                print(e)
                pbar.update(1)
                exit()
    
    clear_output(wait=True)
    print('SCAN COMPLETE for period beginning: ' + end)

if __name__ == "__main__":
    symbols = digest()["stocks"]
    date = digest()["last_update"]
    #insider_trading_all(symbols, date)

    connection.close()