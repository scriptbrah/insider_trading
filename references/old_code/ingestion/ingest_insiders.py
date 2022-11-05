from tqdm import tqdm 
import pandas as pd
import os
import json
from bs4 import BeautifulSoup
import requests
import psycopg

"""
Database connection info here.
"""
connection = psycopg.connect(user="ethan", password="ethan", host="127.0.0.1", port="5432", dbname="insider_trading")

"""
Looks up the Edgar CIK number using the ticker_and_edgar_cik.csv file.
"""
def symbol_to_cik(symbol):
    ticker_cik = pd.read_csv('ticker_and_edgar_cik.csv', delimiter=',')
    sym_cik = ticker_cik.copy(deep=True)
    sym_cik.set_index('Ticker', inplace=True)
    cik = sym_cik.loc[symbol.lower(), 'CIK']
    return cik

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

"""
Turns a URL into a Soup object.
"""
def to_soup(url):
    url_response = requests.get(url)
    webpage = url_response.content
    soup = BeautifulSoup(webpage, 'html.parser')
    return soup

"""
Uses the connection object to insert a new row into the insiders table of the insider_trading database. Ignores duplicates.
"""
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
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT DO NOTHING"""
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
        cursor.close()
    except Exception as e:
        print(e)

"""
Gathers info from the EDGAR database pages. Calls the append_table() function.
"""
def insider_trading_all(symbol_list, end_date):
    with tqdm(total = len(symbol_list)) as progress_bar:
        for symbol in symbol_list:
            try:
                cik_identifier = symbol_to_cik(symbol)
                page = 0
                urls = ['https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK='+str(cik_identifier)+'&type=&dateb=&owner=include&start='+str(page*80)]
                for url in urls:
                    print("fetching data from " + url)
                    soup = to_soup(url)
                    transaction_report = soup.find('table', {'id':'transaction-report'})

                    t_chil = [i for i in transaction_report.children]
                    t_cont = [i for i in t_chil if i != '\n']

                    data_rough = [i for lst in t_cont[1:] for i in lst.get_text().split('\n') if i != '' ]
                    data = [data_rough[i:i+12] for i in range(0,len(data_rough), 12)]
                    last_line = data[-1]
                    for row in data:
                        if (end_date > row[1]):
                            break
                        else:
                            if (row != last_line):
                                append_table(row, connection, symbol)
                            else:
                                page += 1
                                urls.append('https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK='+str(cik_identifier)+'&type=&dateb=&owner=include&start='+str(page*80))
                os.system('cls' if os.name=='nt' else 'clear')
                progress_bar.update(1)
            except:
                print("failed to find " + symbol + " resource")
                os.system('cls' if os.name=='nt' else 'clear')
                progress_bar.update(1)
    connection.close()

if __name__ == "__main__":
    symbols = digest()["stocks"]
    date = digest()["last_update"]
    insider_trading_all(symbols, date)