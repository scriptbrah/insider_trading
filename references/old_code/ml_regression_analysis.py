import pandas
import psycopg

with psycopg.connect("host='{}' port={} dbname='{}' user={} password={}".format("127.0.0.1", "5432", "insider_trading", "ethan", "ethan")) as conn:
    sql = "SELECT acquistion_or_disposition, transaction_date, reporting_owner, number_of_securities_transacted, number_of_securities_owned FROM insiders WHERE company = 'AAPL';"
    string_dat = pandas.read_sql_query(sql, conn)
    print(string_dat)