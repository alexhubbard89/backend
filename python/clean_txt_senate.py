import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os

urlparse.uses_netloc.append("postgres")
url = urlparse.urlparse(os.environ["DATABASE_URL"])

def open_connection():
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection


import imp
try:
    reports_tools = imp.load_source('module', './python/reports_tools.py')
except:
    reports_tools = imp.load_source('module', 'reports_tools.py')


data_counts = pd.read_sql_query("""
SELECT * FROM (
select date, count(date) from congressional_record_senate where date < '2017-01-01' 
and text != 'NO TEXT FOUND'group by date order by date asc)
AS data_counts
WHERE count > 1
""", open_connection())

data_counts_in = pd.read_sql_query("""
select date, count(date) from(
select * from congressional_record_transcripts 
where date < '2017-01-01'
and chamber = 'senate'
) as counts
group by date order by date asc
""", open_connection())

data_counts = pd.DataFrame(list(set(data_counts['date']) - set(data_counts_in['date'])), columns=['date']).sort_values(['date'])



for date in data_counts['date']:
    print date
    reports_tools.Congressional_report_collector.daily_text_clean(chamber='senate', date=date)

print 'done!'