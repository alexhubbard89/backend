import psycopg2
import urlparse
import os
import pandas as pd
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

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

speaking_house = tally_toolkit.Congressional_report_collector()

df = pd.read_sql_query("""
SELECT * FROM congressional_record_house
;
""", open_connection())

for i in df.index:
    if df.loc[i, 'pdf_str'] != 'nan':
        print 'has: {}'.format(i)
        by_section = df.loc[i, 'pdf_str'][28:].split(' \n\nf ')
        master_df = pd.DataFrame()

        for section in by_section:
            speaking_house.text = section
            tally_toolkit.Congressional_report_collector.get_sub_clean_text(speaking_house)
            clean_df = tally_toolkit.Congressional_report_collector.whatd_they_say(speaking_house, 'house')
            if len(clean_df) > 0:
                master_df = master_df.append(clean_df).reset_index(drop=True)

        ## Add column for date and drop speaker trigger
        master_df.loc[:, 'date'] = df.loc[i, 'date']
        master_df = master_df.drop(['speaker_trigger'], 1)
        master_df.loc[:, 'chamber'] = 'house'
        
        speaking_house.df = master_df
        tally_toolkit.Congressional_report_collector.transcript_to_sql(speaking_house)
    else:
        print 'not: {}'.format(i)
        
print 'done'