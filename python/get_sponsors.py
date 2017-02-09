import pandas as pd
import numpy as np
import json
import requests
from bs4 import BeautifulSoup
from pandas.io.json import json_normalize
import psycopg2
import urlparse
import us
from xmljson import badgerfish as bf
from xml.etree.ElementTree import fromstring
from pandas.io.json import json_normalize

urlparse.uses_netloc.append("postgres")
url = urlparse.urlparse(os.environ["HEROKU_POSTGRESQL_BROWN_URL"])
    
def open_connection():
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection

def get_sponsor_data(url):
    r = requests.get('{}/cosponsors'.format(url))
    page = BeautifulSoup(r.content, "lxml")
    sponsor = page.find_all('div', 
                             class_=
                             'overview_wrapper bill')[0].find_all(
        'tr')[0].find_all('a')[0].get('href').split('/')[-1]
    sponsors_df = pd.DataFrame([url, sponsor]).transpose()
    sponsors_df.columns = ['url', 'bioguide_id']
    
    try:
        ## If there are cosponsors
        cosponsors_id = []
        cosponsors_name = []
        cosponsors_date = []

        loop_max = len(page.find_all('div', class_='col2_lg basic-search-results nav-on')[0].find_all('td', class_='date'))
        date_split = page.find_all('div', class_='col2_lg basic-search-results nav-on')[0]

        for i in range(1, loop_max):
            cosponsors_id.append(str(date_split.find_all('a', target='_blank')[i].get('href').split('/')[-1]))
            cosponsors_name.append(str(str(date_split.find_all('a', target='_blank')[i].text)))
            cosponsors_date.append(str(date_split.find_all('td', class_='date')[i].text))
    except:
        "either nothing or something weird"

    try:
        cosponsor_df = pd.DataFrame([cosponsors_id, cosponsors_name, cosponsors_date]).transpose()
        cosponsor_df.columns = ['bioguide_id', 'member_full', 'date_cosponsored']

        sponsors_df.loc[:, 'cosponsor_bioguide_id'] = pd.Series(list(cosponsor_df['bioguide_id']))
        sponsors_df.set_value(0, 'cosponsor_bioguide_id', (list(cosponsor_df['bioguide_id'])))
        
        sponsors_df.loc[:, 'cosponsor_member_full'] = pd.Series(list(cosponsor_df['member_full']))
        sponsors_df.set_value(0, 'cosponsor_member_full', (list(cosponsor_df['member_full'])))
        
        sponsors_df.loc[:, 'date_cosponsored'] = pd.Series(list(cosponsor_df['date_cosponsored']))
        sponsors_df.set_value(0, 'date_cosponsored', (list(cosponsor_df['date_cosponsored'])))
        
        ## Remove single quotes.
        ## I tried in single data collection but they still showed up
        sponsors_df.loc[:, 'cosponsor_bioguide_id'] = sponsors_df.loc[:, 'cosponsor_bioguide_id'].apply(lambda x: str(x).replace("'", ""))
        sponsors_df.loc[:, 'cosponsor_member_full'] = sponsors_df.loc[:, 'cosponsor_member_full'].apply(lambda x: str(x).replace("'", ""))
        sponsors_df.loc[:, 'date_cosponsored'] = sponsors_df.loc[:, 'date_cosponsored'].apply(lambda x: str(x).replace("'", ""))
    except:
        ## No cosponsors
        sponsors_df.loc[0, 'cosponsor_bioguide_id'] = None
        sponsors_df.loc[0, 'cosponsor_member_full'] = None
        sponsors_df.loc[0, 'date_cosponsored'] = None

    
    return sponsors_df


def sponsor_to_sql(df):
    
    connection = open_connection()
    cursor = connection.cursor()
    
    ## Put each row into sql
    for i in range(len(df)):
        print i
        x = list(df.loc[i,])

        for p in [x]:
            format_str = """
            INSERT INTO bill_sponsors (
            url, 
            bioguide_id, 
            cosponsor_bioguide_id,
            cosponsor_member_full,
            date_cosponsored)
            VALUES ('{url}', '{bioguide_id}', '{cosponsor_bioguide_id}',
                    '{cosponsor_member_full}', '{date_cosponsored}');"""


        sql_command = format_str.format(url=p[0], bioguide_id=p[1], cosponsor_bioguide_id=p[2],
                                       cosponsor_member_full=p[3], date_cosponsored=p[4])
        ## Commit to sql
        try:
            cursor.execute(sql_command)
            connection.commit()
        except:
            ## Update what I got
            connection.rollback()
            sql_command = """UPDATE bill_sponsors 
            SET
            bioguide_id = '{}',
            cosponsor_bioguide_id = '{}',
            cosponsor_member_full = '{}',
            date_cosponsored = '{}'
            WHERE url = '{}';""".format(
            df.loc[i, 'bioguide_id'],
            df.loc[i, 'cosponsor_bioguide_id'],
            df.loc[i, 'cosponsor_member_full'],
            df.loc[i, 'date_cosponsored'],
            df.loc[i, 'url'])    
            cursor.execute(sql_command)
            connection.commit()

    connection.close()

## collect all data
print 'collect it all!'

vote_menu = pd.read_sql_query("""SELECT * FROM house_vote_menu;""", open_connection())
unique_urls = np.unique(vote_menu.loc[:, 'issue_link'])

master_sponsor = pd.DataFrame()

print 'collect data'
for i in range(0, 50):
    if unique_urls[i] != ' ':
        url = unique_urls[i]
        print url
        x = get_sponsor_data(url)
        master_sponsor = master_sponsor.append(x)
        
master_sponsor = master_sponsor.reset_index(drop=True)

print 'into sql'
sponsor_to_sql(master_sponsor)