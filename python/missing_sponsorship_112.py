import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os
import us
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

urlparse.uses_netloc.append("postgres")

def open_connection():
    url = urlparse.urlparse(os.environ["DATABASE_URL"])
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection


vc_data = tally_toolkit.vote_collector()

## Get all votes to collect
vc_data.house_vote_menu = pd.read_sql_query("""
SELECT * 
FROM house_vote_menu 
WHERE congress = {}
""".format(112), open_connection())

## Get unique where bioguide is none
membs = vc_data.house_votes[['member_full', 'state', 'year', 'bioguide_id']].drop_duplicates()
null_index = membs.loc[((membs['bioguide_id'] == 'None') | (membs['bioguide_id'].isnull()))].index

## for all nones find the bioguide id
for i in range(len(null_index)):
    i = null_index[i]
    potential = pd.read_sql_query("""
                SELECT DISTINCT bioguide_id, 
                year_elected,
                served_until
                FROM congress_bio
                WHERE lower(name) ilike '%' || '{}' || '%'
                AND state = '{}'
                AND chamber = 'house';
                """.format(membs.loc[i, 'member_full'].split(' (')[0].lower(),
                          str(us.states.lookup(membs.loc[i, 'state']))), open_connection())
    
    potential = potential.drop_duplicates(['bioguide_id'])
    potential.loc[potential['served_until'] == 'Present', 'served_until'] = 2017
    
    potential = potential.loc[((potential['year_elected'] <= vc_data.house_votes.loc[i, 'year']) &
                   (potential['served_until'] >= vc_data.house_votes.loc[i, 'year']))].reset_index(drop=True)
    
    if len(potential) == 1:
        membs.loc[i, 'bioguide_id'] = potential.loc[0, 'bioguide_id']
    else:
        membs.loc[i, 'bioguide_id'] = 'none {}'.format(membs.loc[i, 'member_full'])
        
    vc_data.house_votes = pd.merge(vc_data.house_votes.drop(['bioguide_id'], 1), 
            membs, how='left', on=['member_full', 'state', 'year'])

print 'add {} votes'.format(len(vc_data.house_votes))
## Put in databse
tally_toolkit.vote_collector.house_votes_into_sql(vc_data)

print 'done 112'