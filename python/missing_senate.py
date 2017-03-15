import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

urlparse.uses_netloc.append("postgres")

def open_connection():
    url = urlparse.urlparse(os.environ["HEROKU_POSTGRESQL_BROWN_URL"])
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection

senate_data = tally_toolkit.Senate_colleciton()

for congress in range(101, 115):
    for session in range(1,3):
        print congress
        print session
        x = pd.read_sql_query("""
        SELECT * FROM senate_vote_menu
        where congress = {}
        and session = {}
        """.format(congress, session), open_connection())
        
        print 'collect {} missing senate votes!'.format(len(x))
        for i in range(len(x)):
            senate_data.roll_search = x.loc[i, 'vote_number']
            senate_data.congress_search = x.loc[i, 'congress']
            senate_data.session_search = x.loc[i, 'session']
            senate_data.date_search = x.loc[i, 'vote_date']
            senate_data.roll_id = x.loc[i, 'roll_id']
            ## Find data
            try:
                tally_toolkit.Senate_colleciton.get_senate_votes(senate_data)
                ## House
                tally_toolkit.Senate_colleciton.votes_to_sql(senate_data)
            except:
                print 'did not work {}'.format(x.loc[i, 'roll_id'])

print 'all done!'