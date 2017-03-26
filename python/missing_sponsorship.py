import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os

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
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')


vc_data = tally_toolkit.vote_collector()
for i in range(101, 115):
    vc_data.house_vote_menu = pd.read_sql_query("""
    SELECT * 
    FROM house_vote_menu 
    WHERE congress = {}
    """.format(i), open_connection())
    
    ## Collect missing roll call votes
    tally_toolkit.vote_collector.get_congress_votes(vc_data)

    print 'add {} votes'.format(len(vc_data.house_votes))
    ## Put in databse
    tally_toolkit.vote_collector.house_votes_into_sql(vc_data)