import imp
import pandas as pd
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')
import psycopg2
import urlparse
import os
import sys


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


leg_collection = tally_toolkit.collect_legislation()

get_data = pd.read_sql_query("""SELECT * FROM all_legislation_2
    where policy_area = 'collect';""", open_connection())

for i in range(len(get_data)):
    if i % 500 == 0:
        print 'collecting number {}'.format(i)
    try:
        leg_collection.url = get_data.loc[i, 'issue_link']
        leg_collection.bill_subjects_df = tally_toolkit.collect_legislation.bill_subjects(leg_collection)
        tally_toolkit.collect_legislation.policy_subjects_to_sql(leg_collection)
    except:
        print "did not work: {}".format(i)
        print leg_collection.url