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


print 'collect house'
daily_collection = tally_toolkit.Congressional_report_collector()
daily_collection.chamber = "house"
daily_collection.table = "congressional_record_house"
tally_toolkit.Congressional_report_collector.collect_missing_reports(daily_collection)


print 'collect senate'
daily_collection = tally_toolkit.Congressional_report_collector()
daily_collection.chamber = "senate"
daily_collection.table = "congressional_record_senate"
tally_toolkit.Congressional_report_collector.collect_missing_reports(daily_collection)