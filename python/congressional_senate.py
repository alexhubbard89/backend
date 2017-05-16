import pandas as pd
import calendar
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

congressional_test = tally_toolkit.Congressional_report_collector()

dailY_collection.chamber = "senate"
dailY_collection.table = "congressional_record_senate"
tally_toolkit.Congressional_report_collector.collect_missing_reports(dailY_collection)

print 'done collecting senate text'