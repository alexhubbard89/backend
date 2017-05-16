import pandas as pd
import calendar
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

congressional_test = tally_toolkit.Congressional_report_collector()

dailY_collection = tally_toolkit.Congressional_report_collector()
dailY_collection.chamber = "house"
dailY_collection.table = "congressional_record_house"

print 'done collecting house text'