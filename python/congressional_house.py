import pandas as pd
import calendar
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

congressional_test = tally_toolkit.Congressional_report_collector()

for i in range(1, 6):
    max_day = calendar.monthrange(2017,i)[1]
    for j in range(1, max_day+1):
        print "2017-{}-{} House".format(i, j)
        congressional_test.year = 2017
        congressional_test.month = i
        congressional_test.day = j
        congressional_test.chamber = "house"
        congressional_test.table = "congressional_record_house"

        tally_toolkit.Congressional_report_collector.collect_and_house(congressional_test)
        tally_toolkit.Congressional_report_collector.to_sql(congressional_test)

print 'done collecting house text'