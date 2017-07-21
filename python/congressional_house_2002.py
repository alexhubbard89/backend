import pandas as pd
import numpy as np
import requests
import os
import calendar

import imp
reports_tools = imp.load_source('module', './python/reports_tools.py')

print "collet list of dates"
date_list = []
for year in range(2002, 2003):
    ## Collect array of dats
    for month in range(1, 13):
        num_days = calendar.monthrange(year, month)[1]
        for day in range(1, num_days+1):
            search_date = '{}-{}-{}'.format(year, '{}'.format(month).zfill(2), '{}'.format(day).zfill(2))
            ## Convert to date time when saving to array
            date_list.append(pd.to_datetime(search_date))

print "look through days to get raw text"
## make object
collect_missing = reports_tools.Congressional_report_collector()
chamber = 'house'
for date in date_list:
    print date
    record_exists = reports_tools.Congressional_report_collector.collect_subjets_and_links(collect_missing, year=date.year, 
                                                  month=date.month, day=date.day, chamber=chamber)
    if record_exists == True:
        for i in range(len(collect_missing.subjects)):
            print i
            print collect_missing.subjects[i]
            if i > 0:
                reports_tools.Congressional_report_collector.collect_text(collect_missing, index=i, date=date, chamber=chamber)
            elif i == 0:
                reports_tools.Congressional_report_collector.collect_text(collect_missing, index=i, date=date, chamber=chamber, first=True)

        reports_tools.Congressional_report_collector.record_to_sql(collect_missing, "congressional_record_{}".format(chamber), uid=['index'])
    else:
        print "no data"

        
print 'done'