import os
import pandas as pd
import imp
try:
    collect_current_congress = imp.load_source('module', './python/collect_current_congress.py')
    tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')
    reports_tools = imp.load_source('module', './python/reports_tools.py')

except:
    # # For testing
    collect_current_congress = imp.load_source('module', 'collect_current_congress.py')
    tally_toolkit = imp.load_source('module', 'tally_toolkit.py')
    reports_tools = imp.load_source('module', 'reports_tools.py')

# print "daily report collection"
# try:
#     print "for house"
#     ## New recoreds
#     reports_tools.Congressional_report_collector.collect_missing_records('house')
#     good_collection += """\n\tCongressional reports House"""
# except:
#     bad_collection += """\n\tCongressional reports House"""

# try:
#     print "for senate"
#     reports_tools.Congressional_report_collector.collect_missing_records('senate')
#     good_collection += """\n\tCongressional reports Senate"""
# except:
#     bad_collection += """\n\tCongressional reports Senate"""

print "clean the transcripts"
try:
    print 'house'
    reports_tools.Congressional_report_collector.clean_missing_text('house')
    good_collection += """\n\tCongressional reports cleaning House"""
except:
    bad_collection += """\n\tCongressional reports cleaning House"""

try:
    print 'senate'
    reports_tools.Congressional_report_collector.clean_missing_text('senate')
    good_collection += """\n\tCongressional reports cleaning Senate"""
except:
    bad_collection += """\n\tCongressional reports cleaning Senate"""