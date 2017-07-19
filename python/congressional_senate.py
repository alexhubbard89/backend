import imp
reports_tools = imp.load_source('module', './python/reports_tools.py')

print "collect raw text senate"
reports_tools.Congressional_report_collector.collect_missing_records('senate')

print "clean text senate"
reports_tools.Congressional_report_collector.clean_missing_text('senate')
        
print 'done'