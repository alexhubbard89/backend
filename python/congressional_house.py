import imp
reports_tools = imp.load_source('module', './python/reports_tools.py')

print "collect raw text house"
reports_tools.Congressional_report_collector.collect_missing_records('house')

print "clean text house"
reports_tools.Congressional_report_collector.clean_missing_text('house')
        
print 'done'