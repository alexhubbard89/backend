import imp
reports_tools = imp.load_source('module', 'reports_tools.py')
reports_tools.Congressional_report_collector.collect_missing_records('senate')
        
print 'done'