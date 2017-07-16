import imp
reports_tools = imp.load_source('module', 'reports_tools.py')
reports_tools.Congressional_report_collector.clean_missing_text('senate')
        
print 'done'