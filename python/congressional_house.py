import imp
tally_toolkit = imp.load_source('module', '/Users/Alexanderhubbard/Documents/projects/backend/python/tally_toolkit.py')

senate_collection = tally_toolkit.Congressional_report_collector()
tally_toolkit.Congressional_report_collector.collect_missing_reports(senate_collection, 'house')

transcript_cleaning = tally_toolkit.Congressional_report_collector()
tally_toolkit.Congressional_report_collector.clean_transcripts(transcript_cleaning, 'house')
        
print 'done'