import imp
tally_toolkit = imp.load_source('module', '/Users/Alexanderhubbard/Documents/projects/backend/python/tally_toolkit.py')

sponsorship_data = tally_toolkit.sponsorship_collection()
sponsorship_data.congress_search = 115

tally_toolkit.sponsorship_collection.collect_sponsorship(sponsorship_data)