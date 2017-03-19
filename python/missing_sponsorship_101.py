import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

sponsorship_data = tally_toolkit.sponsorship_collection()
sponsorship_data.congress_search = 101

tally_toolkit.sponsorship_collection.collect_sponsorship(sponsorship_data)