import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

leg_collection = tally_toolkit.collect_legislation()
leg_collection.congress_search = 112
tally_toolkit.collect_legislation.legislation_info_by_congress(leg_collection)
tally_toolkit.collect_legislation.legislation_to_sql(leg_collection)

print 'done! 112'