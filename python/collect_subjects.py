import imp
import pandas as pd
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

leg_collection = tally_toolkit.collect_legislation()
print 'start loop'
for i in range(101, 116):
    leg_collection.congress_search = i
    print "looking for congress: {}".format(i)
    tally_toolkit.collect_legislation.legislation_info_by_congress(leg_collection)
    print 'putting {} rows into sql'.format(len(leg_collection.legislation_by_congress))
    tally_toolkit.collect_legislation.legislation_to_sql(leg_collection)

print 'done bitches!'