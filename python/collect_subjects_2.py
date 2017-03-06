import imp
import pandas as pd
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

leg_collection = tally_toolkit.collect_legislation()

for i in range(len(get_data)):
    if i % 1000 == 0:
        print 'collecting number {}'.format(i)
    try:
        leg_collection.url = get_data.loc[i, 'issue_link']
        leg_collection.bill_subjects_df = tally_toolkit.collect_legislation.bill_subjects(leg_collection)
        tally_toolkit.collect_legislation.policy_subjects_to_sql(leg_collection)
    except:
        print "did not work: {}".format(i)
        print leg_collection.url