import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os
import us
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

contributions = tally_toolkit.Campaign_contributions()
contributions.data_set_url = 'ftp://ftp.fec.gov/FEC/2016/indiv16.zip'
contributions.db_tbl = "fec_individuals_contributions_2016"
contributions.unique_id = "sub_id"
tally_toolkit.Campaign_contributions.collect_data(contributions)

print 'done FEC Contributions by Individuals'