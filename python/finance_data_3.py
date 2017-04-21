import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os
import us
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

contributions = tally_toolkit.Campaign_contributions()
contributions.data_set_url = 'ftp://ftp.fec.gov/FEC/2016/ccl16.zip'
contributions.db_tbl = "fec_candidate_committee_link"
contributions.unique_id = "linkage_id"
tally_toolkit.Campaign_contributions.collect_data(contributions)

print 'done FEC Candidate Committee Linkage File'