import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os
import us
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

contributions = tally_toolkit.Campaign_contributions()
contributions.data_set_url = 'ftp://ftp.fec.gov/FEC/2016/cn16.zip'
contributions.db_tbl = "fec_candidate_master"
contributions.unique_id = "cand_id"
tally_toolkit.Campaign_contributions.collect_data(contributions)

print 'done FEC Candidate Master File'