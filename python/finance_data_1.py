import pandas as pd
import numpy as np
import psycopg2
import urlparse
import os
import us
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

contributions = tally_toolkit.Campaign_contributions()
contributions.data_set_url = 'ftp://ftp.fec.gov/FEC/2018/cm18.zip'
contributions.db_tbl = "fec_committee_master"
contributions.unique_id = "cmte_id"
tally_toolkit.Campaign_contributions.collect_data(contributions)

print 'done FEC Committee Master File'