import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import imp
collect_current_congress = imp.load_source('module', './python/collect_current_congress.py')
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

### For testing
# collect_current_congress = imp.load_source('module', 'collect_current_congress.py')
# tally_toolkit = imp.load_source('module', 'tally_toolkit.py')

fromaddr = 'tallyscraper@gmail.com'
toaddrs = 'alexhubbard89@gmail.com'

msg = MIMEMultipart('alternative')
msg['From'] = "tallyscraper@gmail.com"
msg['To'] = "alexhubbard89@gmail.com"
good_collection = ''
bad_collection = ''
try:
	congress_data = collect_current_congress.bio_data_collector()
	to_collect_or_not_collect = collect_current_congress.bio_data_collector.collect_current_congress(congress_data)
	good_collection += """\n\tCurrent Congress: {}""".format(to_collect_or_not_collect)

except:
    bad_collection += """\n\tCurrent Congress"""

try:
	vc_data = tally_toolkit.vote_collector()
	tally_toolkit.vote_collector.daily_house_menu(vc_data)
	good_collection += """\n\tHouse vote menu: {}""".format(vc_data.to_db)
except:
	bad_collection += """\n\tHouse vote menu"""

try:
    vc_data = tally_toolkit.vote_collector()
    tally_toolkit.vote_collector.collect_missing_house_votes(vc_data)
    good_collection += """\n\tHouse votes collected: {}""".format(vc_data.duplicate_entries)
except:
    bad_collection += """\n\tHouse votes collector"""

try:
	print 'collect committee data'
	committee_data = tally_toolkit.committee_collector()
	tally_toolkit.committee_collector.get_committees(committee_data)
	tally_toolkit.committee_collector.get_subcommittees(committee_data)
	tally_toolkit.committee_collector.get_all_membership(committee_data)
	tally_toolkit.committee_collector.membership_to_sql(committee_data)
	good_collection += """\n\tHouse committee membership"""
except:
	bad_collection += """\n\tHouse committee membership"""

msg['Subject'] = "Data Collection Report"
body_msg = """Data Collection Report

Data colltion script(s) that worked: 
{}
\nData colltion script(s) that didn't work: 
{}""".format(good_collection, bad_collection)
body = MIMEText(body_msg)
msg.attach(body)

print body_msg

username = 'tallyscraper@gmail.com'
password = os.environ["tallyscraper_password"]
server = smtplib.SMTP_SSL('smtp.googlemail.com', 465)
server.login(username, password)
server.sendmail(fromaddr, toaddrs, msg.as_string())
server.quit()