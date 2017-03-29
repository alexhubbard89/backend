import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import imp
collect_current_congress = imp.load_source('module', './python/collect_current_congress.py')
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

# # # For testing
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
	print 'collect hosue committee data'
	committee_data = tally_toolkit.committee_collector()
	tally_toolkit.committee_collector.get_committees(committee_data)
	tally_toolkit.committee_collector.get_subcommittees(committee_data)
	tally_toolkit.committee_collector.get_all_membership(committee_data)
	tally_toolkit.committee_collector.membership_to_sql(committee_data)
	good_collection += """\n\tHouse committee membership"""
except:
	bad_collection += """\n\tHouse committee membership"""

try:
    print 'collect senate committee data'
    senate_committee_data = tally_toolkit.committee_collector()
    tally_toolkit.committee_collector.get_senate_committees(senate_committee_data)
    tally_toolkit.committee_collector.collect_senate_committee_membership(senate_committee_data)
    tally_toolkit.committee_collector.senate_membership_to_sql(senate_committee_data)
    good_collection += """\n\tSenate committee membership"""
except:
    bad_collection += """\n\tSenate committee membership"""

print 'collect all legislation from this congress'
"""This is different from vote menu data. VM data is only roll call
votes. This method collects all legislations from the current congress.
This is necessary for scoring.
congress_data.current_congress was found when I got current congress
data. The attribute still exists so use it.
"""
try:
    leg_collection = tally_toolkit.collect_legislation()
    leg_collection.congress_search = congress_data.current_congress
    tally_toolkit.collect_legislation.legislation_info_by_congress(leg_collection)
    tally_toolkit.collect_legislation.legislation_to_sql(leg_collection)
    good_collection += """\n\tLegislation collected for {} congress - New data: {}, Update data: {}""".format(
        leg_collection.congress_search,
        leg_collection.new_data,
        leg_collection.updated_data)
except:
    bad_collection += """\n\tLegislation collector"""

try:
    leg_collection = tally_toolkit.collect_legislation()
    tally_toolkit.collect_legislation.daily_subject_collection(leg_collection)
    good_collection += """\n\tSubjects collector - New data: {}""".format(leg_collection.new_data)
except:
    bad_collection += """\n\tSubjects collector"""

try:
    sponsorship_data = tally_toolkit.sponsorship_collection()
    sponsorship_data.congress_search = congress_data.current_congress
    tally_toolkit.sponsorship_collection.collect_sponsorship(sponsorship_data)
    sponsorship_data.new_data
    sponsorship_data.updated_data
    good_collection += """\n\tSponsorship collected - New data: {}, Update data: {}""".format(sponsorship_data.new_data,
        sponsorship_data.updated_data)
except:
    bad_collection += """\n\tSponsorship collector"""

print 'Collect senate vote menu'
try:
    senate_data = tally_toolkit.Senate_colleciton()
    tally_toolkit.Senate_colleciton.daily_senate_menu(senate_data)
    good_collection += """\n\tSenate Vote Menu - {}""".format(senate_data.to_db)
except:
    bad_collection += """\n\tSenate Vote Menu"""


print 'Collect new senate votes'
try:
    senate_data = tally_toolkit.Senate_colleciton()
    tally_toolkit.Senate_colleciton.daily_senate_menu(senate_data)
    tally_toolkit.Senate_colleciton.daily_senate_votes(senate_data)
    good_collection += """\n\tSenate Votes"""
except:
    bad_collection += """\n\tSenate Votes"""

## Social issues
to_classify = ['women and minority rights', 'immigration', 'abortion', 'environmental protection', 'second amendment', 'obamacare', 'lgbt rights']

ideology_data = tally_toolkit.Ideology()
ideology_data.ideology_type = 'social'
for ideology_category in to_classify:
    try: 
        print 'Classify ideology - {}'.format(ideology_category)
        ideology_data.ideology = ideology_category
        tally_toolkit.Ideology.make_tally_score(ideology_data)
        good_collection += """\n\tIdeology collection - {}""".format(ideology_category)
        print 'Put ideology to sql - {}'.format(ideology_category)
        try:
            tally_toolkit.Ideology.put_finalized_ideology_stats_into_sql(ideology_data)
            good_collection += """\n\tIdeology to sql - {}""".format(ideology_category)
        except:
            bad_collection += """\n\tIdeology to sql - {}""".format(ideology_category)

    except:
        bad_collection += """\n\tIdeology colection - {}""".format(ideology_category)


## Economic issues
to_classify = ['taxes']

ideology_data = tally_toolkit.Ideology()
ideology_data.ideology_type = 'economy'
for ideology_category in to_classify:
    try: 
        print 'Classify ideology - {}'.format(ideology_category)
        ideology_data.ideology = ideology_category
        tally_toolkit.Ideology.make_tally_score(ideology_data)
        good_collection += """\n\tIdeology collection - {}""".format(ideology_category)
        print 'Put ideology to sql - {}'.format(ideology_category)
        try:
            tally_toolkit.Ideology.put_finalized_ideology_stats_into_sql(ideology_data)
            good_collection += """\n\tIdeology to sql - {}""".format(ideology_category)
        except:
            bad_collection += """\n\tIdeology to sql - {}""".format(ideology_category)

    except:
        bad_collection += """\n\tIdeology colection - {}""".format(ideology_category)

print 'Conduct the daily grading ritual'

try:
    rep_grades = tally_toolkit.Grade_reps()
    tally_toolkit.Grade_reps.current_congress_num(rep_grades)
    tally_toolkit.Grade_reps.total_grade_calc(rep_grades)
    tally_toolkit.Grade_reps.total_grade_calc(rep_grades)
    rep_grades.congress_grades.loc[:, 'congress'] = rep_grades.congress
    tally_toolkit.Grade_reps.grades_to_sql(rep_grades)
    good_collection += """\n\tGrading"""
except:
    bad_collection += """\n\tGrading"""


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