import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import create_engine
import pandas as pd
import imp
collect_current_congress = imp.load_source('module', './python/collect_current_congress.py')
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')
reports_tools = imp.load_source('module', './python/reports_tools.py')

# # # For testing
# collect_current_congress = imp.load_source('module', 'collect_current_congress.py')
# tally_toolkit = imp.load_source('module', 'tally_toolkit.py')
# reports_tools = imp.load_source('module', 'reports_tools.py')

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
# try:
leg_collection = tally_toolkit.collect_legislation()
leg_collection.congress_search = tally_toolkit.current_congress_num()
tally_toolkit.collect_legislation.legislation_info_by_congress(leg_collection)
tally_toolkit.collect_legislation.legislation_to_sql(leg_collection)
good_collection += """\n\tLegislation collected for {} congress - New data: {}, Update data: {}""".format(
    leg_collection.congress_search,
    leg_collection.new_data,
    leg_collection.updated_data)
# except:
#     bad_collection += """\n\tLegislation collector"""

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
to_classify = ['women and minority rights', 'abortion', 'obamacare', 'lgbt rights']

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
to_classify = ['taxes', 'stimulus or market led recovery']

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

## Domestic issues
to_classify = ['environmental protection', 'second amendment']

ideology_data = tally_toolkit.Ideology()
ideology_data.ideology_type = 'domestic'
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

## International issues
to_classify = ['immigration', 'homeland security']

ideology_data = tally_toolkit.Ideology()
ideology_data.ideology_type = 'international'
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
    rep_grades.congress_grades.loc[:, 'congress'] = rep_grades.congress
    tally_toolkit.Grade_reps.grades_to_sql(rep_grades)
    good_collection += """\n\tGrading"""
except:
    bad_collection += """\n\tGrading"""

## Make tables with most recent stats
print "Get most recent stats"
engine = create_engine(os.environ['DATABASE_URL'])
rep_perfomance = tally_toolkit.Performance()
tally_toolkit.Performance.current_congress_num(rep_perfomance)
master_attend = pd.DataFrame()
master_participation = pd.DataFrame()
master_sponsor = pd.DataFrame()
try:
    chambers = ['house', 'senate']
    for chamber in chambers:
        rep_perfomance.chamber = chamber
        
        ## Attendance
        tally_toolkit.Performance.num_days_voted_all(rep_perfomance)
        rep_perfomance.days_voted.loc[:, 'chamber'] = chamber
        master_attend = master_attend.append(rep_perfomance.days_voted)
        
        ## Participation
        tally_toolkit.Performance.num_votes_all(rep_perfomance)
        rep_perfomance.rep_votes_metrics.loc[:, 'chamber'] = chamber
        master_participation = master_participation.append(rep_perfomance.rep_votes_metrics)
        
        ## Sponsor
        tally_toolkit.Performance.num_sponsored_all(rep_perfomance)
        rep_perfomance.rep_sponsor_metrics.loc[:, 'chamber'] = chamber
        master_sponsor = master_sponsor.append(rep_perfomance.rep_sponsor_metrics)

    master_attend.to_sql("current_attendance", engine, if_exists='replace', index=False)
    master_participation.to_sql("current_participation", engine, if_exists='replace', index=False)
    master_sponsor.to_sql("current_sponsor", engine, if_exists='replace', index=False)
    good_collection += """\n\tMost Recent Performance Stats"""
except:
    bad_collection += """\n\tMost Recent Performance Stats"""

print "daily report collection"
try:
    print "for house"
    ## New recoreds
    reports_tools.Congressional_report_collector.collect_missing_records('house')
    good_collection += """\n\tCongressional reports House"""

    ## Check the null records
    reports_tools.Congressional_report_collector.collect_missing_records('house', type='null')
    good_collection += """\n\tCongressional reports House"""
except:
    bad_collection += """\n\tCongressional reports House"""

try:
    print "for senate"
    reports_tools.Congressional_report_collector.collect_missing_records('senate')
    good_collection += """\n\tCongressional reports Senate"""

    ## Check the null records
    reports_tools.Congressional_report_collector.collect_missing_records('senate', type='null')
    good_collection += """\n\tCongressional reports Senate"""
except:
    bad_collection += """\n\tCongressional reports Senate"""

print "clean the transcripts"
try:
    print 'house'
    reports_tools.Congressional_report_collector.clean_missing_text('house')
    good_collection += """\n\tCongressional reports cleaning House"""
except:
    bad_collection += """\n\tCongressional reports cleaning House"""

try:
    print 'senate'
    reports_tools.Congressional_report_collector.clean_missing_text('senate')
    good_collection += """\n\tCongressional reports cleaning Senate"""
except:
    bad_collection += """\n\tCongressional reports cleaning Senate"""



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