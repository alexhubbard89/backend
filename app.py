from __future__ import division
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash, jsonify, Response
from flask_cors import CORS, cross_origin
import json
import pandas as pd
import numpy as np
import requests
import os
import sys
import json
import logging
import imp
tally_toolkit = imp.load_source('module', './python/tally_toolkit.py')

app = Flask(__name__)
CORS(app)
app.config.from_object(__name__)

app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.ERROR)


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('landing.html')

## Login testing
@app.route("/login", methods=["POST"])
def login():
    user = tally_toolkit.user_info()
    try:
        data = json.loads(request.data.decode())
        user.email = tally_toolkit.sanitize(data['email'])
        user.password = tally_toolkit.sanitize(data['password'])
    except:
        user.email = tally_toolkit.sanitize(request.form['email'])
        user.password = tally_toolkit.sanitize(request.form['password'])
    matched_credentials = tally_toolkit.user_info.search_user(user)
    if matched_credentials == True:
        user_data = tally_toolkit.user_info.get_user_data(user)
        print user_data
        # return jsonify(user_data.to_dict(orient='records'))
        return jsonify(user_data.to_dict(orient='records')[0])
    else:
        error = "Wrong user name or password"
        print error
        return jsonify(results=None)

## Create New User
@app.route("/new_user", methods=["POST"])
def create_user():
    user = tally_toolkit.user_info()
    try:
        print 'trying first way'
        data = json.loads(request.data.decode())
        user.email = tally_toolkit.sanitize(data['email'])
        user.password = tally_toolkit.sanitize(data['password'])
        user.first_name = tally_toolkit.sanitize(data['first_name'])
        user.last_name = tally_toolkit.sanitize(data['last_name'])
        # user.gender = tally_toolkit.sanitize(data['gender'])
        # user.dob = tally_toolkit.sanitize(data['dob'])
        user.street = tally_toolkit.sanitize(data['street'].replace("'", ''))
        user.zip_code = tally_toolkit.sanitize(data['zip_code'])

    except:
        print 'trying second way'
        user.email = tally_toolkit.sanitize(request.form['email'])
        user.password = tally_toolkit.sanitize(request.form['password'])
        user.first_name = tally_toolkit.sanitize(request.form['first_name'])
        user.last_name = tally_toolkit.sanitize(request.form['last_name'])
        # user.gender = tally_toolkit.sanitize(request.form['gender'])
        # user.dob = tally_toolkit.sanitize(request.form['dob'])
        user.street = tally_toolkit.sanitize(request.form['street'].replace("'", ''))
        user.zip_code = tally_toolkit.sanitize(request.form['zip_code'])

    #### Validate

    user.gender = "None"
    user.dob = '1970-01-01'

    ## DOB
    try: 
        print pd.to_datetime(user.dob)
    except: 
        return jsonify(results="Incorrect date format, should be YYYY-MM-DD")

    ## Address
    """Check if something bad was returned. If not keep moving."""

    tally_toolkit.user_info.check_address(user)
    if user.address_check == "Bad address":
        return jsonify(results="Bad address")
    elif user.address_check == "Bad request":
        return jsonify(results="Bad request")

    user.user_df = tally_toolkit.user_info.create_user_params(user)
    user_made = tally_toolkit.user_info.user_info_to_sql(user)

    if user_made == True:
        return jsonify(results=True)
    elif user_made == False:
        return jsonify(results="oops! That user name already exists.")

## Pass back congress bios
@app.route("/congress_bio", methods=["POST"])
def congress_bio():
    user = tally_toolkit.user_info()
    try:
        data = json.loads(request.data.decode())
        user.district = data['district']
        user.state_long = data['state_long']
    except:
        user.district = request.form['district']
        user.state_long = request.form['state_long']
    congress_bio = tally_toolkit.user_info.get_congress_bio(user)
    if len(congress_bio) > 0:
        return jsonify(results=congress_bio.to_dict(orient='records'))
    else:
        return jsonify(results=False)

## Pass Committee Membership
@app.route("/committee_membership", methods=["POST"])
def committee_membership():
    user = tally_toolkit.user_info()
    try:
        data = json.loads(request.data.decode())
        user.chamber = data['chamber']
        user.bioguide_id_to_search = data['bioguide_id']
    except:
        user.chamber = request.form['chamber']
        user.bioguide_id_to_search = request.form['bioguide_id']
    rep_membership = tally_toolkit.user_info.get_committee_membership(user)
    if len(rep_membership) > 0:
        return jsonify(results=rep_membership.to_dict(orient='records'))
    else:
        return jsonify(results=False)

## Find Legislation for user to vote on
@app.route("/legislation_for_user", methods=["POST"])
def legislation_for_user():
    vote_data = tally_toolkit.user_votes()
    try:
        data = json.loads(request.data.decode())
        vote_data.user_id = data['user_id']
    except:
        vote_data.user_id = request.form['user_id']
    tally_toolkit.user_votes.available_votes(vote_data)
    try:
        bill_summary = tally_toolkit.user_votes.summarize_bill(vote_data)
        vote_data.leg_for_user.loc[0, 'summary'] = bill_summary
    except:
        vote_data.leg_for_user.loc[0, 'summary'] = 'No Summary'
    if len(vote_data.leg_for_user) > 0:
        return jsonify(vote_data.leg_for_user.to_dict(orient='records')[0])
    else:
        return jsonify(results=False)

## Put user vote in db
@app.route("/user_vote", methods=["POST"])
def user_vote():
    vote_data = tally_toolkit.user_votes()
    try:
        data = json.loads(request.data.decode())
        vote_data.user_id = data['user_id']
        vote_data.roll_id = data['roll_id']
        vote_data.vote = bool(data['vote'])
    except:
        vote_data.user_id = request.form['user_id']
        vote_data.roll_id = request.form['roll_id']
        vote_data.vote = bool(request.form['vote'])
    tally_toolkit.user_votes.vote_to_db(vote_data)
    return jsonify(results=vote_data.insert)

## Find number of days voted
@app.route("/attendance", methods=["POST"])
def attendance():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
        # rep_perfomance.congress_num = data['congress']
        chamber = data['chamber']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']
        # rep_perfomance.congress_num = request.form['congress']
        chamber = request.form['chamber']

    """
    The dashboard should show the current congress. But
    for future metrics this should be dynamic for the 
    congress the user wants to search.
    """
    tally_toolkit.Performance.current_congress_num(rep_perfomance)

    # if rep_perfomance.congress_num == 'current':
    #     tally_toolkit.Performance.current_congress_num(rep_perfomance)
    # else:
    #     rep_perfomance.congress_num = int(rep_perfomance.congress_num)

    if chamber.lower() == 'house':
        try:
            ## Get attendance
            tally_toolkit.Performance.num_days_voted_house(rep_perfomance)
            rep_perfomance.days_voted = rep_perfomance.days_voted[['days_at_work', 'percent_at_work', 'total_work_days']]
            return jsonify(rep_perfomance.days_voted.to_dict(orient='records')[0])
        except:
            ## If returns no data
            return jsonify(results=False)
    elif chamber.lower() == 'senate':
        try:
            ## Get attendance
            tally_toolkit.Performance.num_days_voted_senate(rep_perfomance)
            rep_perfomance.days_voted = rep_perfomance.days_voted[[  'days_at_work', 'percent_at_work', 'total_work_days']]
            return jsonify(rep_perfomance.days_voted.to_dict(orient='records')[0])
        except:
            ## If returns no data
            return jsonify(results=False)
    else:
        return jsonify(results='check the chamber')

## Return all attendance
@app.route("/rank_attendance", methods=["POST"])
def rank_attendance():
    rep_perfomance = tally_toolkit.Performance()
    tally_toolkit.Performance.current_congress_num(rep_perfomance)
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.chamber = request.form['chamber']

    ## Get data
    tally_toolkit.Performance.num_days_voted_all(rep_perfomance)
    try:
        return jsonify(results=rep_perfomance.days_voted.to_dict(orient='records'))
    except:
        ## If returns no data
        return jsonify(results=False)


## Find number of votes cast
@app.route("/participation", methods=["POST"])
def participation():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
        rep_perfomance.congress_num = data['congress']
        chamber = data['chamber']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']
        rep_perfomance.congress_num = request.form['congress']
        chamber = request.form['chamber']

    """
    The dashboard should show the current congress. But
    for future metrics this should be dynamic for the 
    congress the user wants to search.
    """
    if rep_perfomance.congress_num == 'current':
        tally_toolkit.Performance.current_congress_num(rep_perfomance)
    else:
        rep_perfomance.congress_num = int(rep_perfomance.congress_num)

    if chamber.lower() == 'house':
        try:
            ## Get participation
            tally_toolkit.Performance.num_votes_house(rep_perfomance)
            return jsonify(rep_perfomance.rep_votes_metrics.to_dict(orient='records')[0])
        except:
            ## If returns no data
            return jsonify(results=False)
    elif chamber.lower() == 'senate':
        try:
            ## Get participation
            tally_toolkit.Performance.num_votes_senate(rep_perfomance)
            return jsonify(rep_perfomance.rep_votes_metrics.to_dict(orient='records')[0])
        except:
            ## If returns no data
            return jsonify(results=False)
    else:
        return jsonify(results='check the chamber')

## Return all attendance
@app.route("/rank_participation", methods=["POST"])
def rank_participation():
    rep_perfomance = tally_toolkit.Performance()
    tally_toolkit.Performance.current_congress_num(rep_perfomance)
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.chamber = request.form['chamber']

    ## Get data
    tally_toolkit.Performance.num_votes_all(rep_perfomance)
    try:
        return jsonify(results=rep_perfomance.rep_votes_metrics.to_dict(orient='records'))
    except:
        ## If returns no data
        return jsonify(results=False)


## Find number of votes shes cast
@app.route("/efficacy", methods=["POST"])
def efficacy():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
        rep_perfomance.congress_num = data['congress']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']
        rep_perfomance.congress_num = request.form['congress']

    """
    The dashboard should show the current congress. But
    for future metrics this should be dynamic for the 
    congress the user wants to search.
    """
    if rep_perfomance.congress_num == 'current':
        tally_toolkit.Performance.current_congress_num(rep_perfomance)
    else:
        rep_perfomance.congress_num = int(rep_perfomance.congress_num)

    try:
        ## Get efficacy
        tally_toolkit.Performance.num_sponsor(rep_perfomance)
        return jsonify(rep_perfomance.rep_sponsor_metrics.to_dict(orient='records')[0])
    except:
        ## If returns no data
        return jsonify(results=False)

## Return all attendance
@app.route("/rank_efficacy", methods=["POST"])
def rank_efficacy():
    rep_perfomance = tally_toolkit.Performance()
    tally_toolkit.Performance.current_congress_num(rep_perfomance)
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.chamber = request.form['chamber']

    ## Get data
    tally_toolkit.Performance.num_sponsored_all(rep_perfomance)
    try:
        return jsonify(results=rep_perfomance.rep_sponsor_metrics.to_dict(orient='records'))
    except:
        ## If returns no data
        return jsonify(results=False)


## Return list of reps for user to search
@app.route("/list_reps", methods=["POST"])
def list_reps():
    user = tally_toolkit.user_info()
    user.return_rep_list = 'Present'
    x = tally_toolkit.user_info.list_reps(user)
    return jsonify(results=x.to_dict(orient='records'))

## Return reps by zip code
@app.route("/reps_by_zip", methods=["POST"])
def reps_by_zip():
    user = tally_toolkit.user_info()
    try:
        print 'trying first way'
        data = json.loads(request.data.decode())
        user.zip_code = tally_toolkit.sanitize(data['zip_code'])

    except:
        print 'trying second way'
        user.zip_code = tally_toolkit.sanitize(request.form['zip_code'])
    try:
        x = tally_toolkit.user_info.find_dist_by_zip(user)
        return jsonify(results=x.to_dict(orient='records'))
    except:
        return jsonify(results='Could not find zip code')

## Return membership stats
@app.route("/membership_stats", methods=["POST"])
def membership_stats():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
        rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']
        rep_perfomance.chamber = request.form['chamber']
    try:
        ## Get stats
        tally_toolkit.Performance.membership_stats(rep_perfomance)
        return jsonify(rep_perfomance.membership_stats_df.to_dict(orient='records')[0])
    except:
        ## If returns no data
        return jsonify(results=False)

## Return policy stats
@app.route("/policy_areas", methods=["POST"])
def policy_areas():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']
    ## Searching for current congress
    tally_toolkit.Performance.current_congress_num(rep_perfomance)
    try:
        ## Get stats
        tally_toolkit.Performance.policy_areas(rep_perfomance)
        return jsonify(results=rep_perfomance.policy_area_df.to_dict(orient='records'))
    except:
        ## If returns no data
        return jsonify(results=False)

## Return policy stats
@app.route("/search", methods=["POST"])
def search():
    user_search = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        user_search.search_term = data['search_term']
    except:
        user_search.search_term = request.form['search_term']
    try:
        return jsonify(results=tally_toolkit.Performance.search(user_search))
    except:
        ## If returns no data
        return jsonify(results=[])


if __name__ == '__main__':
    ## app.run is to run with flask
    app.run(debug=True)
