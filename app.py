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
        ## Try: Search user by ID
        try:
            data = json.loads(request.data.decode())
            user.user_id = tally_toolkit.sanitize(data['user_id'])
        except:
            user.user_id = tally_toolkit.sanitize(request.form['user_id'])
        if user.user_id != None:
            try:
                user_data = tally_toolkit.user_info.get_user_data(user)
                tally_toolkit.user_info.session_tracking(user)
                print user_data
                return jsonify(user_data.to_dict(orient='records')[0])
            except:
                error = "Wrong user id"
                print error
                return jsonify(results="wrong id")
    except:
    ## Try: Search user by email and password
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
            return jsonify(user_data.to_dict(orient='records')[0])
        else:
            error = "Wrong user name or password"
            print error
            return jsonify(results="wrong username/password")

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
        return jsonify(tally_toolkit.user_info.get_id_from_email(user)[0])
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

## Find number of days voted
@app.route("/attendance", methods=["POST"])
def attendance():
    rep_perfomance = tally_toolkit.Performance()
    rep_perfomance.table = 'current_attendance'
    rep_perfomance.how = 'bioguide_id'
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']
    try:
        tally_toolkit.Performance.get_current_performance(rep_perfomance)
        return jsonify(rep_perfomance.current_stats[['days_at_work', 
            'percent_at_work', 
            'total_work_days']].to_dict(orient='records')[0])
    except:
        return jsonify(results=False)


## Return all attendance
@app.route("/rank_attendance", methods=["POST"])
def rank_attendance():
    rep_perfomance = tally_toolkit.Performance()
    rep_perfomance.how = 'chamber'
    rep_perfomance.table = 'current_attendance'
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.chamber = request.form['chamber']

    ## Get data
    tally_toolkit.Performance.get_current_performance(rep_perfomance)
    try:
        return jsonify(results=rep_perfomance.current_stats.to_dict(orient='records'))
    except:
        ## If returns no data
        return jsonify(results=False)


## Find number of votes cast
@app.route("/participation", methods=["POST"])
def participation():
    rep_perfomance = tally_toolkit.Performance()
    rep_perfomance.how = 'bioguide_id'
    rep_perfomance.table = 'current_participation'
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']

    try:
        tally_toolkit.Performance.get_current_performance(rep_perfomance)
        return jsonify(rep_perfomance.current_stats[['bioguide_id', 
            'percent_votes', 
            'rep_votes', 
            'total_votes']].to_dict(orient='records')[0])
    except:
        ## If returns no data
        return jsonify(results=False)

## Return all attendance
@app.route("/rank_participation", methods=["POST"])
def rank_participation():
    rep_perfomance = tally_toolkit.Performance()
    rep_perfomance.how = 'chamber'
    rep_perfomance.table = 'current_participation'
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.chamber = request.form['chamber']

    ## Get data
    try:
        tally_toolkit.Performance.get_current_performance(rep_perfomance)
        return jsonify(results=rep_perfomance.current_stats.to_dict(orient='records'))
    except:
        ## If returns no data
        return jsonify(results=False)


## Find number of bills she's made
@app.route("/efficacy", methods=["POST"])
def efficacy():
    rep_perfomance = tally_toolkit.Performance()
    rep_perfomance.how = 'bioguide_id'
    rep_perfomance.table = 'current_sponsor'
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']

    try:
        tally_toolkit.Performance.get_current_performance(rep_perfomance)
        return jsonify(rep_perfomance.current_stats[['bioguide_id', 
            'max_sponsor', 
            'rep_sponsor', 
            'sponsor_percent']].to_dict(orient='records')[0])
    except:
        return jsonify(results=False)

## Return all attendance
@app.route("/rank_efficacy", methods=["POST"])
def rank_efficacy():
    rep_perfomance = tally_toolkit.Performance()
    rep_perfomance.how = 'chamber'
    rep_perfomance.table = 'current_sponsor'
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.chamber = request.form['chamber']

    try:
        tally_toolkit.Performance.get_current_performance(rep_perfomance)
        return jsonify(results=rep_perfomance.current_stats.to_dict(orient='records'))
    except:
        ## If returns no data
        return jsonify(results=False)

## Return all attendance
@app.route("/bills_to_law", methods=["POST"])
def bills_to_law():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.how = data['how']
        print rep_perfomance.how
        if rep_perfomance.how == 'bioguide_id':
            rep_perfomance.bioguide_id = data['bioguide_id']
        else:
            rep_perfomance.chamber = data['chamber']
    except:
        rep_perfomance.how = request.form['how']
        print rep_perfomance.how
        if rep_perfomance.how == 'bioguide_id':
            rep_perfomance.bioguide_id = request.form['bioguide_id']
        else:
            rep_perfomance.chamber = request.form['chamber']
    try:
        return jsonify(results=tally_toolkit.Performance.bills_to_law(rep_perfomance))
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

## Search shit!
@app.route("/search", methods=["POST"])
def search():
    user_search = tally_toolkit.Search()
    try:
        data = json.loads(request.data.decode())
        user_search.search_term = data['search_term']
    except:
        user_search.search_term = request.form['search_term']
    # try:

    user_search.df = tally_toolkit.Search.search(user_search)
    tally_toolkit.Search.add_sim(user_search)

    return jsonify(results=user_search.df.drop_duplicates(['bioguide_id']).drop(['b_id'],1).to_dict(orient='records'))
    # except:
    #     ## If returns no data
    #     return jsonify(results=[])

## Get a reps grade
@app.route("/rep_grade", methods=["POST"])
def rep_grade():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
        # rep_perfomance.congress_num = data['congress']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']
        # rep_perfomance.congress_num = request.form['congress']

    ## Get current congress
    tally_toolkit.Performance.current_congress_num(rep_perfomance)
    try:
        tally_toolkit.Performance.get_rep_grade(rep_perfomance)
        return jsonify(rep_perfomance.rep_grade.to_dict(orient='records')[0])
    except:
        ## If returns no data
        return jsonify(results=False)

## Get a reps beliefs
@app.route("/rep_beliefs", methods=["POST"])
def rep_beliefs():
    rep_perfomance = tally_toolkit.Performance()
    try:
        data = json.loads(request.data.decode())
        rep_perfomance.bioguide_id = data['bioguide_id']
    except:
        rep_perfomance.bioguide_id = request.form['bioguide_id']

    ## Get and return beliefs
    try:
        return jsonify(results=tally_toolkit.Performance.rep_beliefs(rep_perfomance))
    except:
        ## If returns no data
        return jsonify(results=False)

if __name__ == '__main__':
    ## app.run is to run with flask
    app.run(debug=True)
