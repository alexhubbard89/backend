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
tally_toolkit = imp.load_source('module', 'python/tally_toolkit.py')


app = Flask(__name__)
CORS(app)
app.config.from_object(__name__)

app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.ERROR)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        user = tally_toolkit.user_info()
        user.email = request.form['email']
        user.password = request.form['password']
        matched_credentials = tally_toolkit.user_info.search_user(user)

        if matched_credentials == True:
            user_data = tally_toolkit.user_info.get_user_dashboard_data(user)
            print user_data
            return render_template('login_yes.html', user_data=user_data)
        else:
            error = "Wrong user name or password"
            return render_template('login.html', error=error)
    else:
        return render_template('login.html')


## Login testing
@app.route("/login", methods=["POST"])
def login():
    user = tally_toolkit.user_info()
    try:
        data = json.loads(request.data.decode())
        user.email = data['email']
        user.password = data['password']
    except:
        user.email = request.form['email']
        user.password = request.form['password']
    matched_credentials = tally_toolkit.user_info.search_user(user)
    if matched_credentials == True:
        user_data = tally_toolkit.user_info.get_user_dashboard_data(user)
        print user_data
        return jsonify(results=user_data.to_dict(orient='records'))
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
        user.email = data['email']
        user.password = data['password']
        user.first_name = data['first_name']
        user.last_name = data['last_name']
        user.gender = data['gender']
        user.dob = data['dob']
        user.street = data['street']
        user.zip_code = data['zip_code']

    except:
        print 'trying second way'
        user.email = request.form['email']
        user.password = request.form['password']
        user.first_name = request.form['first_name']
        user.last_name = request.form['last_name']
        user.gender = request.form['gender']
        user.dob = request.form['dob']
        user.street = request.form['street']
        user.zip_code = request.form['zip_code']

    print zip_code
    user.user_df = tally_toolkit.user_info.create_user_params(user)
    user_made = user_info.user_info_to_sql(user)

    if user_made == True:
        return jsonify(result=True)
    elif user_made == False:
        error = "oops! That user name already exists."
        return jsonify(result=False)

if __name__ == '__main__':
    ## app.run is to run with flask
    app.run(debug=True)
