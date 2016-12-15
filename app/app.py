from __future__ import division
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash, jsonify, Response
import pandas as pd
import numpy as np
import requests


app = Flask(__name__)
app.config.from_object(__name__)




@app.route('/', methods=['GET', 'POST'])
def index():
	if request.method == 'POST':

	    username = request.form['username']
	    password = request.form['password']
        render_template('login.html')
	    # matched_credentials = reps_query.search_user(username, password)    
	    # if matched_credentials == True:
	    #     user_data = reps_query.get_user_data(username)
	    #     print user_data
	    #     return render_template('login_yes.html')
	    # else:
	    #     error = "Wrong user name or password"
	    #     return render_template('login.html', error=error)
	else:
		return render_template('login.html')



if __name__ == '__main__':
    ## app.run is to run with flask
    #app.run(debug=True)

    """I should learn why to use tornado and 
    if it's worth it for us to switch. The
    code below is to connect to tornado."""
    from tornado.wsgi import WSGIContainer
    from tornado.httpserver import HTTPServer
    from tornado.ioloop import IOLoop
    import tornado.options

    tornado.options.parse_command_line()
    http_server = HTTPServer(WSGIContainer(app))
    http_server.listen(5000, address='127.0.0.1')
    tornado.web.Application(debug=True)
    IOLoop.instance().start()