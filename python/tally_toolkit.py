import pandas as pd
import numpy as np
import psycopg2
import urlparse
import hashlib, uuid
from uszipcode import ZipcodeSearchEngine
import us
import os
import sys
import requests
from bs4 import BeautifulSoup
from pandas.io.json import json_normalize
from xmljson import badgerfish as bf
from xml.etree.ElementTree import fromstring
from json import dumps
from xml.etree import ElementTree
import datetime
from pytz import reference
import re
import us
from unidecode import unidecode
## algo to summarize
from gensim.summarization import summarize
import ast
from scipy import stats
import math
from collections import Counter
from StringIO import StringIO
import urllib2
from zipfile import ZipFile
import calendar
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO
import string
from nltk import tokenize

urlparse.uses_netloc.append("postgres")
url = urlparse.urlparse(os.environ["DATABASE_URL"])

def open_connection():
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection


"""Function to sanitize user input.
This would be too much to put into a class"""
def sanitize(inputstr):
    sanitized = str(inputstr).replace("'", "''")
    sanitized = str(sanitized).replace("--", " - ")
    badstrings = [
        ';',
        '$',
        '&&',
        '../',
        '<',
        '>',
        '%3C',
        '%3E',
        '--',
        '1,2',
        '\x00',
        '`',
        '(',
        ')',
        'file://',
        'input://'
    ]
    for badstr in badstrings:
        if badstr in sanitized:
            sanitized = sanitized.replace(badstr, '')
    return sanitized

def current_congress_num():
    """
    This method will be used to find the
    maximum congresss number. The max
    congress will be the current congress.
    """
    
    cong_num = pd.read_sql_query("""select max(congress) from house_vote_menu;""",open_connection())
    return cong_num.loc[0, 'max']
    
class user_info(object):
    """
    This will be used to store users to db and and test login credentials.
    
    Attributes: email, password, if password is correct, name, gender, dob,
    street, zip_code, and user_df
    """

    def check_address(self):
        street = self.street.lower().title().replace(' ', '+')
        url = "https://maps.googleapis.com/maps/api/geocode/json?address={},+{}".format(street, str(self.zip_code))
        r = requests.get(url)
        if r.status_code == 200:
            try:
                r.json()['results'][0]['partial_match']
                self.address_check = "Bad address"
            except:
                """Address is good"""
                self.address_check = True
        else:
            self.address_check = "Bad request"
        
    
    def create_user_params(self):
        """Hold data about the user. We've collected all of the information we need from the
        user. The last thing that needs to be done is to find out what state they live in, and which 
        district they are from. Then we can find their Presenent reps from that info."""

        search = ZipcodeSearchEngine()
        zipcode = search.by_zipcode(str(self.zip_code))

        df = pd.DataFrame(columns=[['email', 'password', 'first_name', 
            'last_name', 'gender', 'dob', 'street', 'zip_code', 'city',
            'state_short', 'state_long', 'district', 'party']])
        

        df.loc[0, 'email'] = self.email
        df.loc[0, 'password'] = user_info.hash_password(self)
        df.loc[0, 'first_name'] = self.first_name.lower().title()
        df.loc[0, 'last_name'] = self.last_name.lower().title()
        df.loc[0, 'gender'] = self.gender.lower().title()
        df.loc[0, 'dob'] = pd.to_datetime(self.dob)
        df.loc[0, 'street'] = self.street.lower().title()
        df.loc[0, 'zip_code'] = str(self.zip_code)
        df.loc[0, 'city'] = str(zipcode['City'].lower().title())
        df.loc[0, 'state_short'] = str(zipcode['State'])
        df.loc[0, 'state_long'] = str(us.states.lookup(df.loc[0, 'state_short']))

        self.city = df.loc[0, 'city']
        self.state_short = df.loc[0, 'state_short']
        self.state_long = df.loc[0, 'state_long']

        try:
            df.loc[0, 'district'] = user_info.get_district_from_address(self)
        except IndexError:
            df.loc[0, 'district'] = user_info.rep_from_zip_extention(self)
        df.loc[0, 'party'] = self.party

        return df

    def get_id_from_email(self):
        return pd.read_sql_query("""
        SELECT user_id 
        FROM user_tbl 
        WHERE email = '{}';
        """.format(self.email), open_connection()).to_dict(orient='records')

    def user_info_to_sql(self):
        connection = open_connection()
        x = list(self.user_df.loc[0,])
        cursor = connection.cursor()

        for p in [x]:
            format_str = """
            INSERT INTO user_tbl (
            email,
            password,
            street,
            zip_code,
            city,
            state_short,
            state_long,
            first_name,
            last_name,
            gender,
            dob,
            district,
            party,
            address_fail)
            VALUES ('{email}', '{password}', '{street}', '{zip_code}', '{city}', '{state_short}',
                    '{state_long}', '{first_name}', '{last_name}', 
                    '{gender}', '{dob}', '{district}', '{party}', '{address_fail}');"""


        sql_command = format_str.format(email=self.user_df.loc[0, 'email'], 
            password=self.user_df.loc[0, 'password'], street=self.user_df.loc[0, 'street'], 
            zip_code=int(self.user_df.loc[0, 'zip_code']), city=self.user_df.loc[0, 'city'], 
            state_short=self.user_df.loc[0, 'state_short'], 
            state_long=self.user_df.loc[0, 'state_long'],  
            first_name=self.user_df.loc[0, 'first_name'], 
            last_name=self.user_df.loc[0, 'last_name'], 
            gender=self.user_df.loc[0, 'gender'], 
            dob=self.user_df.loc[0, 'dob'], 
            district=int(self.user_df.loc[0, 'district']),
            party=self.user_df.loc[0, 'party'],
            address_fail=self.address_fail)


        try:
            cursor.execute(sql_command)
            connection.commit()
            user_made = True
        except:
            """duplicate key value violates unique constraint "user_tbl_user_name_key"
            DETAIL:  Key (user_name)=(user_test) already exists."""
            connection.rollback()
            user_made = False
        connection.close()
        return user_made    

    def get_district_from_address(self):
        state = '{}{}'.format(self.state_short, self.state_long)

        s = requests.Session()
        s.auth = ('user', 'pass')
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        }
        url = 'https://ziplook.house.gov/htbin/findrep?ADDRLK'
        form_data = {
            'street': self.street,
            'city': self.city,
            'state': state,
            'submit': 'FIND YOUR REP',
        }

        response = requests.request(method='POST', url=url, data=form_data, headers=headers)

        page = BeautifulSoup(response.content, 'lxml')
        your_rep = page.find('div', class_='relatedContent')
        district = int(str(your_rep).split('src="/zip/pictures/{}'.format(self.state_short.lower()))[1].split('_')[0])
        return district

    def rep_from_zip_extention(self):
        s = requests.session()
        a = requests.adapters.HTTPAdapter(max_retries=5)
        s.mount('http://', a)
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        s.headers.update(headers)
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}+,{}'.format(self.street, self.zip_code)
        r = s.get(url)
        if r.status_code == 200:
            try:
                df = pd.DataFrame(r.json()['results'][0]['address_components'])
                df['suffix'] = df.loc[:,'types'].apply(lambda x: 'suffix' in x[0])
                df = df.loc[df['suffix'] == True].reset_index(drop=True)

                extention = df.loc[0, 'long_name']
                return user_info.zip_for_dist(self, extention)
            except:
                "could not find extention"
                self.address_fail = True
                return user_info.zip_for_dist(self)
        else:
            return user_info.zip_for_dist(self)

    def zip_for_dist(self, extention=None):
        
        s = requests.session()
        a = requests.adapters.HTTPAdapter(max_retries=5)
        s.mount('http://', a)
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        s.headers.update(headers)
        if extention != None:
            url = 'https://ziplook.house.gov/htbin/findrep?ZIP={}-{}'.format(self.zip_code, extention)
        else:
            url = 'https://ziplook.house.gov/htbin/findrep?ZIP={}'.format(self.zip_code)
        r = s.get(url)
        page = BeautifulSoup(r.content, 'lxml')
        possible_reps = page.findAll('div', id='PossibleReps')[0]

        dist = ''
        possible_reps = possible_reps.find('div')
        dist = int(str(possible_reps).split('/zip/pictures/')[1].split('_')[0].replace(self.state_short.lower(), ''))
        print dist

        return dist


    
    def search_email(self):
        connection = open_connection()
        sql_command = """
        select password from  user_tbl
        where email = '{}'""".format(self.email)

        user_results = pd.read_sql_query(sql_command, connection)
        connection.close()
        return user_results
    
    
    def search_user(self):
        try:
            password_found = user_info.search_email(self).loc[0, 'password']
            pw_match = user_info.test_password(self, password_found, version=1)
            if pw_match == True:
                self.password_match = True
                return True
            elif pw_match == False:
                return False
        except KeyError:
            return "user does not exist"
        
    def get_user_data(self):
        if self.user_id != None:
            sql_command = """
            select * from  user_tbl
            where user_id = '{}'""".format(self.user_id)

            user_results = pd.read_sql_query(sql_command, open_connection())
            return user_results[['user_id', 'city', 'state_short', 'state_long', 'first_name', 'last_name', 'district', 'street', 'zip_code']]

        if self.password_match == True:
            sql_command = """
            select * from  user_tbl
            where email = '{}'""".format(self.email)

            user_results = pd.read_sql_query(sql_command, open_connection())
            return user_results[['user_id', 'city', 'state_short', 'state_long', 'first_name', 'last_name', 'district', 'street', 'zip_code']]
        elif self.password_match == False:
            return "Check credentials frist"

    def get_congress_bio(self):
        ## Search for user's reps in current year
        cong_num = current_congress_num()

        return pd.read_sql_query("""
                SELECT * FROM (
                SELECT * 
                FROM congress_bio 
                WHERE state = '{}' 
                AND served_until = 'Present'
                AND ((chamber = 'senate') 
                OR (chamber = 'house' and district = {})))
                AS rep_bio
                LEFT JOIN (
                SELECT bioguide_id as b_id,
                letter_grade_extra_credit as letter_grade,
                total_grade_extra_credit as number_grade
                FROM congress_grades
                WHERE congress = {}
                ) AS grades 
                ON grades.b_id = rep_bio.bioguide_id
                ;""".format(self.state_long, self.district, cong_num), open_connection()).drop(['b_id'], 1)


        user_results = pd.read_sql_query(sql_command, open_connection())
        return user_results

    def get_committee_membership(self):

        """
        This method will grab the committee memership for a rep.

        Input: bioguide_id
        """
        if self.chamber.lower() == 'house': 
            table = 'house_membership'
        elif self.chamber.lower() == 'senate':
            table = 'senate_membership'

        sql_query = "SELECT * FROM {} WHERE bioguide_id = '{}';".format(table, self.bioguide_id_to_search)
        reps_membership = pd.read_sql_query(sql_query, open_connection())
        return reps_membership
        
    def get_user_dashboard_data(self):
        if self.password_match == True:
            ## Open the connection
            connection = open_connection()
            
            ## Search for user info
            sql_command = """
            select * from  user_tbl
            where email = '{}'""".format(self.email)
            user_results = pd.read_sql_query(sql_command, connection)
            
            ## Search for user's reps
            sql_command = """select * 
            from congress_bio 
            where state = '{}' 
            and served_until = 'Present'
            and ((chamber = 'senate') 
            or (chamber = 'house' and district = {}));""".format(user_results.loc[0, 'state_long'],
                                                                user_results.loc[0, 'district'])
            user_reps = pd.read_sql_query(sql_command, open_connection())
            
            ## Drop uneeded info
            user_results = user_results[['user_id', 'city', 'state_short', 'state_long', 'first_name', 'last_name', 'district']]
            
            ## Add reps membership data to reps data.
            ## For each house rep locate their membership and add it 
            ## to the user_reps data set.
            indices = user_reps.loc[user_reps['chamber'] == 'house'].index
            for i in range(len(indices)):
                sql_query = "SELECT * FROM house_membership WHERE bioguide_id = '{}';".format(user_reps.loc[indices[i], 'bioguide_id'])
                reps_membership = pd.read_sql_query(sql_query, open_connection())
                user_reps.loc[indices[i], 'reps_membership'] = [reps_membership.transpose().to_dict()]

            ## Add reps membership data to reps data.
            ## For each senator locate their membership and add it 
            ## to the user_reps data set.
            indices = user_reps.loc[user_reps['chamber'] == 'senate'].index
            for i in range(len(indices)):
                sql_query = "SELECT * FROM senate_membership WHERE bioguide_id = '{}';".format(user_reps.loc[indices[i], 'bioguide_id'])
                reps_membership = pd.read_sql_query(sql_query, open_connection())
                user_reps.loc[indices[i], 'reps_membership'] = [reps_membership.transpose().to_dict()]

            ## Clean the rows that have no data
            user_reps.loc[user_reps['reps_membership'].isnull(), 'reps_membership'] = None

            ## Add reps info to user data
            user_results.loc[0, 'reps_data'] =  [user_reps.transpose().to_dict()]
            
            ## Close connection and return
            connection.close()
            return user_results
        elif self.password_match == False:
            return "Check credentials frist"
        
    def hash_password(self, version=1, salt=None):
        if version == 1:
            if salt == None:
                salt = uuid.uuid4().hex[:16]
            hashed = salt + hashlib.sha1( salt + self.password).hexdigest()
            # generated hash is 56 chars long
            return hashed
        # incorrect version ?
        return None

    def test_password(self, hashed, version=1):
        if version == 1:
            salt = hashed[:16]
            rehashed = user_info.hash_password(self, version, salt)
            return rehashed == hashed
        return False

    def list_reps(self):
        x = pd.read_sql_query("""SELECT * FROM congress_bio
        WHERE served_until = '{}';""".format(self.return_rep_list), open_connection())
        
        return x[['name', 'bioguide_id', 'state', 'district', 'chamber']].drop_duplicates().reset_index(drop=True)

    def find_dist_by_zip(self):

        s = requests.Session()
        s.auth = ('user', 'pass')
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        }
        url = 'http://ziplook.house.gov/htbin/findrep?ZIP={}&Submit=FIND+YOUR+REP+BY+ZIP'.format(self.zip_code)
        r = requests.get(url=url, headers=headers)
        page = BeautifulSoup(r.content, 'lxml')
        possible_reps = str(page.findAll('div', id='PossibleReps')[0])
        
        district_info = pd.DataFrame()

        for i in range(1, len(possible_reps.split('/zip/pictures/'))):
            state_dist = possible_reps.split('/zip/pictures/')[i].split('_')[0]
            split_sd = re.split('(\d+)', state_dist)
            for j in range(len(split_sd)):
                if j == 0:
                    ## Letters is state short
                    state_short = str(split_sd[j])
                    district_info.loc[i, 'state_short'] = state_short
                    state_long = str(us.states.lookup(state_short))
                    district_info.loc[i, 'state_long'] = state_long
                elif j == 1:
                    ## Numbers is district number
                    district_num = int(split_sd[j])
                    district_info.loc[i, 'district_num'] = district_num
                    
        dist = district_info.reset_index(drop=True)

        dist_query = ''
        for i in range(len(dist)):
            if i != 0:
                dist_query += " OR (state = '{}' AND district ='{}')".format(
                    dist.loc[i, 'state_long'], int(dist.loc[i, 'district_num']))
            if i == 0:
                dist_query += "(state = '{}' AND district ='{}')".format(
                    dist.loc[i, 'state_long'], int(dist.loc[i, 'district_num']))


        sql_query = """SELECT distinct name, bioguide_id, state, district, served_until, chamber
        FROM congress_bio
        WHERE (({})
        AND served_until = 'Present')
        OR (state = '{}' AND served_until = 'Present' AND chamber = 'senate')""".format(dist_query, dist.loc[i, 'state_long'],)
        
        return pd.read_sql_query(sql_query, open_connection())

    def session_tracking(self):
        
        x = pd.read_sql_query("""
        SELECT user_id, admin
        FROM user_tbl
        WHERE user_id = '{}'""".format(self.user_id), open_connection())
        
        if x.loc[0, 'admin'] == False:
            ## If not admin then get login stats
            connection = open_connection()
            cursor = connection.cursor()

            now = datetime.datetime.now()
            tz = reference.LocalTimezone().tzname(now)

            sql_command = """INSERT INTO user_sessions (
                        user_id, 
                        session_datetime,
                        time_zone)
                        VALUES ('{}', '{}', '{}');""".format(self.user_id, 
                                                      now, 
                                                      tz)
            try:
                cursor.execute(sql_command)
                connection.commit()
            except:
                connection.rollback()
            connection.close()

    @staticmethod      
    def change_setting(param, user_id, password=None, street=None, zip_code=None):
        user_params = user_info()
        if param == 'password':
            if password == None:
                return 'no password found'
            user_params.password = sanitize(password)
            password = user_info.hash_password(user_params)

            ## Update
            connection = open_connection()
            cursor = connection.cursor()
            sql_command = """UPDATE user_tbl 
                            SET
                            password = '{}'
                            WHERE user_id = '{}';""".format(
                            password,
                            user_id)
            try:
                cursor.execute(sql_command)
                connection.commit()
                connection.close()
                return True
            except:
                connection.rollback()
                connection.close()
                return 'who the fuck is that user?'
        elif param == 'address':
            try:
                user_params.street = sanitize(street.replace("'", ''))
                user_params.zip_code = sanitize('{}'.format(zip_code).zfill(5))
                user_info.check_address(user_params)
                if user_params.address_check == True:
                    search = ZipcodeSearchEngine()
                    zipcode = search.by_zipcode(str(user_params.zip_code))
                    user_params.city = str(zipcode['City'].lower().title())
                    user_params.state_short = str(zipcode['State'])
                    user_params.state_long = str(us.states.lookup(str(user_params.state_short)))
                    try:
                        district = user_info.get_district_from_address(self)
                    except IndexError:
                        district = user_info.rep_from_zip_extention(self)

                    ## Update
                    connection = open_connection()
                    cursor = connection.cursor()
                    sql_command = """UPDATE user_tbl 
                                    SET
                                    street = '{}',
                                    zip_code = '{}',
                                    city = '{}',
                                    state_short = '{}',
                                    state_long = '{}',
                                    district = '{}'
                                    WHERE user_id = '{}';""".format(
                                    user_params.street,
                                    user_params.zip_code,
                                    user_params.city,
                                    user_params.state_short,
                                    user_params.state_long,
                                    district,
                                    user_id)
                    try:
                        cursor.execute(sql_command)
                        connection.commit()
                        connection.close()
                        return True
                    except:
                        connection.rollback()
                        connection.close()
                        return 'who the fuck is that user?'

            except AttributeError:
                return "no address found"
        else:
            return 'not a real request'

    
    def __init__(self, email=None, password=None, password_match=False, first_name=None,
                last_name=None, gender=None, dob=None, street=None, zip_code=None, user_df=None,
                state_long=None, district=None, bioguide_id_to_search=None, chamber=None,
                address_check=None, return_rep_list=None, city=None, state_short=None,
                user_id=None, party=None):
        self.email = email
        self.password = password
        self.password_match = password_match
        self.first_name = first_name
        self.last_name = last_name
        self.gender = gender
        self.dob = dob
        self.street = street
        self.zip_code = zip_code
        self.party = party
        self.user_df = user_df
        self.state_long = state_long
        self.district = district
        self.bioguide_id_to_search = bioguide_id_to_search
        self.chamber = chamber
        self.address_check = address_check
        self.return_rep_list = return_rep_list
        self.city = city
        self.state_short = state_short
        self.user_id = user_id
        self.address_fail = False


class vote_collector(object):
    """
    This class will be used to collect votes from congress.
    
    
    Attributes:
    house_vote_menu - votes collected for this year's vote menu.
    to_db - how many new rows were put in the database.
    
    """

    def house_vote_menu(self, year):
        ## Set columns
        column = ['roll', 'roll_link', 'date', 'issue', 'issue_link',
                  'question', 'result', 'title_description']

        ## Structure data frame
        df = pd.DataFrame(columns=[column])
        page_num = 0
        next_page = True

        url = 'http://clerk.house.gov/evs/{}/ROLL_000.asp'.format(year)
        print url
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        congress = str(soup.find_all('body')[0].find_all('h2')[0]).split('<br/>\r\n ')[1].split('<')[0]
        session = str(soup.find_all('body')[0].find_all('h2')[0]).split('Congress - ')[1].split('<')[0]

        while next_page == True:
            ## Vistit page to scrape
            url = 'http://clerk.house.gov/evs/{}/ROLL_{}00.asp'.format(year, page_num)
            print url
            page = requests.get(url)

            if len(page.content.split('The page you requested cannot be found')) == 1:
                soup = BeautifulSoup(page.content, 'lxml')

                ## Find section to scrape
                x = soup.find_all('tr')

                ## Find sectino to scrape
                x = soup.find_all('tr')
                for i in range(1, len(x)):
                    counter = 0
                    ## Make array to hold data scraped by row
                    test = []
                    for y in x[i].find_all('td'):
                        ## scrape the text data
                        test.append(y.text)
                        if ((counter == 0) | (counter == 2)):
                            if len(y.find_all('a', href=True)) > 0:
                                ## If there's a link scrape it
                                for a in y.find_all('a', href=True):
                                    test.append(a['href'])
                            else:
                                test.append(' ')
                        counter +=1
                    ## The row count matches with the
                    ## number of actions take in congress
                    df.loc[int(test[0]),] = test
                page_num +=1
            else:
                next_page = False

        df['date'] = df['date'].apply(lambda x: str(
            datetime.datetime.strptime('{}-{}-{}'.format(x.split('-')[0],
                                                         x.split('-')[1],year), '%d-%b-%Y')))
        df.loc[:, 'congress'] = congress
        df.loc[:, 'session'] = session
        df.loc[:, 'roll'] = df.loc[:, 'roll'].astype(int)
        df.loc[:, 'roll_id'] = (df.loc[:, 'congress'].astype(str) + df.loc[:, 'session'].astype(str) +
                               df.loc[:, 'roll'].astype(str)).astype(int)

        self.house_vote_menu = df.sort_values('roll').reset_index(drop=True)
        
        
    def put_vote_menu(self):
        connection = open_connection()
        cursor = connection.cursor()

        for i in range(len(self.house_vote_menu)):
            ## Remove special character from the title
            try:
                self.house_vote_menu.loc[i, 'title_description'] = self.house_vote_menu.loc[i, 'title_description'].replace("'", "''")
            except:
                'hold'
            try:
                self.house_vote_menu.loc[i, 'title_description'] = self.house_vote_menu.loc[i, 'title_description'].encode('utf-8').replace('\xc3\xa1','a')
            except:
                'hold'
            try:
                self.house_vote_menu.loc[i, 'question'] = self.house_vote_menu.loc[i, 'question'].encode('utf-8').replace('\xc2\xa0', '')
            except:
                'hold'
            try:
                self.house_vote_menu.loc[i, 'title_description'] = self.house_vote_menu.loc[i, 'title_description'].replace('\xc2\xa0', '').encode('utf-8')
            except:
                'hold'
            x = list(self.house_vote_menu.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO house_vote_menu (
                roll, 
                roll_link, 
                date, 
                issue, 
                issue_link, 
                question,
                result, 
                title_description, 
                congress, 
                session, 
                roll_id)
                VALUES ('{roll}', '{roll_link}', '{date}', '{issue}',
                 '{issue_link}', '{question}', '{result}', '{title_description}',
                 '{congress}', '{session}', '{roll_id}');"""


            sql_command = format_str.format(roll=p[0], roll_link=p[1], 
                date=p[2], issue=p[3], issue_link=p[4], question=p[5], result=p[6],
                title_description=p[7], congress=p[8], session=p[9], roll_id=p[10])
            try:
                cursor.execute(sql_command)
                connection.commit()
            except:
                connection.rollback()
        connection.close()
        
    def daily_house_menu(self):
        """
        In this method I will be collecting the house vote menu
        for the entire current year. I will then compare the 
        highest roll call vote in the database to the collected
        data. If I have collected data that is not in the db
        then I'll insert the new data points. I will this save
        an attribute to say how many new rows were inserted
        to the db. That number will be included in the daily
        emails.
        """

        ## Connect to db
        connection = open_connection()

        ## Query db for max roll call for current year
        current_year = datetime.date.today().year

        sql_query = """
        SELECT max(roll) FROM house_vote_menu
        where date(date) >= '{}-01-01;'
        """.format(current_year)
        house_menu = pd.read_sql_query(sql_query, connection)

        ## Collect house vote menu for current year and compare
        vote_collector.house_vote_menu(self, current_year)
        self.house_vote_menu = self.house_vote_menu[self.house_vote_menu['roll'] > 
                                                    house_menu.loc[0,'max']].reset_index(drop=True)
        num_rows = len(self.house_vote_menu)
        
        if num_rows == 0:
            self.to_db = 'No new vote menu data.'
            print self.to_db
        if num_rows > 0:
            self.to_db = '{} new vote(s) in the data base.'.format(num_rows)
            print self.to_db
            vote_collector.put_vote_menu(self)

    def get_congress_votes(self):
        master_house_votes = pd.DataFrame()
        
        for i in range(len(self.house_vote_menu)):
            url = self.house_vote_menu.loc[i, 'roll_link']
            print url
            page =  requests.get(url)
            df = json_normalize(pd.DataFrame(
                    bf.data(fromstring(page.content))).loc['vote-data', 'rollcall-vote']['recorded-vote'])
            try:
                df.columns = ['member_full', 'bioguide_id', 'party', 'role', 'name', u'state', 'unaccented-name', 'vote']
                df = df[['member_full', 'bioguide_id', 'party', 'role', u'state', 'vote']]
            except:
                df.columns = ['member_full','party', 'role', 'state', 'vote'] 
                df.loc[:, 'bioguide_id'] = None
                df = df[['member_full', 'bioguide_id', 'party', 'role', u'state', 'vote']]

            df.loc[:, 'year'] = self.house_vote_menu.loc[i, 'date'].year
            df.loc[:, 'roll'] = self.house_vote_menu.loc[i, 'roll']
            df.loc[:, 'congress'] = self.house_vote_menu.loc[i, 'congress']
            df.loc[:, 'session'] = self.house_vote_menu.loc[i, 'session']
            df.loc[:, 'date'] = pd.to_datetime(
                json_normalize(
                    pd.DataFrame(
                        bf.data(
                            fromstring(page.content))).loc[
                        'vote-metadata', 'rollcall-vote']).loc[0, 'action-date.$'])

            master_house_votes = master_house_votes.append(df)

        ## Add roll_id
        master_house_votes['roll_id'] = (master_house_votes['congress'].astype(str) + 
        	master_house_votes['session'].astype(str) + 
        	master_house_votes['roll'].astype(str)).astype(int)

        ## Sanitize names
        master_house_votes['member_full'] = master_house_votes['member_full'].apply(lambda x: unidecode(x))
        master_house_votes['member_full'] = master_house_votes['member_full'].str.replace("'", "''")

        ## Save to attribute
        self.house_votes = master_house_votes.reset_index(drop=True)

    def house_votes_into_sql(self):
    	"""This method takes the house votes collected
    	and puts them in the database."""
    	
        connection = open_connection()
        cursor = connection.cursor()

        duplicated = 0

        self.house_votes.loc[:, 'date'] = self.house_votes.loc[:, 'date'].astype(int)
        self.house_votes.loc[:, 'member_full'] = self.house_votes.loc[:, 'member_full'].apply(lambda x: sanitize(x))
        self.house_votes['date'] = pd.to_datetime(self.house_votes['date'])

        self.house_votes = self.house_votes[['member_full', 'bioguide_id', 'party', 'role', 'state', 'vote', 'year', 'roll', 'congress', 'session', 'date', 'roll_id']]

        ## Put data into table
        for i in range(len(self.house_votes)):
            x = list(self.house_votes.loc[i,])

            for p in [x]:
                format_str = """INSERT INTO house_votes_tbl (
                member_full,
                bioguide_id,
                party,
                role,
                state,
                vote, 
                year, 
                roll,
                congress,
                session,
                date, 
                roll_id)
                VALUES ('{member_full}', '{bioguide_id}', '{party}', '{role}',
                 '{state}', '{vote}', '{year}', '{roll}', '{congress}', 
                 '{session}', '{date}', '{roll_id}');"""


                sql_command = format_str.format(member_full=p[0], bioguide_id=p[1], party=p[2],
                    role=p[3], state=p[4], vote=p[5], year=p[6],
                    roll=p[7], congress=p[8], session=p[9], date=p[10], roll_id=p[11])

                try:
                    cursor.execute(sql_command)
                    connection.commit()
                except:
                    duplicated += 1
                    connection.rollback()
        connection.close()
        if duplicated > 0:
            self.duplicate_entries = 'There were {} duplicaetes... But why?'.format(duplicated)
            print self.duplicate_entries

    def collect_missing_house_votes(self):
        """
        This method collects missing house votes
        by checking the max house votes collected
        and comparing that to the vote menu table.
        """
        print 'Getting house votes'

        ## Get the max date for roll call votes collected
        house_votes_max = str(pd.read_sql_query("""select max(date) from house_votes_tbl;""", 
                                            open_connection()).loc[0, 'max'])

        ## Get vote menu where date is greater than
        ## max roll call votes collcted
        self.house_vote_menu = pd.read_sql_query("""SELECT * 
        FROM house_vote_menu 
        where date > '{}';""".format(house_votes_max), open_connection())

        ## If there are votes to collect try to collect them
        if len(self.house_vote_menu) > 0:
            ## Collect missing roll call votes
            vote_collector.get_congress_votes(self)

            print 'add {} votes'.format(len(self.house_votes))
            ## Put in databse
            vote_collector.house_votes_into_sql(self)


    def __init__(self, house_vote_menu=None, to_db=None, house_votes=None):
        self.house_vote_menu = house_vote_menu
        self.to_db = to_db
        self.house_votes = house_votes
        self.duplicate_entries = "No duplicate vote entries."

class committee_collector(object):
    """
    This class will be used to collect committee data.
    What committees are there, what subcommittees are there,
    and whose apart of both of them.
    
    Attributes:
    committee_links - All different committees
    subcommittee_links - All different subcommittees
    committee_membership - Whose in what
    
    """
    
    def get_committees(self):
        """
        This method will be used to grab all of
        the house of representatives committees.
        """

        ## URL for house committees
        url = 'http://clerk.house.gov/committee_info/index.aspx'
        r = requests.get(url)
        page = BeautifulSoup(r.content, 'lxml')


        ## Find div where committees are held
        x = page.find_all('div', id='com_directory')[0].find_all('ul')
        a = str(x[0]).split('<li>')

        ## Set up dataframe to save to
        committee_links = pd.DataFrame()

        ## Loop through each committee and save name and url
        for i in range(1, len(a)):
            try:
                committee_links.loc[i, 'committee'] = a[i].split('">')[1].split('</a')[0]
                committee_links.loc[i, 'url'] = 'http://clerk.house.gov{}'.format(a[i].split('href="')[1].split('">')[0])
            except:
                "If there is no linke, then don't store"

        ## Loop started at 1, so df started at 1. Reset df index.
        self.committee_links = committee_links.reset_index(drop=True)
        
    def get_subcommittees(self):
        """
        This method will be used to grab all of
        the house of representatives subcommittees.
        """

        ## Set up master dataframe to save to
        master_subcommittees = pd.DataFrame()

        ## Loop through all master committees
        for committee in self.committee_links ['committee']:

            ## Find committee url to search for subcommittees
            committee_search = self.committee_links.loc[self.committee_links['committee'].str.lower() == committee.lower()].reset_index(drop=True)
            committee = committee_search.loc[0, 'committee']
            url = committee_search.loc[0, 'url']
            r = requests.get(url)
            page = BeautifulSoup(r.content, 'lxml')

            ## Split where the subcommittee list is
            x = page.find_all('div', id='subcom_list')[0].find_all('ul')

            ## Set up dataframe to save to
            subcommittee = pd.DataFrame()

            ## Loop through each subcommittee and save name and url
            if len(x):
                a = str(x[0]).split('<li>')

                for i in range(1, len(a)):
                    try:
                        subcommittee.loc[i, 'subcommittee'] = a[i].split('">')[1].split('</a')[0].strip('\t').strip('\n').strip('\r')
                        subcommittee.loc[i, 'url'] = 'http://clerk.house.gov{}'.format(a[i].split('href="')[1].split('">')[0])
                    except:
                        "If there is no linke, then don't store"

                ## Loop started at 1, so df started at 1. Reset df index.
                subcommittee.loc[:, 'committee'] = committee

            ## Append subcommittee data
            master_subcommittees = master_subcommittees.append(subcommittee)

        ## Save subcommittee data to class attribute
        self.subcommittee_links = master_subcommittees.reset_index(drop=True)
        
    def get_committee_memb(self, committee, subcommittee=None):
        """
        This method will be used to grab membership
        for committees and subcommittees.
        """

        ## Check if we are searching for committee or subcommittee.
        ## Subset the data set to search for url
        ## Grab committee and subcommittee names.
        ## Search URL
        if subcommittee == None:
            committee_search = self.committee_links.loc[self.committee_links['committee'].str.lower() == committee.lower()].reset_index(drop=True)
            committee = committee_search.loc[0, 'committee']
        elif subcommittee != None:
            committee_search = self.subcommittee_links.loc[((self.subcommittee_links['committee'].str.lower() == committee.lower()) &
                                                        (self.subcommittee_links['subcommittee'].str.lower() == subcommittee.lower()))].reset_index(drop=True)
            committee = committee_search.loc[0, 'committee']
            subcommittee = committee_search.loc[0, 'subcommittee']
        url = committee_search.loc[0, 'url']
        r = requests.get(url)
        page = BeautifulSoup(r.content, 'lxml')

        #### There are two columns of people. Search them separately. ####

        ## Set dataframe to save data to
        membership = pd.DataFrame()

        ## Section 1
        ## Find where data is
        x = page.find_all('div', id='primary_group')[0].find_all('ol')
        a = str(x[0]).split('<li>')

        ## Loop through all li items to find people.
        for i in range(1, len(a)):
            ## If vacancy then there is no person.
            if 'Vacancy' not in a[i]:
                ## Collect state short and district number
                state_dist = str(a[i]).split('statdis=')[1].split('">')[0]

                ## Split the string by number and letters
                split_sd = re.split('(\d+)', state_dist)
                for j in range(len(split_sd)):
                    if j == 0:
                        ## Letters is state short
                        state_short = str(split_sd[j])
                        membership.loc[i, 'state_short'] = state_short
                        state_long = str(us.states.lookup(state_short))
                        membership.loc[i, 'state_long'] = state_long
                    elif j == 1:
                        ## Numbers is district number
                        district_num = int(split_sd[j])
                        membership.loc[i, 'district_num'] = district_num
                ## Save member name and remove special charaters with unidecode
                ## no need to collect names for now
                # membership.loc[i, 'member_full'] = unidecode(str(a[i]).split('{}">'.format(state_dist))[1].split('</a>')[0].decode("utf8")).replace('A!', 'a').replace('A(c)', 'e').replace("'", "''")
                ## Clean position text
                position = str(a[i]).split(', {}'.format(state_short))[1].strip('</li>').strip('\n').strip('</o')
                ## If there is a position save it. Otherwise it's none.
                if position != '':
                    position = position.replace(', ', '').strip('</li>')
                    position = position.strip('\n').strip('</li>     ').strip('\n').strip('\r')
                    membership.loc[i, 'committee_leadership'] = position
                else:
                    membership.loc[i, 'committee_leadership'] = None

        ## Reset index so I can save to the proper index in the next loop
        membership = membership.reset_index(drop=True)

        ## Section 2
        ## Find where data is
        x = page.find_all('div', id='secondary_group')[0].find_all('ol')
        a = str(x[0]).split('<li>')

        ## Length of dataframe is where the index saving starts
        counter = len(membership)

        ## Loop through all li items to find people.
        for i in range(1, len(a)):
            if 'Vacancy' not in a[i]:
                ## Collect state short and district number
                state_dist = str(a[i]).split('statdis=')[1].split('">')[0]

                ## Split the string by number and letters
                split_sd = re.split('(\d+)', state_dist)
                for j in range(len(split_sd)):
                    if j == 0:
                        ## Letters is state short
                        state_short = str(split_sd[j])
                        membership.loc[counter, 'state_short'] = state_short
                        state_long = str(us.states.lookup(state_short))
                        membership.loc[counter, 'state_long'] = state_long
                    elif j == 1:
                        ## Numbers is district number
                        district_num = int(split_sd[j])
                        membership.loc[counter, 'district_num'] = district_num
                ## Save member name and remove special charaters with unidecode
                ## no need to collect names for now
                # membership.loc[counter, 'member_full'] = unidecode(str(a[i]).split('{}">'.format(state_dist))[1].split('</a>')[0].decode("utf8")).replace('A!', 'a').replace('A(c)', 'e').replace("'", "''")
                ## Clean position text
                position = str(a[i]).split(', {}'.format(state_short))[1].strip('</li>').strip('\n').strip('</o')
                ## If there is a position save it. Otherwise it's none.
                if position != '':
                    position = position.replace(', ', '').strip('</li>')
                    position = position.strip('\n').strip('</li>     ').strip('\n').strip('\r')
                    membership.loc[counter, 'committee_leadership'] = position
                else:
                    membership.loc[counter, 'committee_leadership'] = None
                ## Increase counter
                counter += 1
        ## If we found data then add committee and subcommittee details.
        if len(membership) > 0:
            membership.loc[:, 'committee'] = committee
            if subcommittee != None:
                membership.loc[:, 'subcommittee'] = subcommittee
            else:
                membership.loc[:, 'subcommittee'] = None
            membership = membership.reset_index(drop=True)
        return membership


    def get_all_membership(self):
        """
        This method will collect membership for all committees
        and subcommittees.
        """

        ## Make master dataframe for committees and subcommittees
        overall = self.committee_links.append(self.subcommittee_links).reset_index(drop=True)
        overall.loc[overall['subcommittee'].isnull(), 'subcommittee'] = None

        ## Set dataframe to save data to
        master_committees = pd.DataFrame()

        ## Loop through all committee urls.
        ## Append to master data set.
        for i in range(len(overall)):
            committee_grab = committee_collector.get_committee_memb(self, overall.loc[i, 'committee'], 
                                                subcommittee=overall.loc[i, 'subcommittee'])
            master_committees = master_committees.append(committee_grab)

        ## Save all scraped data to attribute
        self.committee_membership = master_committees.reset_index(drop=True)
        
    def membership_to_sql(self):
        """
        This method will be used to clean the collected
        data and put it into sql.
        """
        
        ## Connect
        connection = open_connection()
        cursor = connection.cursor()

        ## I'm going to get the bioguide_id from the bio tbale
        congress_bio = pd.read_sql_query("""SELECT * FROM congress_bio WHERE served_until = 'Present';""", connection)

        ## Join
        df = pd.merge(self.committee_membership, congress_bio[['bioguide_id', 'district', 'state']],
             left_on=['state_long', 'district_num'], right_on=['state', 'district']).drop_duplicates().reset_index(drop=True)
        df = df[['committee_leadership', 'committee', 'subcommittee', 'bioguide_id']]

        ## Clean columns
        df['committee'] = df['committee'].str.replace("'", "''")
        df['subcommittee'] = df['subcommittee'].str.replace("'", "''")

        ## delete 
        # I'm deleting to make sure we have the most
        # up-to-date reps. The collection is small
        # so it's not a bottle next to do this.
        try:
            cursor.execute("""DROP TABLE house_membership;""")
        except:
            'table did not exist'

        ## Create table
        sql_command = """
            CREATE TABLE house_membership (
            committee_leadership varchar(255), 
            committee varchar(255), 
            subcommittee varchar(255), 
            bioguide_id varchar(255),
            UNIQUE (committee, subcommittee, bioguide_id));"""

        cursor.execute(sql_command)
        connection.commit()

        print 'Inserting {} into house_membership'.format(len(df))
        ## Put each row into sql
        for i in range(len(df)):
            x = list(df.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO house_membership (
                committee_leadership, 
                committee, 
                subcommittee, 
                bioguide_id)
                VALUES ('{committee_leadership}', '{committee}', '{subcommittee}', '{bioguide_id}');"""


            sql_command = format_str.format(committee_leadership=p[0], committee=p[1], 
                subcommittee=p[2], bioguide_id=p[3])
            ## Commit to sql
            cursor.execute(sql_command)
            connection.commit()

        connection.close()

    def get_senate_committees(self):
        """
        This method will be used to find all
        of the senate committees and the xml
        urls for them.
        """
        
        ## Make df to save to
        master_df = pd.DataFrame()
        
        ## Save payload options to send stack to senate.gov
        ## The senate does a good job trying to blacklist scraper IPs
        payload = {
            "Host": "www.mywbsite.fr",
            "Connection": "keep-alive",
            "Content-Length": 129,
            "Origin": "https://www.mywbsite.fr",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Referer": "https://www.mywbsite.fr/data/mult.aspx",
            "Accept-Encoding": "gzip,deflate,sdch",
            "Accept-Language": "fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4",
            "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.3",
            "Cookie": "ASP.NET_SessionId=j1r1b2a2v2w245; GSFV=FirstVisit=; GSRef=https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&ved=0CHgQFjAA&url=https://www.mywbsite.fr/&ei=FZq_T4abNcak0QWZ0vnWCg&usg=AFQjCNHq90dwj5RiEfr1Pw; HelpRotatorCookie=HelpLayerWasSeen=0; NSC_GSPOUGS!TTM=ffffffff09f4f58455e445a4a423660; GS=Site=frfr; __utma=1.219229010.1337956889.1337956889.1337958824.2; __utmb=1.1.10.1337958824; __utmc=1; __utmz=1.1337956889.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)"
        }
        headers = {}
        
        ## Request url
        url = 'https://www.senate.gov/committees/membership.htm'
        r = requests.get(url, data=dumps(payload), headers=headers)
        page = BeautifulSoup(r.content, 'lxml')
        
        ## Split page to dropdown options
        drop_down_options = page.find_all('select', {"name":"dropDownJump"})[0].find_all('option')

        ## Collect all committees
        for i in range(1, len(drop_down_options)):
            master_df.loc[i, 'committee'] = drop_down_options[i].text.replace("'", "")
            master_df.loc[i, 'url'] = 'https://www.senate.gov{}'.format(drop_down_options[i].get('value')).replace('htm', 'xml')
            
        self.senate_urls = master_df.reset_index(drop=True)

    def get_senate_membership(self):
        """
        This method gets the membership for each
        senate committee and subcommittee.
        """
        
        
        ## Save payload options to send stack to senate.gov
        ## The senate does a good job trying to blacklist scraper IPs
        payload = {
            "Host": "www.mywbsite.fr",
            "Connection": "keep-alive",
            "Content-Length": 129,
            "Origin": "https://www.mywbsite.fr",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Referer": "https://www.mywbsite.fr/data/mult.aspx",
            "Accept-Encoding": "gzip,deflate,sdch",
            "Accept-Language": "fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4",
            "Accept-Charset": "ISO-8859-1,utf-8;q=0.7,*;q=0.3",
            "Cookie": "ASP.NET_SessionId=j1r1b2a2v2w245; GSFV=FirstVisit=; GSRef=https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&ved=0CHgQFjAA&url=https://www.mywbsite.fr/&ei=FZq_T4abNcak0QWZ0vnWCg&usg=AFQjCNHq90dwj5RiEfr1Pw; HelpRotatorCookie=HelpLayerWasSeen=0; NSC_GSPOUGS!TTM=ffffffff09f4f58455e445a4a423660; GS=Site=frfr; __utma=1.219229010.1337956889.1337956889.1337958824.2; __utmb=1.1.10.1337958824; __utmc=1; __utmz=1.1337956889.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)"
        }
        headers = {}
        
        try:
            ## Request url
            r = requests.get(self.url, data=dumps(payload), headers=headers)
            page = BeautifulSoup(r.content, 'lxml')

            ## Clean data
            x = ElementTree.fromstring(r.content)
            x = bf.data(x)
            x_normalized = json_normalize(pd.DataFrame(x).loc['committees', 'committee_membership'])
        
            ## Return Data
            membership_df = json_normalize(x_normalized.loc[0, 'members.member'])
            membership_df['committee'] = x_normalized.loc[0, 'committee_name.$']

            try: 
                """
                In this section I'll search for subcommittee membership.
                I'm going to find all of the subcommittee members,
                add a column for the overall committee and a column
                for the subcommittee. I none exist then skip to except
                and add none. At the end rename the columns.
                """

                ## Datarame to save to
                sub_df_master = pd.DataFrame()

                ## Split where subcommittees are
                search_sub = json_normalize(x_normalized.loc[0, 'subcommittee'])

                ## Loop though all subcommittees to get membership and names.
                for i in range(len(search_sub)):
                    sub_df = json_normalize(search_sub.loc[i, 'members.member'])
                    subcommittee = search_sub.loc[i, 'subcommittee_name.$']

                    sub_df['committee'] = x_normalized.loc[0, 'committee_name.$']
                    sub_df.loc[:, 'subcommittee'] = subcommittee
                    sub_df_master = sub_df_master.append(sub_df)

                ## Append all to master and clean column names.
                membership_df = membership_df.append(sub_df_master)
                column_names = membership_df.columns.str.strip('.$').str.replace('.', '_')
                membership_df.columns = [column_names]
            except:
                membership_df['subcommittee'] = None
                column_names = membership_df.columns.str.strip('.$').str.replace('.', '_')
                membership_df.columns = [column_names]

            ## Change position to leadership and drop members info.
            ## Obvi they're a member if they show up. I care about
            ## who is in leadership positions.
            membership_df.loc[:, 'committee_leadership'] = membership_df.loc[:, 'position']
            membership_df.loc[membership_df['committee_leadership'] == 'Member', 'committee_leadership'] = None
            membership_df = membership_df.drop(['position'], 1)
        
        except:
            print 'getting the url except'
            ## Request url
            self.url = self.url.replace('xml', 'htm')
            r = requests.get(self.url, data=dumps(payload), headers=headers)
            page = BeautifulSoup(r.content, 'lxml')
            
            membership_df = pd.DataFrame()
            committee_name = str(page.find_all('span', class_='contenttitle')[0]).split('<committee_name>')[1].split('</committee_name>')[0]
            member_section = page.find_all('table', class_='contenttext')

            for i in range(len(member_section)):
                if "Members:" in member_section[i].text:
                    split_section = member_section[1].find_all('td', valign="top")           

            each_member = str(split_section[0]).strip('<td nowrap="" valign="top">\n').split('\n\t\t\t\t\t\n\t\t\t\t\t')
            ## There are two formats
            had_leadership = False
            counter = 0
            ## There's dead space at the end so subtract by 1
            for k in range(len(each_member)-1):
                if had_leadership == False:
                    membership_df.loc[counter, 'name_first'] = each_member[k].split('last>')[1].split('</pub')[0]
                    membership_df.loc[counter, 'name_last'] = each_member[k].split('first>')[1].split('</pub')[0]
                    membership_df.loc[counter, 'state'] = each_member[k].split('<state>')[1].split('</state>')[0]
                    try :
                        membership_df.loc[counter, 'committee_leadership'] = each_member[k+1].split('<position>')[1].split('</position>')[0]
                        had_leadership = True
                    except:
                        membership_df.loc[counter, 'committee_leadership'] = None
                    counter += 1
                elif had_leadership == True:
                    """If the previous person had leadership
                    then I only got it from k+1 in the index.
                    That means this index is still the leadership.
                    Skip and collect next person."""
                    had_leadership = False

            each_member = str(split_section[1]).strip('<td nowrap="" valign="top">\n').split('<br/>')

            ## There's dead space at the end so subtract by 1
            for k in range(len(each_member)-1):
                membership_df.loc[counter, 'name_first'] = each_member[k].split(',')[0]
                membership_df.loc[counter, 'name_last'] = each_member[k].split(', ')[1].split(' (')[0]
                membership_df.loc[counter, 'state'] = each_member[k].split(' (')[1].split(')')[0]
                try :
                    membership_df.loc[counter, 'committee_leadership'] = each_member[k].split('<position>')[1].split('</position>')[0]
                except:
                    membership_df.loc[counter, 'committee_leadership'] = None
                counter += 1
                
            membership_df['committee'] = committee_name
            
        return membership_df.reset_index(drop=True)

    def collect_senate_committee_membership(self):
        senate_committee_membership = pd.DataFrame()

        for url in self.senate_urls['url']:
            print url
            try:
                self.url = url
                single_membership = committee_collector.get_senate_membership(self)
                senate_committee_membership = senate_committee_membership.append(single_membership)
            except:
                print 'that url didnt work'
        senate_committee_membership.loc[senate_committee_membership['subcommittee'].isnull(), 'subcommittee'] = None

        senate_committee_membership['state_short'] = senate_committee_membership['state']
        senate_committee_membership['state'] = senate_committee_membership['state_short'].apply(lambda x: str(us.states.lookup(x)))

        senators_bio = pd.read_sql_query("""SELECT * FROM congress_bio WHERE served_until = 'Present' and chamber = 'senate';""", open_connection())
        senators_bio['name'] = senators_bio['name'].str.strip()
        senators_bio['name_last'] = senators_bio['name'].apply(lambda x: x.split(',')[0])

        mappings_df = pd.DataFrame()
        x = senate_committee_membership[['name_last', 'state']].drop_duplicates().reset_index(drop=True)

        for i in range(len(x)):
            name = x.loc[i, 'name_last']
            state = x.loc[i, 'state']

            bio_search = pd.read_sql_query("""
            SELECT bioguide_id 
            FROM congress_bio 
            WHERE served_until = 'Present' 
            AND chamber = 'senate'
            AND name ilike '%' || '{}' || '%'
            AND state = '{}'
            """.format(name.split(' ')[0],
                      state), open_connection())

            mappings_df.loc[i, 'name_last'] = name
            mappings_df.loc[i, 'state'] = state
            mappings_df.loc[i, 'bioguide_id'] = bio_search.loc[0,'bioguide_id']

        senate_committee_membership = pd.merge(senate_committee_membership, mappings_df,
                 how='left', on=['name_last', 'state'])
        senate_committee_membership = senate_committee_membership[['committee_leadership', 'committee', 'subcommittee', 'bioguide_id']].reset_index(drop=True)
        
        self.committee_membership = senate_committee_membership

    def senate_membership_to_sql(self):
        """
        This method will be used to clean the collected
        data and put it into sql.
        """
        ## Connect
        connection = open_connection()
        cursor = connection.cursor()
        
        ## delete 
        # I'm deleting to make sure we have the most
        # up-to-date reps. The collection is small
        # so it's not a bottle next to do this.
        try:
            cursor.execute("""DROP TABLE senate_membership;""")
        except:
            'table did not exist'

        cursor = connection.cursor()
        sql_command = """
            CREATE TABLE senate_membership (
            committee_leadership varchar(255),
            committee varchar(255),  
            subcommittee varchar(255), 
            bioguide_id varchar(255),
            UNIQUE (committee, subcommittee, bioguide_id));"""

        cursor.execute(sql_command)
        connection.commit()

        ## Connect
        connection = open_connection()
        cursor = connection.cursor()

        ## Clean columns
        self.committee_membership.loc[self.committee_membership['committee'].notnull(), 'committee'] = self.committee_membership.loc[self.committee_membership['committee'].notnull(), 'committee'].apply(lambda x: sanitize(x))
        self.committee_membership.loc[self.committee_membership['subcommittee'].notnull(), 'subcommittee'] = self.committee_membership.loc[self.committee_membership['subcommittee'].notnull(), 'subcommittee'].apply(lambda x: sanitize(x))

        ## Put each row into sql
        for i in range(len(self.committee_membership)):
            x = list(self.committee_membership.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO senate_membership (
                committee_leadership, 
                committee, 
                subcommittee, 
                bioguide_id)
                VALUES ('{committee_leadership}', '{committee}', '{subcommittee}', '{bioguide_id}');"""


            sql_command = format_str.format(committee_leadership=p[0], committee=p[1], 
                subcommittee=p[2], bioguide_id=p[3])
            ## Commit to sql
            cursor.execute(sql_command)
            connection.commit()

        connection.close()
    
    
    def __init__(self, committee_links=None, subcommittee_links=None, all_committee_links=None, 
        committee_membership=None, senate_urls=None, url=None):
        self.committee_links = committee_links
        self.subcommittee_links = subcommittee_links
        self.committee_membership = committee_membership
        self.senate_urls = senate_urls
        self.url = url

class sponsorship_collection(object):
    """
    This class is used to collect sponsorship
    information for all bills in the house and
    senate. The collector will search for all
    legislation drafted this year (which is held
    in the database). It will then search all of
    that legislation on congress.gov to get the
    sponsor and cosponsor data. Finially, it will
    insert new data and update older data.
    
    The reason that I am collecting for the whole
    year and collecting old data is because new 
    cosponsors can join a bill at later data, and
    some cosponsors decided they no longer wish
    to cospons.
    
    Attributes:
    Sponsorship data - The information to put in the db
    New data - How many new data points were added
    Updated data - How many data poins were updated
    """

    def current_congress_num(self):
        """
        This method will be used to find the
        maximum congresss number. The max
        congress will be the current congress.
        """
        
        cong_num = pd.read_sql_query("""select max(congress) from house_vote_menu;""",open_connection())
        self.congress_search = cong_num.loc[0, 'max']
    
    def get_sponsor_data(self):
        """
        This method is used to collect
        the sponsorship and cosponsorhip
        from a given URL. If no sponsorship
        and or cosponsorship exists then
        return None.
        """


        ## Get prxoy IP address
        spoof_df = Ip_Spoofer.random_ip()

        ## Request with proxy IP address
        s = requests.session()
        a = requests.adapters.HTTPAdapter(max_retries=5)
        s.mount('http://', a)
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        s.headers.update(headers)
        url = '{}/cosponsors'.format(self.search_url)
        r = s.get(url)
        page = BeautifulSoup(r.content, 'lxml')


        # try:
        tr_page = page.find('div', class_='overview_wrapper bill').find_all('tr')
        for i in range(len(tr_page)):
            if 'Sponsor:' in tr_page[i].text:
                sponsor = tr_page[i].find('a').get('href').split('/')[-1]
        sponsors_df = pd.DataFrame([self.search_url, sponsor]).transpose()
        sponsors_df.columns = ['url', 'bioguide_id']

        try:
            ## If there are cosponsors
            cosponsors_id = []
            cosponsors_name = []
            cosponsors_date = []

            loop_max = len(page.find_all('div', class_='col2_lg basic-search-results nav-on')[0].find_all('td', class_='date'))
            date_split = page.find_all('div', class_='col2_lg basic-search-results nav-on')[0]

            for i in range(1, loop_max):
                cosponsors_id.append(str(date_split.find_all('a', target='_blank')[i].get('href').split('/')[-1]))
                cosponsors_name.append(str(str(date_split.find_all('a', target='_blank')[i].text)))
                cosponsors_date.append(str(date_split.find_all('td', class_='date')[i].text))
        except:
            "either nothing or something weird"

        try:
            cosponsor_df = pd.DataFrame([cosponsors_id, cosponsors_name, cosponsors_date]).transpose()
            cosponsor_df.columns = ['bioguide_id', 'member_full', 'date_cosponsored']

            sponsors_df.loc[:, 'cosponsor_bioguide_id'] = pd.Series(list(cosponsor_df['bioguide_id']))
            sponsors_df.set_value(0, 'cosponsor_bioguide_id', (list(cosponsor_df['bioguide_id'])))

            sponsors_df.loc[:, 'cosponsor_member_full'] = pd.Series(list(cosponsor_df['member_full']))
            sponsors_df.set_value(0, 'cosponsor_member_full', (list(cosponsor_df['member_full'])))

            sponsors_df.loc[:, 'date_cosponsored'] = pd.Series(list(cosponsor_df['date_cosponsored']))
            sponsors_df.set_value(0, 'date_cosponsored', (list(cosponsor_df['date_cosponsored'])))

            ## Remove single quotes.
            ## I tried in single data collection but they still showed up
            sponsors_df.loc[:, 'cosponsor_bioguide_id'] = sponsors_df.loc[:, 'cosponsor_bioguide_id'].apply(lambda x: str(x).replace("'", ""))
            sponsors_df.loc[:, 'cosponsor_member_full'] = sponsors_df.loc[:, 'cosponsor_member_full'].apply(lambda x: str(x).replace("'", ""))
            sponsors_df.loc[:, 'date_cosponsored'] = sponsors_df.loc[:, 'date_cosponsored'].apply(lambda x: str(x).replace("'", ""))
        except:
            ## No cosponsors
            sponsors_df.loc[0, 'cosponsor_bioguide_id'] = None
            sponsors_df.loc[0, 'cosponsor_member_full'] = None
            sponsors_df.loc[0, 'date_cosponsored'] = None


        return sponsors_df
    
    def sponsor_to_sql(self):
        """
        This is used to put the collected
        sponsorship data into the database.
        I am collecting metrics on what was
        new and what was updated for emails
        reports.
        """
        
        connection = open_connection()
        cursor = connection.cursor()
        new_data = 0
        updated_data = 0


        ## Put each row into sql
        for i in range(len(self.master_sponsors)):
            x = list(self.master_sponsors.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO bill_sponsors (
                url, 
                bioguide_id, 
                cosponsor_bioguide_id,
                cosponsor_member_full,
                date_cosponsored)
                VALUES ('{url}', '{bioguide_id}', '{cosponsor_bioguide_id}',
                        '{cosponsor_member_full}', '{date_cosponsored}');"""


            sql_command = format_str.format(url=p[0], bioguide_id=p[1], cosponsor_bioguide_id=p[2],
                                           cosponsor_member_full=p[3], date_cosponsored=p[4])
            # Commit to sql
            try:
                cursor.execute(sql_command)
                connection.commit()
                new_data += 1
            except:
                ## Update what I got
                connection.rollback()
                if ((self.master_sponsors.loc[i, 'bioguide_id'] != None) |
                    (self.master_sponsors.loc[i, 'bioguide_id'] != 'None')):
                    sql_command = """UPDATE bill_sponsors 
                    SET
                    bioguide_id = '{}',
                    cosponsor_bioguide_id = '{}',
                    cosponsor_member_full = '{}',
                    date_cosponsored = '{}'
                    WHERE url = '{}';""".format(
                    self.master_sponsors.loc[i, 'bioguide_id'],
                    self.master_sponsors.loc[i, 'cosponsor_bioguide_id'],
                    self.master_sponsors.loc[i, 'cosponsor_member_full'],
                    self.master_sponsors.loc[i, 'date_cosponsored'],
                    self.master_sponsors.loc[i, 'url'])    
                    cursor.execute(sql_command)
                    connection.commit()
                    updated_data += 1
                else:
                    print '{} had no sponsor.'.format(self.master_sponsors.loc[i, 'url'])

        connection.close()
        self.new_data = new_data
        self.updated_data = updated_data
        
    def collect_sponsorship(self):
        """
        This method will be used to collected
        the sponsorship of each bill. Since I don't
        know how far along a bill is without going to
        the congress.gov page, I'm just going to
        recollect for now.
        """

        ## Get current congress's legislation

        sql_query = """SELECT * 
        FROM all_legislation 
        WHERE congress = '{}'""".format(self.congress_search)

        all_legislation = pd.read_sql_query(sql_query, open_connection())
        unique_legislation = np.unique(all_legislation.loc[all_legislation['issue_link'] != ' ', 'issue_link'])

        print 'Collect sponsorship data :P'
        master_sponsors = pd.DataFrame()
        not_collect = 0
        for url in unique_legislation:
            self.search_url = url
            try:
                master_sponsors = master_sponsors.append(sponsorship_collection.get_sponsor_data(self))
            except:
                print 'bad url: {}'.format(self.search_url)
                not_collect += 1
        
        self.master_sponsors = master_sponsors.reset_index(drop=True)
        print 'To the database!'
        print 'Something was wrong collecting {} bills'.format(not_collect)
        sponsorship_collection.sponsor_to_sql(self)
        
    def __init__(self, search_url=None, master_sponsors=None, new_data=None, updated_data=None,
        congress_search=None):
        self.search_url = search_url
        self.master_sponsors = master_sponsors
        self.new_data = new_data
        self.updated_data = updated_data
        self.congress_search = congress_search

class collect_legislation(object):
    """
    This class will be used to collect legislation
    for the congress. The primary purpose will be 
    to collect and hosue.
    
    Attributes:
    legislation_by_congress - The legislation collected
    congress_search - The congression I want to find legislation for
    new_data - New data put into db
    updated_data - Number of data updated in db
    """
    
    
    def legislation_info_by_congress(self):
    
        ## Master dasta set to save to
        master_df = pd.DataFrame()

        data_collected = False
        while data_collected == False:
            try:
                ## Get prxoy IP address
                spoof_df = Ip_Spoofer.random_ip()

                ## Request with proxy IP address
                s = requests.session()
                a = requests.adapters.HTTPAdapter(max_retries=5)
                s.mount('http://', a)
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
                s.headers.update(headers)
                url = 'https://www.congress.gov/search?q=%7B"congress":"{}","source":"legislation"%7D&searchResultViewType=expanded&pageSize=250&page=1'.format(self.congress_search)
                print url
                r = s.get(url)
                print r.status_code
                page = BeautifulSoup(r.content, 'lxml')
                print page

                max_page = int(page.find('div', 
                      class_='nav-pag-top').find(
                'div', class_='pagination').find_all(
                'a')[-1].get('href').split('page=')[1])

                data_collected = True
            except:
                data_collected = False

        for i in range(1, max_page+1):
            page_df = pd.DataFrame()
            if i != 1:

                data_collected = False
                while data_collected == False:
                    try:
                        ## Request next page
                        url = 'https://www.congress.gov/search?q=%7B"congress":"{}","source":"legislation"%7D&searchResultViewType=expanded&pageSize=250&page={}'.format(self.congress_search, i) 
                        ## Get prxoy IP address
                        spoof_df = Ip_Spoofer.random_ip()

                        ## Request with proxy IP address
                        s = requests.session()
                        a = requests.adapters.HTTPAdapter(max_retries=5)
                        s.mount('http://', a)
                        s.headers.update(headers)
                        print url
                        r = s.get(url)
                        print r.status_code
                        
                        page = BeautifulSoup(r.content, 'lxml')
                        data_collected = True
                    except:
                        data_collected = False

            ## Get legislation container
            page_list = page.find_all('ol', class_='basic-search-results-lists expanded-view')[0]

            ## Get list of legislation
            page_list_expanded = page_list.find_all('li', class_='expanded')

            for j in range(len(page_list_expanded)):
                page_df.loc[j, 'issue_link'] = page_list_expanded[j].find_all(
                    'span', class_='result-heading')[0].find('a').get('href').split('?')[0]

                page_df.loc[j, 'issue'] = str(page_list_expanded[j].find_all(
                    'span', class_='result-heading')[0].find('a').text)

                try:
                    page_df.loc[j, 'title_description'] = unidecode(page_list_expanded[j].find_all(
                        'span', class_='result-title')[0].text).replace("'", "''")
                except:
                    page_df.loc[j, 'title_description'] = None

                try:
                    if 'Committees:' in page_list_expanded[j].find_all(
                        'span', class_='result-item')[1].text: 

                        committee_stuff = page_list_expanded[j].find_all(
                            'span', class_='result-item')[1].text.strip('\nCommittees:')
                        page_df.loc[j, 'committees'] = unidecode(committee_stuff.strip()).replace("'", "''")
                    else: 
                        page_df.loc[j, 'committees'] = None
                except:
                    page_df.loc[j, 'committees'] = None

                try:
                    page_df.loc[j, 'tracker'] = str(page_list_expanded[j].find_all(
                        'span', class_='result-item')[-1].find(
                        'li', class_='selected').text.split('Array')[0])
                except:
                    page_df.loc[j, 'tracker'] = None
            master_df = master_df.append(page_df)
        master_df = master_df.reset_index(drop=True)
        master_df.loc[:, 'congress'] = self.congress_search
        self.legislation_by_congress = master_df
        
    def legislation_to_sql(self):
        connection = open_connection()
        cursor = connection.cursor()
        
        new_data = 0
        updated_data = 0

        ## Put each row into sql
        for i in range(len(self.legislation_by_congress)):
            x = list(self.legislation_by_congress.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO all_legislation (
                issue_link, 
                issue, 
                title_description,
                committees,
                tracker,
                congress,
                policy_area,
                legislative_subjects)
                VALUES ('{issue_link}', '{issue}', '{title_description}',
                        '{committees}', '{tracker}', '{congress}',
                        '{policy_area}', '{legislative_subjects}');"""


            sql_command = format_str.format(issue_link=p[0], issue=p[1], title_description=p[2],
                                           committees=p[3], tracker=p[4], congress=p[5],
                                           policy_area='collect', legislative_subjects='collect')
            ## Commit to sql
            try:
                cursor.execute(sql_command)
                connection.commit()
                new_data += 1
            except:
                ## Update what I got
                connection.rollback()
                sql_command = """UPDATE all_legislation 
                SET
                issue = '{}',
                title_description = '{}',
                committees = '{}',
                tracker = '{}'
                WHERE (issue_link = '{}'
                and congress = '{}');""".format(
                self.legislation_by_congress.loc[i, 'issue'],
                self.legislation_by_congress.loc[i, 'title_description'],
                self.legislation_by_congress.loc[i, 'committees'],
                self.legislation_by_congress.loc[i, 'tracker'],
                self.legislation_by_congress.loc[i, 'issue_link'],
                self.legislation_by_congress.loc[i, 'congress'])    
                cursor.execute(sql_command)
                connection.commit()
                updated_data += 1

        connection.close()
        print 'Data put into sql - New: {}, Updated: {}'.format(new_data, updated_data)
        self.new_data = new_data
        self.updated_data = updated_data


    def bill_subjects(self):    
        ## url for bill text
        headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'
            }
            
        ## url for bill 
        r = requests.get(self.url + '/subjects', headers=headers)
        page = BeautifulSoup(r.content, 'lxml')
        
        master_df = pd.DataFrame()

        if "page not found" in page.text.lower():
            master_df = pd.DataFrame([[None], [None], [self.url]]).transpose()
            master_df.columns = ['policy_area', 'legislative_subjects', 'issue_link']
        
        else:
            try:
                ## Collect policy area
                policy_area = page.find('div', class_='col2_sm').findAll('li')
                policy_area_dict = {}

                for i in range(len(policy_area)):
                    policy_area_dict.update({i: str(policy_area[i].text).strip()})
                    
                    
                ## Collect subjects
                legislative_subjects = page.find('div', class_='col2_lg').find('ul').findAll('li')
                legislative_subjects_dict = {}

                for i in range(len(legislative_subjects)):
                    legislative_subjects_dict.update({i: str(legislative_subjects[i].text).strip()})
                
                ## Set array data to data set
                master_df = pd.DataFrame([[policy_area_dict], [legislative_subjects_dict], [self.url]]).transpose()
                master_df.columns = ['policy_area', 'legislative_subjects', 'issue_link']

                ## Clean empty data
                check_cols = ['policy_area', 'legislative_subjects']

                for col in check_cols:
                    if master_df.loc[0, col] == {}:
                        master_df.loc[0, col] = None
            except:
                "There's not data to collect"
                master_df = pd.DataFrame([[None], [None], [self.url]]).transpose()
                master_df.columns = ['policy_area', 'legislative_subjects', 'issue_link']
        
        return master_df

    def subjects_to_collect(self):
        """
        THis method will be used to find the bills that
        are missing subjects and policy areas.
        """

        self.bills_to_get_df = pd.read_sql_query("""SELECT * FROM all_legislation
        where policy_area = 'collect';""", open_connection())

        for i in range(len(self.bills_to_get_df)):
            try:
                self.url = self.bills_to_get_df.loc[i, 'issue_link']
                self.bill_subjects_df = collect_legislation.bill_subjects(leg_collection)
                collect_legislation.policy_subjects_to_sql(leg_collection)
            except:
                print "did not work:"
                print leg_collection.url

    def policy_subjects_to_sql(self):
        connection = open_connection()
        cursor = connection.cursor()
        for i in range(len(self.bill_subjects_df)):
            sql_command = """UPDATE all_legislation 
            SET
            policy_area = '{}',
            legislative_subjects = '{}'
            WHERE (issue_link = '{}');""".format(
            sanitize(self.bill_subjects_df.loc[i, 'policy_area']),
            sanitize(self.bill_subjects_df.loc[i, 'legislative_subjects']),
            sanitize(self.bill_subjects_df.loc[i, 'issue_link']))
            cursor.execute(sql_command)
            connection.commit()
        connection.close()

    def daily_subject_collection(self):
        """
        This method will be used to check if new bill come in
        and collect the subjects and policy areas.
        """

        new_data = 0
        get_data = pd.read_sql_query("""SELECT * FROM all_legislation
            where policy_area = 'collect';""", open_connection())

        for i in range(len(get_data)):
            try:
                self.url = get_data.loc[i, 'issue_link']
                self.bill_subjects_df = collect_legislation.bill_subjects(self)
                collect_legislation.policy_subjects_to_sql(self)
                new_data += 1
            except:
                print "did not work: {}".format(i)
                print self.url

        print 'Data put into sql - New: {}'.format(new_data)
        self.new_data = new_data


        
    def __init__(self, legislation_by_congress=None, congress_search=None,
                new_data=None, updated_data=None, url=None, bill_subjects_df=None,
                bills_to_get_df=None):
        self.legislation_by_congress = legislation_by_congress
        self.congress_search = congress_search
        self.new_data = new_data
        self.updated_data = updated_data
        self.url = url
        self.bill_subjects_df = bill_subjects_df
        self.bills_to_get_df = bills_to_get_df


class Performance(object):
    """
    This class generates the performance
    metrics for each representative. 
    It will allow us to calculate number of
    days they have been at work, number of
    bills they have voted on, how many bills
    they have sponsors & cosponsored,
    and the number of bills they have helped
    draft that became law.
    
    Attributes:
    
    """
    
    def current_congress_num(self):
        """
        This method will be used to find the
        maximum congresss number. The max
        congress will be the current congress.
        """
        
        cong_num = pd.read_sql_query("""select max(congress) from house_vote_menu;""",open_connection())
        self.congress_num = cong_num.loc[0, 'max']
        
    def num_days_voted_house(self):
        """
        This method will be used to find the
        number of days a house rep has voted on
        legislatoin and compare it to the
        total number of days that roll call
        voting happened.

        This metric will be a proxy for 
        days showing up to work.
        """

        days_voted = pd.read_sql_query("""
                    SELECT DISTINCT bioguide_id, date
                    FROM house_votes_tbl
                    where congress = {}
                    AND bioguide_id = '{}'
                    AND vote != 'Not Voting';
                    """.format(self.congress_num,
                               self.bioguide_id), open_connection())

        total_days = pd.read_sql_query("""
                    SELECT DISTINCT(date) as total_work_days 
                    FROM house_votes_tbl 
                    WHERE congress = {}
                    ORDER BY total_work_days
                    ;
                    """.format(self.congress_num),open_connection())

        if len(total_days) > 5:
            min_date = pd.read_sql_query("""
                        SELECT MIN(date) FROM (
                        SELECT DISTINCT bioguide_id, date
                        FROM house_votes_tbl
                        where congress = {}
                        AND bioguide_id = '{}'
                        AND vote != 'Not Voting')
                        AS dates;
                        """.format(self.congress_num,
                                   self.bioguide_id), open_connection())
            if min_date.loc[0, 'min'] > total_days.loc[5, 'total_work_days']:                
                days_voted = pd.DataFrame()
                days_voted.loc[0, 'days_at_work'] = len(days_voted)
                days_voted.loc[:, 'total_work_days'] = len(total_days.loc[total_days['total_work_days'] >= min_date.loc[0, 'min']])
                days_voted['percent_at_work'] = (days_voted['days_at_work']/
                                                 days_voted['total_work_days'])
                self.days_voted = days_voted
                return 

        vote_dates = pd.read_sql_query("""
                    SELECT COUNT(DISTINCT(date)) as total_work_days 
                    FROM house_votes_tbl 
                    WHERE congress = {};
                    """.format(self.congress_num),open_connection())

        days_voted = pd.read_sql_query("""
                    SELECT distinct_votes.bioguide_id, 
                    count(distinct_votes.bioguide_id) as days_at_work
                    FROM (
                    SELECT DISTINCT bioguide_id, date
                    FROM house_votes_tbl
                    where congress = {}
                    AND bioguide_id = '{}'
                    AND vote != 'Not Voting')
                    as distinct_votes
                    GROUP BY bioguide_id;
                    """.format(self.congress_num,
                               self.bioguide_id), open_connection())

        ## Join and get percent
        days_voted.loc[:, 'total_work_days'] = vote_dates.loc[0, 'total_work_days']
        days_voted['percent_at_work'] = (days_voted['days_at_work']/
                                         days_voted['total_work_days'])

        self.days_voted = days_voted
        
    def num_days_voted_senate(self):
        """
        This method will be used to find the
        number of days a senator has voted on
        legislatoin and compare it to the
        total number of days that roll call
        voting happened.
        
        This metric will be a proxy for 
        days showing up to work.
        """

        find_senator = pd.read_sql("""
        SELECT * 
        FROM congress_bio 
        WHERE chamber = 'senate'
        AND bioguide_id = '{}'""".format(
                self.bioguide_id), open_connection())

        state_short = us.states.mapping('name', 'abbr')[find_senator.loc[0, 'state']]
        last_name = find_senator.loc[0, 'name'].split(',')[0]

        days_voted = pd.read_sql_query("""
        SELECT distinct_votes.last_name, 
        COUNT(distinct_votes.last_name) as days_at_work
        FROM (SELECT DISTINCT last_name, date
        FROM senator_votes_tbl
        WHERE congress = {}
        AND last_name ilike '%' || '{}' || '%'
        AND state = '{}'
        AND vote_cast != 'Not Voting')
        AS distinct_votes
        GROUP BY last_name;""".format(
                self.congress_num,
                last_name, state_short),
                                       open_connection())

        vote_dates = pd.read_sql_query("""
            SELECT COUNT(DISTINCT(date)) as total_work_days 
            FROM senator_votes_tbl 
            WHERE congress = {};""".format(
                self.congress_num), open_connection())

        ## Join and get percent
        days_voted.loc[:, 'total_work_days'] = vote_dates.loc[0, 'total_work_days']
        days_voted['percent_at_work'] = (days_voted['days_at_work']/
                                         days_voted['total_work_days'])

        self.days_voted = days_voted
        
    def num_votes_house(self):
        """
        This method will be used to find the
        total number of times a rep has voted
        and compare it to the total number
        of roll call votes for the congress.
        """
        
        rep_votes = pd.read_sql_query("""
        SELECT COUNT(vote) as rep_votes
        FROM house_votes_tbl
        where congress = {}
        AND bioguide_id = '{}'
        AND vote != 'Not Voting';
        """.format(self.congress_num,
                   self.bioguide_id), open_connection())
        
        total_votes = pd.read_sql_query("""
        SELECT COUNT(DISTINCT(roll_id)) total_votes
        FROM house_votes_tbl
        WHERE congress = {};
        """.format(self.congress_num), open_connection())
        
        rep_votes_metrics = pd.DataFrame([self.bioguide_id],
                                        columns=['bioguide_id'])
        rep_votes_metrics['rep_votes'] = rep_votes.loc[0, 'rep_votes']
        rep_votes_metrics['total_votes'] = total_votes.loc[0, 'total_votes']
        rep_votes_metrics['percent_votes'] = (rep_votes_metrics['rep_votes']/
                                              rep_votes_metrics['total_votes'])
        
        self.rep_votes_metrics = rep_votes_metrics
        
    def num_votes_senate(self):
        """
        This method will be used to find the
        total number of times a senator has voted
        and compare it to the total number
        of roll call votes for the congress.
        """

        find_senator = pd.read_sql("""
        SELECT * 
        FROM congress_bio 
        WHERE chamber = 'senate'
        AND bioguide_id = '{}'""".format(self.bioguide_id),
                                   open_connection())

        state_short = us.states.mapping('name', 'abbr')[find_senator.loc[0, 'state']]
        last_name = find_senator.loc[0, 'name'].split(',')[0]

        rep_votes = pd.read_sql_query("""
        SELECT COUNT(vote_cast) as rep_votes
        FROM senator_votes_tbl
        where congress = {}
        AND last_name ilike '%' || '{}' || '%'
        AND state = '{}'
        AND vote_cast != 'Not Voting';
        """.format(self.congress_num,
                   last_name,
                  state_short), open_connection())

        total_votes = pd.read_sql_query("""
        SELECT COUNT(DISTINCT(roll_id)) total_votes
        FROM senator_votes_tbl
        WHERE congress = {};
        """.format(self.congress_num), open_connection())

        rep_votes_metrics = pd.DataFrame([self.bioguide_id],
                                        columns=['bioguide_id'])
        rep_votes_metrics['rep_votes'] = rep_votes.loc[0, 'rep_votes']
        rep_votes_metrics['total_votes'] = total_votes.loc[0, 'total_votes']
        rep_votes_metrics['percent_votes'] = (rep_votes_metrics['rep_votes']/
                                              rep_votes_metrics['total_votes'])

        self.rep_votes_metrics = rep_votes_metrics
        
        
    def num_sponsor(self):
        """
        This method will be used to find the
        total legislation a rep has sponsored
        and compare it to the maximum that
        all reps have sponsored for this congress.
        """

        all_sponsored = pd.read_sql_query("""
            SELECT 
            bioguide_id,
            count(bioguide_id) as rep_sponsor
            FROM(
            SELECT * FROM
            (
            SELECT issue_link, congress
            FROM all_legislation
            WHERE cast(congress as int) = {})
            AS this_congress
            LEFT JOIN bill_sponsors
            ON this_congress.issue_link = bill_sponsors.url
            WHERE bioguide_id != 'None')
            joined_leg
            GROUP BY joined_leg.bioguide_id
            """.format(self.congress_num), open_connection())

        all_cong = pd.read_sql("""
                    SELECT DISTINCT
                    bioguide_id
                    FROM congress_bio 
                    WHERE served_until = 'Present'
                    AND lower(state) != 'guam'
                    AND lower(state) != 'puerto rico'
                    AND lower(state) != 'district of columbia'
                    AND lower(state) != 'virgin islands'
                    AND lower(state) != 'american samoa'
                    AND lower(state) != 'northern mariana islands';""", open_connection())

        all_sponsored = pd.merge(all_cong, all_sponsored, how='left', on='bioguide_id').fillna(0)
        
        all_sponsored = all_sponsored.sort_values(['rep_sponsor', 'bioguide_id'], 
                                  ascending=[False, True]).reset_index(drop=True)

        all_sponsored['max_sponsor'] = all_sponsored['rep_sponsor'].max()
        all_sponsored['sponsor_percent'] = (all_sponsored['rep_sponsor']/all_sponsored['max_sponsor'])

        all_sponsored = all_sponsored.loc[all_sponsored['bioguide_id'] == self.bioguide_id].reset_index(drop=True)
        
        self.rep_sponsor_metrics = all_sponsored


    def membership_stats(self):
        if self.chamber.lower() == 'house':
            tbl = 'house_membership'        
            x = pd.read_sql_query("""
            SELECT * FROM congress_bio
            WHERE chamber = '{}'
            AND served_until = 'Present'
            AND served_until = 'Present'
            AND lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands';
            """.format(self.chamber.lower()), open_connection())
        elif self.chamber.lower() == 'senate':
            tbl = 'senate_membership'
            x = pd.read_sql_query("""
            SELECT * FROM congress_bio
            WHERE chamber = '{}'
            AND served_until = 'Present'
            AND served_until = 'Present'
            AND lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands';
            """.format(self.chamber.lower()), open_connection())

        df = pd.read_sql_query("""
        SELECT * FROM {}""".format(tbl), open_connection())

        df = df.groupby(['bioguide_id']).count()['committee'].reset_index(drop=False)
        df.columns = ['bioguide_id', 'num_committees']
        df = pd.merge(x[['bioguide_id', 'party']], df,
                     how='left', on='bioguide_id').drop(['party'], 1).fillna(0)
        
        df['max_committees'] = df['num_committees'].max()
        df['percent'] = (df['num_committees'] / df['max_committees'])

        ## Save it homie
        self.membership_stats_df = df.loc[df['bioguide_id'] == self.bioguide_id].reset_index(drop=True)

    def policy_areas(self):

        ## Query legilation rep made
        policy_areas = pd.read_sql_query("""
        SELECT * FROM
        (
        SELECT *
        FROM all_legislation
        WHERE cast(congress as int) = {})
        AS this_congress
        LEFT JOIN bill_sponsors
        ON this_congress.issue_link = bill_sponsors.url
        WHERE bioguide_id = '{}'
        """.format(self.congress_num, self.bioguide_id), open_connection())

        ## Unpack the policy area
        policy_area_list = []

        for policy in policy_areas['policy_area']:
            if policy == 'None':
                policy_area_list.append('Misc.')
            else:
                x = ast.literal_eval(policy)
                for i in range(len(x)):
                    policy_area_list.append(x[i])

        policy_area_df = pd.DataFrame(policy_area_list)
        policy_area_df['count'] = 1
        policy_area_df = policy_area_df.groupby([0]).count().reset_index(drop=False)
        policy_area_df.columns = ['policy_area', 'count']

        policy_area_df['percent'] = policy_area_df['count']/policy_area_df['count'].sum()

        self.policy_area_df = policy_area_df[['policy_area', 'percent', 'count']]

    def num_days_voted_all(self):
        
        if self.chamber.lower() == 'house':
            days_voted = pd.read_sql_query("""
            SELECT distinct_votes.bioguide_id, 
            count(distinct_votes.bioguide_id) as days_at_work
            FROM (
            SELECT DISTINCT bioguide_id, date
            FROM house_votes_tbl
            where congress = {}
            AND vote != 'Not Voting')
            as distinct_votes
            GROUP BY bioguide_id;
            """.format(self.congress_num), open_connection())

            find_house = pd.read_sql("""
            SELECT DISTINCT name,
            bioguide_id,
            state, district,
            party,
            photo_url
            FROM congress_bio 
            WHERE chamber = 'house'
            AND served_until = 'Present'
            AND lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands';""", open_connection())

            ## Get 
            days_voted = pd.merge(find_house, days_voted, how='left', on='bioguide_id')

            # LEFT JOIN congress_bio 
            # ON congress_bio.bioguide_id = days_voted.b_id

            vote_dates = pd.read_sql_query("""
            SELECT COUNT(DISTINCT(date)) as total_work_days 
            FROM house_votes_tbl 
            WHERE congress = {};
            """.format(self.congress_num),open_connection())
            
            
        elif self.chamber.lower() == 'senate':
            days_voted = pd.read_sql_query("""
            SELECT distinct_votes.last_name,
            distinct_votes.state,
            COUNT(distinct_votes.last_name) as days_at_work
            FROM (SELECT DISTINCT last_name, 
            date, state
            FROM senator_votes_tbl
            WHERE congress = {}
            AND vote_cast != 'Not Voting')
            AS distinct_votes
            GROUP BY last_name, state;""".format(
                    self.congress_num),open_connection())

            days_voted['state'] = days_voted['state'].apply(lambda x: str(us.states.lookup(x)))

            vote_dates = pd.read_sql_query("""
                SELECT COUNT(DISTINCT(date)) as total_work_days 
                FROM senator_votes_tbl 
                WHERE congress = {};""".format(
                    self.congress_num), open_connection())

            find_senator = pd.read_sql("""
            SELECT DISTINCT name,
            bioguide_id,
            state, district,
            party,
            photo_url
            FROM congress_bio 
            WHERE chamber = 'senate'
            AND served_until = 'Present';""", open_connection())

            find_senator['last_name'] = find_senator['name'].apply(lambda x: x.split(',')[0])
            days_voted = pd.merge(find_senator, days_voted, how='left', on=['state', 'last_name'])

        ## Get percent
        days_voted.loc[days_voted['days_at_work'].isnull(), 'days_at_work'] = 0
        days_voted.loc[:, 'total_work_days'] = vote_dates.loc[0, 'total_work_days']
        days_voted['percent_at_work'] = (days_voted['days_at_work']/
                                         days_voted['total_work_days'])

        ## Subset columns, sort, and remove dupes
        days_voted = days_voted[['bioguide_id', 'days_at_work', 'percent_at_work', 
                                 'total_work_days', 'name', 'state', 'district', 'party', 
                                 'photo_url']].sort_values(['percent_at_work', 'bioguide_id'],
                                                           ascending=[False,True]).drop_duplicates(['bioguide_id']).reset_index(drop=True)
        
        ## Add rank
        days_voted.loc[:, 'rank'] = days_voted['percent_at_work'].rank(method='min', ascending=False)
        self.days_voted = days_voted.loc[days_voted['bioguide_id'].notnull()].reset_index(drop=True)

    def num_votes_all(self):
        if self.chamber.lower() == 'house':
            rep_votes = pd.read_sql_query("""
            SELECT bioguide_id,
            COUNT(bioguide_id) as rep_votes
            FROM house_votes_tbl
            where congress = {}
            AND vote != 'Not Voting'
            GROUP BY bioguide_id
            ;
            """.format(self.congress_num), open_connection())

            total_votes = pd.read_sql_query("""
            SELECT COUNT(DISTINCT(roll_id)) total_votes
            FROM house_votes_tbl
            WHERE congress = {};
            """.format(self.congress_num), open_connection())

            find_house = pd.read_sql("""
            SELECT DISTINCT name,
            bioguide_id,
            state, district,
            party,
            photo_url
            FROM congress_bio 
            WHERE chamber = 'house'
            AND served_until = 'Present'
            AND lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands';""", open_connection())

            rep_votes = pd.merge(find_house, rep_votes, how='left', on='bioguide_id')
            
            
        elif self.chamber.lower() == 'senate':
            rep_votes = pd.read_sql_query("""
            SELECT distinct_votes.last_name,
            distinct_votes.state,
            COUNT(distinct_votes.last_name) as rep_votes
            FROM (SELECT last_name, 
            date, state
            FROM senator_votes_tbl
            WHERE congress = {}
            AND vote_cast != 'Not Voting')
            AS distinct_votes
            GROUP BY last_name, state;""".format(
                    self.congress_num),open_connection())

            rep_votes['state'] = rep_votes['state'].apply(lambda x: str(us.states.lookup(x)))

            total_votes = pd.read_sql_query("""
                SELECT COUNT(DISTINCT(roll_id)) as total_votes 
                FROM senator_votes_tbl 
                WHERE congress = {};""".format(
                    self.congress_num), open_connection())

            find_senator = pd.read_sql("""
            SELECT DISTINCT name,
            bioguide_id,
            state, district,
            party,
            photo_url
            FROM congress_bio 
            WHERE chamber = 'senate'
            AND served_until = 'Present';""", open_connection())

            find_senator['last_name'] = find_senator['name'].apply(lambda x: x.split(',')[0])
            rep_votes = pd.merge(find_senator, rep_votes, how='left', on=['state', 'last_name'])

        ## Get percent
        rep_votes.loc[rep_votes['rep_votes'].isnull(), 'rep_votes'] = 0
        rep_votes['total_votes'] = total_votes.loc[0, 'total_votes']
        rep_votes['percent_votes'] = (rep_votes['rep_votes']/
                                              rep_votes['total_votes'])

        ## Subset columns, sort, and remove dupes
        rep_votes = rep_votes[['bioguide_id', 'rep_votes', 'percent_votes', 
                               'total_votes', 'name', 'state', 'district', 'party', 
                                 'photo_url']].sort_values(['percent_votes', 'bioguide_id'],
                                                           ascending=[False,True]).drop_duplicates(['bioguide_id']).reset_index(drop=True)

        ## Clean and rank
        rep_votes = rep_votes.loc[rep_votes['bioguide_id'].notnull()].reset_index(drop=True)
        rep_votes.loc[:, 'rank'] = rep_votes['percent_votes'].rank(method='min', ascending=False)
        self.rep_votes_metrics = rep_votes

    def num_sponsored_all(self):
        """
        This method will be used to find the
        total legislation a rep has sponsored
        and compare it to the maximum that
        all reps have sponsored for this congress.
        """


        all_sponsored = pd.read_sql_query("""
            SELECT * FROM
            (
            SELECT 
            bioguide_id,
            count(bioguide_id) as rep_sponsor
            FROM(
            SELECT * FROM
            (
            SELECT issue_link, congress
            FROM all_legislation
            WHERE cast(congress as int) = {})
            AS this_congress
            LEFT JOIN bill_sponsors
            ON this_congress.issue_link = bill_sponsors.url
            WHERE bioguide_id != 'None')
            joined_leg
            GROUP BY joined_leg.bioguide_id)
            AS all_sponsor
            """.format(self.congress_num), open_connection())

        find_reps = pd.read_sql("""
            SELECT DISTINCT name,
            bioguide_id,
            state, district,
            party,
            photo_url,
            chamber
            FROM congress_bio 
            WHERE served_until = 'Present'
            AND lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands';""", open_connection())

        all_sponsored = pd.merge(find_reps, all_sponsored, how='left', on='bioguide_id')

        all_sponsored = all_sponsored.sort_values(['rep_sponsor', 'bioguide_id'], 
                                  ascending=[False, True]).reset_index(drop=True)

        all_sponsored.loc[all_sponsored['rep_sponsor'].isnull(), 'rep_sponsor'] = 0
        all_sponsored['max_sponsor'] = all_sponsored['rep_sponsor'].max()
        all_sponsored['sponsor_percent'] = (all_sponsored['rep_sponsor']/all_sponsored['max_sponsor'])
        
        all_sponsored = all_sponsored.loc[all_sponsored['chamber'].str.lower() == self.chamber.lower()]

        all_sponsored = all_sponsored[['bioguide_id', 'rep_sponsor', 'sponsor_percent', 
                                 'max_sponsor', 'name', 'state', 'district', 'party', 
                                 'photo_url']].sort_values(['sponsor_percent', 'bioguide_id'],
                                                           ascending=[False,True]).drop_duplicates(['bioguide_id']).reset_index(drop=True)
        ## Clean and rank
        all_sponsored = all_sponsored.loc[all_sponsored['bioguide_id'].notnull()].reset_index(drop=True)
        all_sponsored.loc[:, 'rank'] = all_sponsored['sponsor_percent'].rank(method='min', ascending=False)

        self.rep_sponsor_metrics = all_sponsored

    def get_rep_grade(self):
        self.rep_grade = pd.read_sql_query("""
        SELECT bioguide_id,
        letter_grade_extra_credit as letter_grade,
        total_grade_extra_credit as number_grade
        FROM congress_grades
        WHERE bioguide_id = '{}'
        AND congress = {}
        """.format(
                self.bioguide_id,
                int(self.congress_num)), open_connection())

    def bills_to_law(self):
        self.congress_num = current_congress_num()

        ## Bills made per rep
        df = pd.read_sql_query("""
            SELECT bioguide_id,
            count(bioguide_id) as bills_to_law
            FROM (
            SELECT * FROM
            (
            SELECT issue_link, congress, tracker
            FROM all_legislation
            WHERE cast(congress as int) = {})
            AS this_congress
            LEFT JOIN bill_sponsors
            ON this_congress.issue_link = bill_sponsors.url
            WHERE bioguide_id != 'None')
            AS joined_df
            WHERE lower(tracker) = 'became law'
            GROUP BY bioguide_id
            ;""".format(self.congress_num), open_connection())
        
        ## Max made by rep
        reps = pd.read_sql_query("""
            SELECT DISTINCT bioguide_id,
            chamber
            FROM congress_bio
            WHERE served_until = 'Present'
            AND lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands'
            ;""", open_connection())
        
        ## stats
        df = pd.merge(reps, df, how='left', on='bioguide_id').fillna(0)
        df.loc[:, 'total_bills_to_law'] = max(df.loc[:, 'bills_to_law'])
        df.loc[:, 'percent_bills_to_law'] = df.loc[:, 'bills_to_law']/df.loc[:, 'total_bills_to_law']
        
        ## subset 
        if self.how == 'bioguide_id':
            df = df.loc[df['bioguide_id'] == self.bioguide_id].reset_index(drop=True)
        elif self.how == 'chamber':
            df = df.loc[df['chamber'] == self.chamber].reset_index(drop=True)
        
        return df.to_dict(orient='records')

    def rep_beliefs(self):
        """
        This method will be used to 
        create the overall ideology score,
        the laddered up ideology scores,
        and the individual ideology scores
        for a given rep.
        
        Input: bioguide_id
        Output: scores
        """
        #################### Get overall score ####################
        
        actions = pd.read_sql_query("""
        SELECT bioguide_id,
        sum(total_actions) AS total_actions
        FROM (
        SELECT * 
        FROM
        representatives_ideology_stats
        WHERE bioguide_id = '{}')
        AS rep_stats
        GROUP BY (bioguide_id);
        """.format(self.bioguide_id), open_connection())

        scores = pd.read_sql_query("""
        SELECT bioguide_id,
        avg(tally_score) as tally_score
        FROM (
        SELECT * 
        FROM
        representatives_ideology_stats
        WHERE bioguide_id = '{}')
        AS rep_stats
        GROUP BY (bioguide_id);
        """.format(self.bioguide_id), 
                                   open_connection())

        overall = pd.merge(scores, actions, how='inner', on=['bioguide_id'])
        overall.loc[0, 'type'] = 'overall'
        overall.loc[0, 'sub_type_of'] = "default"
        
        #################### Get laddered up scores ####################
        
        scores = pd.read_sql_query("""
        SELECT bioguide_id,
        type,
        avg(tally_score) as tally_score
        FROM (
        SELECT * 
        FROM
        representatives_ideology_stats
        WHERE bioguide_id = '{}')
        AS rep_stats
        GROUP BY (bioguide_id, type);
        """.format(self.bioguide_id), open_connection())

        actions = pd.read_sql_query("""
        SELECT bioguide_id,
        type,
        sum(total_actions) AS total_actions
        FROM (
        SELECT * 
        FROM
        representatives_ideology_stats
        WHERE bioguide_id = '{}')
        AS rep_stats
        GROUP BY (bioguide_id, type);
        """.format(self.bioguide_id), open_connection())

        laddered_up = pd.merge(scores, actions, how='inner', on=['bioguide_id', 'type'])
        laddered_up.loc[:, 'sub_type_of'] = 'overall'
        overall = overall.append(laddered_up)
        
        
        #################### Get individual scores ####################
        
        individual_beliefs = pd.read_sql_query("""
        SELECT bioguide_id,
        tally_score,
        total_actions,
        ideology_type AS type,
        type AS sub_type_of
        FROM
        representatives_ideology_stats
        WHERE bioguide_id = '{}';
        """.format(self.bioguide_id), open_connection())
        
        return overall.append(individual_beliefs).reset_index(drop=True).to_dict(orient='records')

    
    def get_current_performance(self):
        if self.how == 'bioguide_id':
            self.current_stats = pd.read_sql_query("""
                                 SELECT * FROM {}
                                 WHERE bioguide_id = '{}'
                                 """.format(self.table,
                                            self.bioguide_id), 
                                                   open_connection())
        elif self.how == 'chamber':
            self.current_stats = pd.read_sql_query("""
                                 SELECT * FROM {}
                                 WHERE chamber = '{}'
                                 """.format(self.table,
                                            self.chamber), 
                                                   open_connection()
                                                  ).drop(['chamber'], 1).sort_values(
                                                  ['rank', 'name'])

    @staticmethod
    def add_card_data(df, chamber):
        congress_num = current_congress_num()

        missing_df = pd.read_sql_query("""
            SELECT * FROM (
            SELECT * FROM congress_bio
            WHERE served_until = 'Present'
            AND chamber = '{}')
            AS bio
            LEFT JOIN (
            SELECT bioguide_id as b_id, 
            letter_grade_extra_credit as letter_grade,
            total_grade_extra_credit as number_grade 
            FROM congress_grades
            WHERE congress = {}
            AND chamber = '{}')
            AS grades
            ON bio.bioguide_id = grades.b_id
            ;
            """.format(chamber,
                       congress_num,
                      chamber), open_connection())
        df = pd.merge(df.drop(['name', 'state', 'district', 'party', 'photo_url'], 1), 
             missing_df, how='left', on='bioguide_id')

        df.loc[df['letter_grade'].isnull(), 'letter_grade'] = 'NA'
        df.loc[df['number_grade'].isnull(), 'number_grade'] = 0
        return df

    def __init__(self, congress_num=None, bioguide_id=None, days_voted=None,
                rep_votes_metrics=None, rep_sponsor_metrics=None,
                chamber=None, membership_stats_df=None, policy_area_df=None,
                search_term=None, rep_grade=None, how=None, current_stats=None,
                table=None):
        self.congress_num = congress_num
        self.bioguide_id = bioguide_id
        self.days_voted = days_voted
        self.rep_votes_metrics = rep_votes_metrics
        self.rep_sponsor_metrics = rep_sponsor_metrics
        self.chamber = chamber
        self.membership_stats_df = membership_stats_df
        self.policy_area_df = policy_area_df
        self.rep_grade = rep_grade
        self.how = how
        self.current_stats = current_stats
        self.table = table

class Senate_colleciton(object):
    """
    This class will be used to gather
    the vote data for the seante.
    
    Attributes:
    congress_num
    session_num
    vote_menu
    new_data
    updated_data
    to_db
    roll_search
    congress_search
    session_search
    date_search
    roll_id
    votes_df
    
    """
    
    def collect_senate_vote_menu(self):
        """
        This method will collect the senate
        vote menu. Since the senate's website
        is somewhat strict on data collection
        I'm going to send headers to try to not
        get blacklisted.
        """
        
        ## Start request session and make headers
        session = requests.Session()
        postHeaders = {
            'Accept-Language': 'en-US,en;q=0.8',
            'Origin': 'http://www.website.com',
            'Referer': 'http://www.website.com/',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.120 Chrome/37.0.2062.120 Safari/537.36'
        }
        
        ## Create url and send post request
        url = 'https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{}_{}.xml'.format(
        self.congress_num, self.session_num)
        r = session.post(url, headers=postHeaders)
        
        ## Normalize the shitty xml data and clean columns
        senate_info = json_normalize(bf.data(fromstring(r.content)))
        senate_info.columns = senate_info.columns.str.strip('.$').str.replace('.', '_')

        ## Save variables for late use
        congress = senate_info.loc[0, 'vote_summary_congress']
        session = senate_info.loc[0, 'vote_summary_session']
        year = senate_info.loc[0, 'vote_summary_congress_year']

        ## Collect vote menu date, clean dates & column names, and add roll_id
        vote_menu = json_normalize(senate_info.loc[0, 'vote_summary_votes_vote'])
        vote_menu.columns = vote_menu.columns.str.strip('.$').str.replace('.', '_')
        vote_menu.loc[:,'vote_date'] = vote_menu.loc[:,'vote_date'].apply(
            lambda x: str(datetime.datetime.strptime(x + '-{}'.format(year), '%d-%b-%Y')).split(' ')[0])
        vote_menu.loc[:, 'congress'] = congress
        vote_menu.loc[:, 'session'] = session
        vote_menu.loc[:, 'roll_id'] = (vote_menu.loc[:, 'congress'].astype(str) + 
                                       vote_menu.loc[:, 'session'].astype(str) +
                                       vote_menu.loc[:, 'vote_number'].astype(str)).astype(int)

        ## Clean null values from each column
        for column in vote_menu.columns:
            vote_menu.loc[vote_menu[column].isnull(), column] = None

        clean_cols = ['issue', 'question', 'question_measure', 'result', 'title']
        for column in clean_cols:
            vote_menu.loc[vote_menu[column].notnull(),
                          column] = vote_menu.loc[vote_menu[column].notnull(),
                                                  column].apply(lambda x: unidecode(x).replace("'", "''"))
            
        clean_cols = ['vote_number', 'vote_tally_nays', 'vote_tally_yeas', 'congress',
                      'session', 'roll_id']
        for column in clean_cols:
            vote_menu[column] = vote_menu[column].astype(int)
            
        self.vote_menu = vote_menu
        
    def menu_to_sql(self):
        connection = open_connection()
        cursor = connection.cursor()

        new_data = 0
        updated_data = 0

        ## Put each row into sql
        for i in range(len(self.vote_menu)):
            x = list(self.vote_menu.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO senate_vote_menu (
                issue,
                question,
                question_measure,
                result,
                title,
                vote_date,
                vote_number,
                vote_tally_nays,
                vote_tally_yeas,
                congress,
                session,
                roll_id)
                VALUES ('{issue}', '{question}', '{question_measure}',
                        '{result}', '{title}', '{vote_date}', '{vote_number}', 
                        '{vote_tally_nays}', '{vote_tally_yeas}',
                        '{congress}', '{session}', '{roll_id}');"""


            sql_command = format_str.format(issue=p[0], question=p[1], question_measure=p[2],
                                           result=p[3], title=p[4], vote_date=p[5],
                                           vote_number=p[6], vote_tally_nays=p[7], 
                                            vote_tally_yeas=p[8], congress=p[9], 
                                            session=p[10], roll_id=p[11])
            ## Commit to sql
            try:
                cursor.execute(sql_command)
                connection.commit()
                new_data += 1
            except:
                ## Update what I got
                connection.rollback()
                sql_command = """UPDATE senate_vote_menu 
                SET
                issue = '{}',
                question = '{}',
                question_measure = '{}',
                result = '{}',
                title = '{}',
                vote_date = '{}',
                vote_number = '{}',
                vote_tally_nays = '{}',
                vote_tally_yeas = '{}',
                congress = '{}',
                session = '{}'
                WHERE (roll_id = '{}');""".format(
                self.vote_menu.loc[i, 'issue'],
                self.vote_menu.loc[i, 'question'],
                self.vote_menu.loc[i, 'question_measure'],
                self.vote_menu.loc[i, 'result'],
                self.vote_menu.loc[i, 'title'],
                self.vote_menu.loc[i, 'vote_date'],
                self.vote_menu.loc[i, 'vote_number'],
                self.vote_menu.loc[i, 'vote_tally_nays'],
                self.vote_menu.loc[i, 'vote_tally_yeas'],
                self.vote_menu.loc[i, 'congress'],
                self.vote_menu.loc[i, 'session'],
                self.vote_menu.loc[i, 'roll_id'])    
                cursor.execute(sql_command)
                connection.commit()
                updated_data += 1
        connection.close()
        self.new_data = new_data
        self.updated_data = updated_data
        
    def get_senate_votes(self):
        """
        This method will be used to get
        votes from the senate.

        I will use the senate vote menu
        to find new votes

        attributes:
        congress number
        session number
        roll number
        date
        """

        ## Create session, headers and url to serach
        session = requests.Session()
        postHeaders = {
            'Accept-Language': 'en-US,en;q=0.8',
            'Origin': 'http://www.website.com',
            'Referer': 'http://www.website.com/',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/37.0.2062.120 Chrome/37.0.2062.120 Safari/537.36'
        }
        url = 'https://www.senate.gov/legislative/LIS/roll_call_votes/vote{}{}/vote_{}_{}_{}.xml'.format(
         self.congress_search, self.session_search, self.congress_search, self.session_search,
        str(self.roll_search).zfill(5))

        ## Post request for url
        r = session.post(url, headers=postHeaders)

        ## Get first level of data
        vote_info = json_normalize(bf.data(fromstring(r.content)))
        vote_info.columns = vote_info.columns.str.strip('.$').str.replace('.', '_')

        ## Get roll call votes
        votes_df = json_normalize(vote_info.loc[0, 'roll_call_vote_members_member'])
        votes_df.columns = votes_df.columns.str.strip('.$').str.replace('.', '_')

        ## Add things for db
        votes_df.loc[:, 'roll'] = int(self.roll_search)
        votes_df.loc[:, 'congress'] = int(self.congress_search)
        votes_df.loc[:, 'session'] = int(self.session_search)
        votes_df.loc[:, 'date'] = str(self.date_search)
        votes_df.loc[:, 'year'] = pd.to_datetime(self.date_search).year
        votes_df.loc[:, 'roll_id'] = int(self.roll_id)

        ## Save that ish!
        self.votes_df = votes_df
        
    def votes_to_sql(self):
        connection = open_connection()
        cursor = connection.cursor()

        ## Put each row into sql
        for i in range(len(self.votes_df)):
            try:
                self.votes_df.loc[i, 'first_name'] = self.votes_df.loc[i, 'first_name'].replace("'", "''")
                self.votes_df.loc[i, 'last_name'] = self.votes_df.loc[i, 'last_name'].replace("'", "''")
                self.votes_df.loc[i, 'member_full'] = self.votes_df.loc[i, 'member_full'].replace("'", "''")
            except:
                'hold'
            try:
                self.votes_df.loc[i, 'first_name'] = str(self.votes_df.loc[i, 'first_name'].decode('unicode_escape').encode('ascii','ignore'))
                self.votes_df.loc[i, 'last_name'] = str(self.votes_df.loc[i, 'last_name'].decode('unicode_escape').encode('ascii','ignore'))
                self.votes_df.loc[i, 'member_full'] = str(self.votes_df.loc[i, 'member_full'].decode('unicode_escape').encode('ascii','ignore'))
            except:
                'hold'
            
            x = list(self.votes_df.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO senator_votes_tbl (
                first_name,
                last_name,
                lis_member_id,
                member_full,
                party,
                state,
                vote_cast,
                roll,
                congress,
                session,
                date,
                year,
                roll_id)
                VALUES ('{first_name}', '{last_name}', '{lis_member_id}',
                        '{member_full}', '{party}', '{state}', '{vote_cast}', 
                        '{roll}', '{congress}', '{session}',
                        '{date}', '{year}', '{roll_id}');"""


            sql_command = format_str.format(first_name=p[0], last_name=p[1], lis_member_id=p[2],
                                            member_full=p[3], party=p[4], state=p[5], vote_cast=p[6],
                                            roll=p[7], congress=p[8], session=p[9], date=p[10], 
                                            year=p[11], roll_id=p[12])
            ## Commit to sql
            try:
                cursor.execute(sql_command)
                connection.commit()
            except:
                ## Update what I got
                connection.rollback()
                sql_command = """UPDATE senator_votes_tbl 
                SET
                first_name = '{}',
                last_name = '{}',
                member_full = '{}',
                party = '{}',
                state = '{}',
                vote_cast = '{}',
                roll = '{}',
                congress = '{}',
                session = '{}',
                date = '{}',
                year = '{}'
                WHERE (lis_member_id = '{}'
                AND roll_id = '{}');""".format(
                self.votes_df.loc[i, 'first_name'],
                self.votes_df.loc[i, 'last_name'],
                self.votes_df.loc[i, 'member_full'],
                self.votes_df.loc[i, 'party'],
                self.votes_df.loc[i, 'state'],
                self.votes_df.loc[i, 'vote_cast'],
                self.votes_df.loc[i, 'roll'],
                self.votes_df.loc[i, 'congress'],
                self.votes_df.loc[i, 'session'],
                self.votes_df.loc[i, 'date'],
                self.votes_df.loc[i, 'year'],
                self.votes_df.loc[i, 'lis_member_id'],
                self.votes_df.loc[i, 'roll_id'])    
                cursor.execute(sql_command)
                connection.commit()
        connection.close()
        
    def daily_senate_menu(self):
        """
        In this method I will be collecting the senate vote menu
        for the entire current year. I will then compare the 
        highest roll call vote in the database to the collected
        data. If I have collected data that is not in the db
        then I'll insert the new data points. I will this save
        an attribute to say how many new rows were inserted
        to the db. That number will be included in the daily
        emails.
        """

        ## Query db for max roll call for current year
        current_year = datetime.date.today().year

        sql_query = """
        SELECT max(vote_number) FROM senate_vote_menu
        where date(vote_date) >= '{}-01-01;'
        """.format(current_year)
        senate_menu = pd.read_sql_query(sql_query, open_connection())
        
        
        sql_query = """
        SELECT max(congress) as congress, max(session) as session 
        FROM senate_vote_menu
        where date(vote_date) >= '{}-01-01;'
        """.format(current_year)
        max_senate_vars = pd.read_sql_query(sql_query, open_connection())


        ## Set congress and session vars
        self.congress_num = max_senate_vars.loc[0, 'congress']
        self.session_num = max_senate_vars.loc[0, 'session']

        ## Collect house vote menu for current year and compare
        Senate_colleciton.collect_senate_vote_menu(self)
        self.vote_menu = self.vote_menu[self.vote_menu['vote_number'] > 
                                        senate_menu.loc[0,'max']].reset_index(drop=True)
        
        num_rows = len(self.vote_menu)
        
        if num_rows == 0:
            self.to_db = 'No new vote menu data.'
            print self.to_db
        if num_rows > 0:
            self.to_db = '{} new vote(s) in the data base.'.format(num_rows)
            print self.to_db
            Senate_colleciton.menu_to_sql(self)
            
    def daily_senate_votes(self):
        """
        In this method I will be checking that
        I have the most up-to-date senate votes.
        I need to collect vote menu data first
        and then check that I'm not missing any
        votes from the vote menu table.
        If I am then go collect them.
        """

        ## Query db for max roll call for current year
        current_year = datetime.date.today().year

        ## Get max vote from vote menu
        sql_query = """
        SELECT max(vote_number) FROM senate_vote_menu
        where date(vote_date) >= '{}-01-01;'
        """.format(current_year)
        senate_menu = pd.read_sql_query(sql_query, open_connection())
        
        ## Get max votes form vote table
        sql_query = """
        SELECT max(roll)
        FROM senator_votes_tbl
        where date(date) >= '{}-01-01;'
        """.format(current_year)
        senate_votes = pd.read_sql_query(sql_query, open_connection())

        ## Check if you have most up-to-date data
        if senate_menu.loc[0, 'max'] == (senate_votes.loc[0, 'max']):
            print 'Have all senate votes :)'
        else:
            """
            If there is more vote menu data than votes
            then go collect and house the missing votes.
            """
            sql_query = """
            SELECT * FROM senate_vote_menu
            where date(vote_date) >= '{}-01-01'
            and vote_number > {};
            """.format(current_year,
                       senate_votes.loc[0, 'max'])
            senate_menu = pd.read_sql_query(sql_query, open_connection())

            print 'collect {} missing senate votes!'.format(len(senate_menu))
            for i in range(len(senate_menu)):
                print senate_menu.loc[i, 'vote_number']
                self.roll_search = senate_menu.loc[i, 'vote_number']
                self.congress_search = senate_menu.loc[i, 'congress']
                self.session_search = senate_menu.loc[i, 'session']
                self.date_search = senate_menu.loc[i, 'vote_date']
                self.roll_id = senate_menu.loc[i, 'roll_id']

                ## Find data
                Senate_colleciton.get_senate_votes(self)

                ## House
                Senate_colleciton.votes_to_sql(self)
        
    def __init__(self, congress_num=None, session_num=None, vote_menu=None,
                new_data=None, updated_data=None, to_db=None, roll_search=None,
                congress_search=None, session_search=None, date_search=None,
                roll_id=None, votes_df=None):
        self.congress_num = congress_num
        self.session_num = session_num
        self.vote_menu = vote_menu
        self.new_data = new_data
        self.updated_data = updated_data
        self.to_db = to_db
        self.roll_search = roll_search
        self.congress_search = congress_search
        self.session_search = session_search
        self.date_search = date_search
        self.roll_id = roll_id
        self.votes_df = votes_df



class Search(object):
    """
    This class will be used to take user input
    and search for reps.
    
    Types of search:
    State
    Name
    District
    Zip Code
    And combinations
    
    """
    
    def search(self):
        search_term = sanitize(str(self.search_term).lower())
        cong_num = current_congress_num()

        #### Is number a zipcode
        try:
            ## remove word zipcode and strip empty space
            search_term_zip = search_term.strip(' ').replace('zip', '').replace('code', '')

            ## length of term == 5
            if len(search_term_zip) == 5:
                try:
                    ## can it convert to number
                    self.zip_code = int(search_term_zip)
                    Search.check_zip_code(self)
                    if self.zip_code_check == True:
                        return Search.find_dist_by_zip(self)
                except:
                    "move on"
                
        except:
            "move on"
            
        try:
            search_term_dist = search_term.strip(' ').replace('district', '')
            num = []
            [num.append(int(s)) for s in search_term_dist.split() if s.isdigit()]
            if len(num) > 0:
                dist_search = ''
                for i in range(len(num)):
                    search_term_dist = search_term_dist.replace(str(num[i]), '')
                    dist_search += " OR district = {} ".format(num[i])
                search_term_dist = search_term_dist.strip(' ')
                if len(search_term_dist) > 0:
                    try:
                        search_term_dist = str(us.states.lookup(unicode(search_term_dist))).lower()
                    except:
                        'dont change it'
                        
                    x = search_term_dist.strip(' ').split(' ')
                    search_term_query = ''
                    for i in range(len(x)):
                        search_term_query += """AND (lower(name) ilike '%' || '{}' || '%'
                        OR lower(state) ilike '%' || '{}' || '%'
                        OR lower(party) ilike '%' || '{}' || '%') """.format(x[i], x[i], x[i])
                        
                    return pd.read_sql_query("""
                    SELECT * FROM (
                    SELECT DISTINCT 
                    address,
                    bio_text,
                    congress_url,
                    facebook,
                    leadership_position,
                    phone,
                    served_until,
                    twitter_handle,
                    twitter_url,
                    website,
                    year_elected,
                    name,
                    bioguide_id,
                    state,
                    district,
                    party,
                    chamber,
                    photo_url
                    FROM congress_bio
                    WHERE served_until = 'Present'
                    AND lower(state) != 'guam'
                    AND lower(state) != 'puerto rico'
                    AND lower(state) != 'district of columbia'
                    AND lower(state) != 'virgin islands'
                    AND lower(state) != 'american samoa'
                    AND lower(state) != 'northern mariana islands'
                    {}
                    AND ({}))
                    AS rep_bio
                    LEFT JOIN (
                    SELECT bioguide_id as b_id,
                    letter_grade_extra_credit as letter_grade,
                    total_grade_extra_credit as number_grade
                    FROM congress_grades
                    WHERE congress = {}
                    ) AS grades 
                    ON grades.b_id = rep_bio.bioguide_id
                    ;
                    """.format(
                        search_term_query,
                        dist_search[4:],
                        cong_num), open_connection())
                else:
                    return pd.read_sql_query("""
                    SELECT * FROM (
                    SELECT DISTINCT 
                    address,
                    bio_text,
                    congress_url,
                    facebook,
                    leadership_position,
                    phone,
                    served_until,
                    twitter_handle,
                    twitter_url,
                    website,
                    year_elected,
                    name,
                    bioguide_id,
                    state,
                    district,
                    party,
                    chamber,
                    photo_url
                    FROM congress_bio
                    WHERE served_until = 'Present'
                    AND lower(state) != 'guam'
                    AND lower(state) != 'puerto rico'
                    AND lower(state) != 'district of columbia'
                    AND lower(state) != 'virgin islands'
                    AND lower(state) != 'american samoa'
                    AND lower(state) != 'northern mariana islands'
                    AND ({}))
                    AS rep_bio
                    LEFT JOIN (
                    SELECT bioguide_id as b_id,
                    letter_grade_extra_credit as letter_grade,
                    total_grade_extra_credit as number_grade
                    FROM congress_grades
                    WHERE congress = {}
                    ) AS grades 
                    ON grades.b_id = rep_bio.bioguide_id
                    ;
                    """.format(
                        dist_search[4:],
                        cong_num), open_connection())

            """
            If you make it here then 
            none of the other stuff worked.
            Just search what you originally got.
            """
            try:
                state_search = str(us.states.lookup(unicode(search_term))).lower()
                search_term_query = """OR (lower(name) ilike '%' || '{}' || '%'
                OR lower(state) ilike '%' || '{}' || '%'
                OR lower(party) ilike '%' || '{}' || '%') """.format(state_search, state_search, state_search)
                
                df =  pd.read_sql_query("""
                SELECT * FROM (
                SELECT DISTINCT 
                address,
                bio_text,
                congress_url,
                facebook,
                leadership_position,
                phone,
                served_until,
                twitter_handle,
                twitter_url,
                website,
                year_elected,
                name,
                bioguide_id,
                state,
                district,
                party,
                chamber,
                photo_url
                FROM congress_bio
                WHERE served_until = 'Present'
                AND lower(state) != 'guam'
                AND lower(state) != 'puerto rico'
                AND lower(state) != 'district of columbia'
                AND lower(state) != 'virgin islands'
                AND lower(state) != 'american samoa'
                AND lower(state) != 'northern mariana islands'
                AND (({}))
                        AS rep_bio
                        LEFT JOIN (
                        SELECT bioguide_id as b_id,
                        letter_grade_extra_credit as letter_grade,
                        total_grade_extra_credit as number_grade
                        FROM congress_grades
                        WHERE congress = {}
                        ) AS grades 
                        ON grades.b_id = rep_bio.bioguide_id
                        ;
                """.format(
                    search_term_query[4:],
                    cong_num
                    ), open_connection())

                if len(df) > 0:
                    return df
                else:
                    x = search_term.strip(' ').split(' ')
                    search_term_query = ''
                    for i in range(len(x)):
                        search_term_query += """OR (lower(name) ilike '%' || '{}' || '%'
                        OR lower(state) ilike '%' || '{}' || '%'
                        OR lower(party) ilike '%' || '{}' || '%') """.format(x[i], x[i], x[i])
                    
                    df = pd.read_sql_query("""
                    SELECT * FROM (
                    SELECT DISTINCT 
                    address,
                    bio_text,
                    congress_url,
                    facebook,
                    leadership_position,
                    phone,
                    served_until,
                    twitter_handle,
                    twitter_url,
                    website,
                    year_elected,
                    name,
                    bioguide_id,
                    state,
                    district,
                    party,
                    chamber,
                    photo_url
                    FROM congress_bio
                    WHERE served_until = 'Present'
                    AND lower(state) != 'guam'
                    AND lower(state) != 'puerto rico'
                    AND lower(state) != 'district of columbia'
                    AND lower(state) != 'virgin islands'
                    AND lower(state) != 'american samoa'
                    AND lower(state) != 'northern mariana islands'
                    AND (({}))
                            AS rep_bio
                            LEFT JOIN (
                            SELECT bioguide_id as b_id,
                            letter_grade_extra_credit as letter_grade,
                            total_grade_extra_credit as number_grade
                            FROM congress_grades
                            WHERE congress = {}
                            ) AS grades 
                            ON grades.b_id = rep_bio.bioguide_id
                            ;
                    """.format(
                        search_term_query[4:],
                        cong_num
                        ), open_connection())

                    return df.fillna(0)


            except:
                'dont change it'
            
            x = search_term.strip(' ').split(' ')
            search_term_query = ''
            for i in range(len(x)):
                search_term_query += """OR (lower(name) ilike '%' || '{}' || '%'
                OR lower(state) ilike '%' || '{}' || '%'
                OR lower(party) ilike '%' || '{}' || '%') """.format(x[i], x[i], x[i])
            
            return pd.read_sql_query("""
            SELECT * FROM (
            SELECT DISTINCT 
            address,
            bio_text,
            congress_url,
            facebook,
            leadership_position,
            phone,
            served_until,
            twitter_handle,
            twitter_url,
            website,
            year_elected,
            name,
            bioguide_id,
            state,
            district,
            party,
            chamber,
            photo_url
            FROM congress_bio
            WHERE served_until = 'Present'
            AND lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands'
            AND (({}))
                    AS rep_bio
                    LEFT JOIN (
                    SELECT bioguide_id as b_id,
                    letter_grade_extra_credit as letter_grade,
                    total_grade_extra_credit as number_grade
                    FROM congress_grades
                    WHERE congress = {}
                    ) AS grades 
                    ON grades.b_id = rep_bio.bioguide_id
                    ;
            """.format(
                search_term_query[4:],
                cong_num
                ), open_connection())
        except:
            'wtf'
    
    def check_zip_code(self):
        url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(str(self.zip_code))
        r = requests.get(url)
        if r.status_code == 200:
            try:
                r.json()['results'][0]['partial_match']
                self.zip_code_check = "Bad address"
            except:
                """Address is good"""
                self.zip_code_check = True
        else:
            print "Bad request from Google on Search"
            self.zip_code_check = "Bad request"
            
    def find_dist_by_zip(self):
        cong_num = current_congress_num()
        s = requests.Session()
        s.auth = ('user', 'pass')
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        }
        url = 'http://ziplook.house.gov/htbin/findrep?ZIP={}&Submit=FIND+YOUR+REP+BY+ZIP'.format(self.zip_code)
        r = requests.get(url=url, headers=headers)
        page = BeautifulSoup(r.content, 'lxml')
        possible_reps = str(page.findAll('div', id='PossibleReps')[0])

        district_info = pd.DataFrame()

        for i in range(1, len(possible_reps.split('/zip/pictures/'))):
            state_dist = possible_reps.split('/zip/pictures/')[i].split('_')[0]
            split_sd = re.split('(\d+)', state_dist)
            for j in range(len(split_sd)):
                if j == 0:
                    ## Letters is state short
                    state_short = str(split_sd[j])
                    district_info.loc[i, 'state_short'] = state_short
                    state_long = str(us.states.lookup(state_short))
                    district_info.loc[i, 'state_long'] = state_long
                elif j == 1:
                    ## Numbers is district number
                    district_num = int(split_sd[j])
                    district_info.loc[i, 'district_num'] = district_num

        dist = district_info.reset_index(drop=True)

        dist_query = ''
        for i in range(len(dist)):
            if i != 0:
                dist_query += " OR (state = '{}' AND district ='{}')".format(
                    dist.loc[i, 'state_long'], int(dist.loc[i, 'district_num']))
            if i == 0:
                dist_query += "(state = '{}' AND district ='{}')".format(
                    dist.loc[i, 'state_long'], int(dist.loc[i, 'district_num']))


        sql_query = """
        SELECT * FROM (
        SELECT distinct 
        address,
        bio_text,
        congress_url,
        facebook,
        leadership_position,
        phone,
        served_until,
        twitter_handle,
        twitter_url,
        website,
        year_elected,
        name, 
        bioguide_id, 
        state, 
        district, 
        party, 
        chamber,
        photo_url
        FROM congress_bio
        WHERE (({})
        AND served_until = 'Present'
        AND lower(state) != 'guam'
        AND lower(state) != 'puerto rico'
        AND lower(state) != 'district of columbia'
        AND lower(state) != 'virgin islands'
        AND lower(state) != 'american samoa'
        AND lower(state) != 'northern mariana islands')
        OR (state = '{}' AND served_until = 'Present' AND chamber = 'senate'))
                AS rep_bio
                LEFT JOIN (
                SELECT bioguide_id as b_id,
                letter_grade_extra_credit as letter_grade,
                total_grade_extra_credit as number_grade
                FROM congress_grades
                WHERE congress = {}
                ) AS grades 
                ON grades.b_id = rep_bio.bioguide_id
                ;""".format(dist_query, 
                    dist.loc[i, 'state_long'], 
                    cong_num)

        return pd.read_sql_query(sql_query, open_connection())

    def get_cosine(self):

        intersection = set(self.vec1.keys()) & set(self.vec2.keys())
        numerator = sum([self.vec1[x] * self.vec2[x] for x in intersection])
        
        sum1 = sum([self.vec1[x]**2 for x in self.vec1.keys()])
        sum2 = sum([self.vec2[x]**2 for x in self.vec2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)

        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator

    def text_to_vector(self):
        WORD = re.compile(r'\w+')
        words = WORD.findall(self.text)
        return Counter(words)


    def add_sim(self):
        
        if len(self.search_term) == 2:
            state_search = str(us.states.lookup(unicode(self.search_term))).lower()
            if state_search == 'none':
                state_search = self.search_term
        else:
            state_search = self.search_term
            
        
        for i in range(len(self.df)):

            self.text = self.df.loc[i, 'name'].lower()
            self.vec1 = Search.text_to_vector(self)

            self.text = self.search_term
            self.vec2 = Search.text_to_vector(self)

            cosine_1 = Search.get_cosine(self)


            self.text = self.df.loc[i, 'state'].lower()
            self.vec1 = Search.text_to_vector(self)

            self.text = state_search
            self.vec2 = Search.text_to_vector(self)

            cosine_2 = Search.get_cosine(self)

            self.df.loc[i, 'sim'] = cosine_1 + cosine_2
            
        self.df = self.df.sort_values(['sim'], ascending=False).reset_index(drop=True).drop(['sim'],1)
    
    
    def add_rank(self):
        if self.category == 'attendance':
            table = 'current_attendance'
        elif self.category == 'participation':
            table = 'current_participation'
        elif self.category == 'efficacy':
            table = 'current_sponsor'
        rank_df = pd.read_sql_query("""
                  SELECT * FROM {}
                  WHERE chamber = '{}'
                  ;""".format(table,
                              self.chamber), open_connection())
        
        return pd.merge(self.df.drop(['chamber', 'district', 'name', 'party', 'photo_url', 'state'], 1), 
                        rank_df, 
                        how='inner', 
                        on='bioguide_id').drop_duplicates(['bioguide_id'])
    
    
    def __init__(self, search_term=None, zip_code=None, df=None, vec1=None, vec2=None,
        text=None, category=None, chamber=None):
        self.search_term = search_term
        self.zip_code = zip_code
        self.df = df
        self.vec1 = vec1
        self.vec2 = vec2
        self.text = text
        self.category = category
        self.chamber = chamber

class Grade_reps(object):
    def get_participation_for_grade(self):
        days_voted = pd.read_sql_query("""
        SELECT * FROM (
        SELECT distinct_votes.bioguide_id, 
        COUNT(distinct_votes.bioguide_id) AS days_at_work
        FROM (
        SELECT DISTINCT bioguide_id, date
        FROM house_votes_tbl
        where congress = {}
        AND vote != 'Not Voting')
        AS distinct_votes
        GROUP BY bioguide_id)
        AS work_stats
        WHERE bioguide_id is not null;
        """.format(self.congress), open_connection())
        
        vote_dates = pd.read_sql_query("""
        SELECT COUNT(DISTINCT(date)) as total_work_days 
        FROM house_votes_tbl 
        WHERE congress = {};
        """.format(self.congress),open_connection())
        
        rep_votes = pd.read_sql_query("""
            SELECT bioguide_id,
            COUNT(bioguide_id) as rep_votes
            FROM house_votes_tbl
            where congress = {}
            AND vote != 'Not Voting'
            GROUP BY bioguide_id
            ;
            """.format(self.congress), open_connection())

        total_votes = pd.read_sql_query("""
            SELECT COUNT(DISTINCT(roll_id)) total_votes
            FROM house_votes_tbl
            WHERE congress = {};
            """.format(self.congress), open_connection())
        
        days_voted.loc[days_voted['days_at_work'].isnull(), 'days_at_work'] = 0
        days_voted.loc[:, 'total_work_days'] = vote_dates.loc[0, 'total_work_days']
        days_voted = days_voted.loc[((days_voted['bioguide_id'].notnull()) &
                       (days_voted['bioguide_id'] != '0'))].reset_index(drop=True)
        
        rep_votes.loc[rep_votes['rep_votes'].isnull(), 'rep_votes'] = 0
        rep_votes.loc[:, 'total_votes'] = total_votes.loc[0, 'total_votes']
        rep_votes = rep_votes.loc[((rep_votes['bioguide_id'].notnull()) &
                       (rep_votes['bioguide_id'] != '0'))].reset_index(drop=True)
        
        days_voted.loc[:, 'rep_votes'] = rep_votes.loc[:, 'rep_votes']
        days_voted.loc[:, 'total_votes'] = rep_votes.loc[:, 'total_votes']

        ## Make final stats
        days_voted['new_participation'] = days_voted['rep_votes'] - days_voted['days_at_work']
        days_voted['new_total'] = days_voted['total_votes'] - days_voted['total_work_days']
        days_voted.loc[:, 'participation_percent'] = days_voted['new_participation']/days_voted['new_total']
        
        days_voted.loc[:, 'chamber'] = 'house'
        
        ## Now for senate
        days_voted_s = pd.read_sql_query("""
                    SELECT distinct_votes.last_name,
                    distinct_votes.state,
                    COUNT(distinct_votes.last_name) as days_at_work
                    FROM (SELECT DISTINCT last_name, 
                    date, state
                    FROM senator_votes_tbl
                    WHERE congress = {}
                    AND vote_cast != 'Not Voting')
                    AS distinct_votes
                    GROUP BY last_name, state;""".format(
                            self.congress),open_connection())

        days_voted_s.loc[:, 'state'] = days_voted_s.loc[:, 'state'].apply(lambda x: str(us.states.lookup(x)))

        vote_dates_s = pd.read_sql_query("""
            SELECT COUNT(DISTINCT(date)) as total_work_days 
            FROM senator_votes_tbl 
            WHERE congress = {};""".format(
                self.congress), open_connection())

        ## Find senators that voted during this time
        date_range = pd.read_sql_query("""
        SELECT min(date) AS min_date,
        max(date) AS max_date
        FROM senator_votes_tbl
        WHERE congress = {}
        """.format(self.congress), open_connection())
        date_range.loc[0, 'min_date'] = date_range.loc[0, 'min_date'].year
        date_range.loc[0, 'max_date'] = date_range.loc[0, 'max_date'].year

        all_senate = pd.read_sql("""
        SELECT DISTINCT name,
        bioguide_id,
        state, district,
        party,
        photo_url, 
        year_elected, 
        served_until
        FROM congress_bio 
        WHERE chamber = 'senate';""", open_connection())
        all_senate.loc[all_senate['served_until'] == 'Present', 'served_until'] = datetime.datetime.today().year
        all_senate['year_elected'] = all_senate['year_elected'].astype(int)
        all_senate['served_until'] = all_senate['served_until'].astype(int)

        find_senator = all_senate.loc[((all_senate['year_elected'] <= date_range.loc[0, 'max_date']) &
                       (all_senate['served_until'] >= date_range.loc[0, 'min_date']))]

        find_senator.loc[:, 'last_name'] = find_senator.loc[:, 'name'].apply(lambda x: x.split(',')[0])
        days_voted_s = pd.merge(days_voted_s, find_senator, how='left', on=['state', 'last_name']).drop_duplicates(['bioguide_id'])
        
        days_voted_s.loc[days_voted_s['days_at_work'].isnull(), 'days_at_work'] = 0
        days_voted_s.loc[:, 'total_work_days'] = vote_dates_s.loc[0, 'total_work_days']
        days_voted_s.loc[:, 'percent_at_work'] = (days_voted_s['days_at_work']/
                                         days_voted_s['total_work_days'])
        
        
        rep_votes_s = pd.read_sql_query("""
                    SELECT distinct_votes.last_name,
                    distinct_votes.state,
                    COUNT(distinct_votes.last_name) as rep_votes
                    FROM (SELECT last_name, 
                    date, state
                    FROM senator_votes_tbl
                    WHERE congress = {}
                    AND vote_cast != 'Not Voting')
                    AS distinct_votes
                    GROUP BY last_name, state;""".format(
                            self.congress),open_connection())

        rep_votes_s.loc[:, 'state'] = rep_votes_s.loc[:, 'state'].apply(lambda x: str(us.states.lookup(x)))

        total_votes_s = pd.read_sql_query("""
            SELECT COUNT(DISTINCT(roll_id)) as total_votes 
            FROM senator_votes_tbl 
            WHERE congress = {};""".format(
                self.congress), open_connection())

        ## Find senators that voted during this time
        find_senator.loc[:, 'last_name'] = find_senator.loc[:, 'name'].apply(lambda x: x.split(',')[0])
        rep_votes_s = pd.merge(rep_votes_s, find_senator, how='left', on=['state', 'last_name']).drop_duplicates(['bioguide_id'])


        rep_votes_s.loc[rep_votes_s['rep_votes'].isnull(), 'rep_votes'] = 0
        rep_votes_s.loc[:, 'total_votes'] = total_votes_s.loc[0, 'total_votes']
        rep_votes_s.loc[:, 'percent_votes'] = (rep_votes_s['rep_votes']/
                                              rep_votes_s['total_votes'])
        
        ## Make final stats
        days_voted_s['new_participation'] = rep_votes_s['rep_votes'] - days_voted_s['days_at_work']
        days_voted_s['new_total'] = rep_votes_s['total_votes'] - days_voted_s['total_work_days']
        days_voted_s.loc[:, 'participation_percent'] = days_voted_s['new_participation']/days_voted_s['new_total']
        days_voted_s.loc[:, 'chamber'] = 'senate'
        
        
        return days_voted.append(days_voted_s)[['bioguide_id', 'chamber', 'participation_percent']].reset_index(drop=True)
        
    def rank_bills_made(self):
        all_sponsored = pd.read_sql_query("""
            SELECT * FROM
            (
            SELECT 
            bioguide_id,
            count(bioguide_id) as rep_sponsor
            FROM(
            SELECT * FROM
            (
            SELECT issue_link, congress
            FROM all_legislation
            WHERE cast(congress as int) = {})
            AS this_congress
            LEFT JOIN bill_sponsors
            ON this_congress.issue_link = bill_sponsors.url
            WHERE bioguide_id != 'None')
            joined_leg
            GROUP BY joined_leg.bioguide_id)
            AS all_sponsor
            """.format(self.congress), open_connection())

        find_reps = pd.read_sql("""
            SELECT DISTINCT name,
            bioguide_id,
            state, district,
            party,
            photo_url,
            chamber
            FROM congress_bio
            WHERE lower(state) != 'guam'
            AND lower(state) != 'puerto rico'
            AND lower(state) != 'district of columbia'
            AND lower(state) != 'virgin islands'
            AND lower(state) != 'american samoa'
            AND lower(state) != 'northern mariana islands';""", open_connection())

        all_sponsored = pd.merge(all_sponsored, find_reps, how='left', on='bioguide_id').drop_duplicates(['bioguide_id'])

        all_sponsored = all_sponsored.sort_values(['rep_sponsor', 'bioguide_id'], 
                                  ascending=[False, True]).reset_index(drop=True)

        all_sponsored.loc[all_sponsored['rep_sponsor'].isnull(), 'rep_sponsor'] = 0
        all_sponsored.loc[:, 'max_sponsor'] = all_sponsored['rep_sponsor'].max()

        house_sponsored = all_sponsored.loc[all_sponsored['chamber'] == 'house']
        sen_sponsored = all_sponsored.loc[all_sponsored['chamber'] == 'senate']

        x = house_sponsored['rep_sponsor']
        house_sponsored.loc[:, 'sponsor_percent'] = [stats.percentileofscore(x, a, 'weak') for a in x]
        house_sponsored.loc[:, 'sponsor_percent'] = house_sponsored.loc[:, 'sponsor_percent']/100

        x = sen_sponsored['rep_sponsor']
        sen_sponsored.loc[:, 'sponsor_percent'] = [stats.percentileofscore(x, a, 'weak') for a in x]
        sen_sponsored.loc[:, 'sponsor_percent'] = sen_sponsored.loc[:, 'sponsor_percent']/100

        all_sponsored = house_sponsored.append(sen_sponsored)
        
        ## Reduce weight
        all_sponsored['sponsor_percent'] = all_sponsored['sponsor_percent']*.5
        return all_sponsored[['bioguide_id', 'rep_sponsor', 'max_sponsor', 'sponsor_percent']]
        
        
    def get_leadership(self):
        self.leadership_df = pd.read_sql_query("""
        SELECT DISTINCT * FROM historical_leadership
        WHERE congress = {}
        ;""".format(self.congress), open_connection())
        
    def committee_membership(self):
        """
        Make sure you colect all 'present'
        congress people. If they don't show up
        in membership then they don't get
        a percentile. I can make it zero later,
        but then it messes up the lower percentile
        groups.
        """

        df = pd.read_sql_query("""
        SELECT DISTINCT bioguide_id,
        chamber
        FROM congress_bio
        WHERE served_until = 'Present'
        AND lower(state) != 'guam'
        AND lower(state) != 'puerto rico'
        AND lower(state) != 'district of columbia'
        AND lower(state) != 'virgin islands'
        AND lower(state) != 'american samoa'
        AND lower(state) != 'northern mariana islands'
        ;""", open_connection())

        house_membership = pd.read_sql_query("""
        SELECT DISTINCT bioguide_id,
        count(bioguide_id) memberhip
        FROM house_membership
        GROUP BY bioguide_id
        """, open_connection())

        senate_membership = pd.read_sql_query("""
        SELECT DISTINCT bioguide_id,
        count(bioguide_id) memberhip
        FROM senate_membership
        GROUP BY bioguide_id
        """, open_connection())

        ## Join all
        all_membersip = house_membership.append(senate_membership)
        df = pd.merge(df, all_membersip, how='left', on='bioguide_id').drop_duplicates('bioguide_id').fillna(0)

        ## Percentile by chamber
        x = df.loc[df['chamber'] == 'house', 'memberhip']
        df.loc[df['chamber'] == 'house', 'memberhip_percent'] = [stats.percentileofscore(x, a, 'weak') for a in x]
        x = df.loc[df['chamber'] == 'senate', 'memberhip']
        df.loc[df['chamber'] == 'senate', 'memberhip_percent'] = [stats.percentileofscore(x, a, 'weak') for a in x]

        ## Convert to percent
        df.loc[:, 'memberhip_percent'] = df.loc[:, 'memberhip_percent']/100
        
        ## Reduce weight
        df['memberhip_percent'] = df['memberhip_percent']*.5
        self.committee_membership = df
        
    def committee_leadership(self):
        """
        Make sure you colect all 'present'
        congress people. If they don't show up
        in membership then they don't get
        a percentile. I can make it zero later,
        but then it messes up the lower percentile
        groups.
        """

        df = pd.read_sql_query("""
        SELECT DISTINCT bioguide_id,
        chamber
        FROM congress_bio
        WHERE served_until = 'Present'
        AND lower(state) != 'guam'
        AND lower(state) != 'puerto rico'
        AND lower(state) != 'district of columbia'
        AND lower(state) != 'virgin islands'
        AND lower(state) != 'american samoa'
        AND lower(state) != 'northern mariana islands'
        ;""", open_connection())

        house_membership = pd.read_sql_query("""
        SELECT DISTINCT bioguide_id,
        count(bioguide_id) leadership
        FROM house_membership
        WHERE committee_leadership != 'None'
        GROUP BY bioguide_id
        """, open_connection())

        senate_membership = pd.read_sql_query("""
        SELECT DISTINCT bioguide_id,
        count(bioguide_id) leadership
        FROM senate_membership
        WHERE committee_leadership != 'None'
        GROUP BY bioguide_id
        """, open_connection())

        ## Join all
        all_membersip = house_membership.append(senate_membership)
        df = pd.merge(df, all_membersip, how='left', on='bioguide_id').drop_duplicates('bioguide_id').fillna(0)

        self.committee_leadership = df
        
    def became_law(self):
        df = pd.read_sql_query("""
        SELECT * FROM
            (
            SELECT issue_link, congress, tracker
            FROM all_legislation
            WHERE cast(congress as int) = {})
            AS this_congress
            LEFT JOIN bill_sponsors
            ON this_congress.issue_link = bill_sponsors.url
            WHERE bioguide_id != 'None'
        """.format(self.congress), open_connection())

        df = df.loc[df['tracker'] == 'Became Law']
        df_goruped = df.groupby(['bioguide_id']).count()['tracker'].reset_index(drop=False).sort_values(['tracker'],ascending=False)
        return df_goruped
        
    def total_grade_calc(self): 
        ## Collect stats
        voting = Grade_reps.get_participation_for_grade(self)
        sponsor = Grade_reps.rank_bills_made(self)
        became_law_df = Grade_reps.became_law(self)
        Grade_reps.get_leadership(self)
        if self.congress == 115:
            Grade_reps.committee_membership(self)
            Grade_reps.committee_leadership(self)

        ## Join data
        total_grades = pd.merge(voting, sponsor[['bioguide_id', 'sponsor_percent']],
                 how='left', on='bioguide_id').drop_duplicates('bioguide_id')
        total_grades = pd.merge(total_grades, became_law_df, how='left', on='bioguide_id').drop_duplicates('bioguide_id').fillna(0)
        total_grades = pd.merge(total_grades, self.leadership_df[['bioguide_id', 'position']],
                                    how='left', on='bioguide_id').drop_duplicates('bioguide_id').fillna(0)
        if self.congress == 115:
            total_grades = pd.merge(total_grades, self.committee_membership[['bioguide_id', 'memberhip_percent']],
                                    how='left', on='bioguide_id').drop_duplicates('bioguide_id').fillna(0)
            total_grades = pd.merge(total_grades, self.committee_leadership[['bioguide_id', 'leadership']],
                                    how='left', on='bioguide_id').drop_duplicates('bioguide_id').fillna(0)
            
            
        ## Make total grade - No extra credit
        if self.congress == 115:
            total_grades['total_grade'] = (((total_grades['participation_percent']*2) + 
                                           total_grades['sponsor_percent'] +
                                          total_grades['memberhip_percent'])/3)
        else:
            total_grades['total_grade'] = (((total_grades['participation_percent']*2) +
                                           total_grades['sponsor_percent'])/2.5)

        ## Extra Credit: Became Law
        total_grades.loc[:, 'total_grade_extra_credit'] = (total_grades['tracker'] * .02) + total_grades['total_grade']
        
        ## Extra Credit: Congress Leadership
        total_grades.loc[((total_grades['position'] != 0) &
                          (total_grades['position'] != 'Speaker of the House')), 
                         'total_grade_extra_credit'] = (total_grades.loc[((total_grades['position'] != 0) &
                                                                          (total_grades['position'] != 'Speaker of the House')), 
                                                                         'total_grade_extra_credit'] + .1)

        if self.congress == 115:
            ## Extra Credit: Committee Leadership
            total_grades.loc[total_grades['chamber'] == 'house', 'total_grade_extra_credit'] = (
                total_grades.loc[total_grades['chamber'] == 'house', 'total_grade_extra_credit'] +
                (total_grades.loc[total_grades['chamber'] == 'house', 'leadership']/50))
            total_grades.loc[total_grades['chamber'] == 'senate', 'total_grade_extra_credit'] = (
                total_grades.loc[total_grades['chamber'] == 'senate', 'total_grade_extra_credit'] +
                (total_grades.loc[total_grades['chamber'] == 'senate', 'leadership']/200))

        ## Gaurdrails
        total_grades.loc[total_grades['total_grade_extra_credit'] > 1, 'total_grade_extra_credit'] = 1

        ## Map to letter grades
        grades = {'100 - 109': 'A+', '93 - 99': 'A', '90 - 92': 'A-',
                  '87 - 89': 'B+', '83 - 86': 'B', '80 - 82': 'B-',
                  '77 - 79': 'C+', '73 - 76': 'C', '70 - 72': 'C-',
                  '67 - 69': 'D+', '63 - 66': 'D', '60 - 62': 'D-',
                  '0 - 59': 'F'}

        total_grades.loc[:, 'grade_int'] = (total_grades.loc[total_grades['total_grade'].notnull(), 
                                       'total_grade']*100).astype(int)
        for grade in grades:
            total_grades.loc[((total_grades['grade_int'] >= int(grade.split(' - ')[0])) &
                                           (total_grades['grade_int'] <= int(grade.split(' - ')[1]))),
                                          'letter_grade'] = grades[grade]

        total_grades.loc[:, 'grade_int'] = (total_grades.loc[total_grades['total_grade_extra_credit'].notnull(), 
                                       'total_grade_extra_credit']*100).astype(int)
        for grade in grades:
            total_grades.loc[((total_grades['grade_int'] >= int(grade.split(' - ')[0])) &
                                           (total_grades['grade_int'] <= int(grade.split(' - ')[1]))),
                                          'letter_grade_extra_credit'] = grades[grade]
            
        total_grades = total_grades.drop(['grade_int'], 1)

        ## Remove speaker of the house from grading
        total_grades.loc[((total_grades['position'] == 'Speaker of the House')),
                        ['total_grade', 'total_grade_extra_credit',
                        'letter_grade', 'letter_grade_extra_credit']] = None
        
        total_grades.loc[:, 'congress'] = self.congress

        self.congress_grades = total_grades.sort_values(['total_grade'],ascending=False).reset_index(drop=True)
        
    def grades_to_sql(self):

        connection = open_connection()
        cursor = connection.cursor()

        self.congress_grades = self.congress_grades[['bioguide_id',
                                                     'chamber',
                                                    'congress',
                                                    'leadership',
                                                    'position',
                                                    'letter_grade',
                                                    'letter_grade_extra_credit',
                                                    'memberhip_percent',
                                                    'participation_percent',
                                                    'sponsor_percent',
                                                    'total_grade',
                                                    'total_grade_extra_credit',
                                                    'tracker']]
        ## Put data into table
        for i in range(len(self.congress_grades)):
            x = list(self.congress_grades.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO congress_grades (
                bioguide_id,
                chamber,
                congress,
                leadership,
                leadership_position,
                letter_grade,
                letter_grade_extra_credit,
                memberhip_percent,
                participation_percent,
                sponsor_percent,
                total_grade,
                total_grade_extra_credit,
                tracker)
                VALUES ('{bioguide_id}', '{chamber}', '{congress}', '{leadership}',
                 '{leadership_position}', '{letter_grade}', '{letter_grade_extra_credit}', '{memberhip_percent}', 
                 '{participation_percent}', '{sponsor_percent}', '{total_grade}', 
                 '{total_grade_extra_credit}', '{tracker}');"""


                sql_command = format_str.format(bioguide_id=p[0], chamber=p[1], congress=int(p[2]), leadership=int(p[3]),
                 leadership_position=p[4], letter_grade=p[5], letter_grade_extra_credit=p[6], memberhip_percent=p[7], 
                 participation_percent=p[8], sponsor_percent=p[9], total_grade=p[10], 
                total_grade_extra_credit=p[11], tracker=int(p[12]))


                try:
                    # Try to insert, if it can't inset then it should update
                    cursor.execute(sql_command)
                    connection.commit()
                except:
                    connection.rollback()
                    ## If the update breaks then something is wrong
                    sql_command = """UPDATE congress_grades 
                    SET  
                    chamber='{}', 
                    leadership='{}',
                    leadership_position='{}',
                    letter_grade='{}', 
                    letter_grade_extra_credit='{}', 
                    memberhip_percent='{}', 
                    participation_percent='{}',  
                    sponsor_percent='{}',
                    total_grade='{}', 
                    total_grade_extra_credit='{}', 
                    tracker='{}'
                    where (bioguide_id = '{}' AND congress = '{}');""".format(
                    self.congress_grades.loc[i, 'chamber'],
                    int(self.congress_grades.loc[i, 'leadership']),
                    self.congress_grades.loc[i, 'position'],
                    self.congress_grades.loc[i, 'letter_grade'],
                    self.congress_grades.loc[i, 'letter_grade_extra_credit'],
                    self.congress_grades.loc[i, 'memberhip_percent'],
                    self.congress_grades.loc[i, 'participation_percent'],
                    self.congress_grades.loc[i, 'sponsor_percent'],
                    self.congress_grades.loc[i, 'total_grade'],
                    self.congress_grades.loc[i, 'total_grade_extra_credit'],
                    int(self.congress_grades.loc[i, 'tracker']),
                    self.congress_grades.loc[i, 'bioguide_id'],
                    int(self.congress_grades.loc[i, 'congress']))

                    cursor.execute(sql_command)
                    connection.commit()

        ## Close yo shit
        connection.close()

    def current_congress_num(self):
        """
        This method will be used to find the
        maximum congresss number. The max
        congress will be the current congress.
        """
        
        cong_num = pd.read_sql_query("""select max(congress) from house_vote_menu;""",open_connection())
        self.congress = cong_num.loc[0, 'max']

    def __init__(self, leadership_df=None, committee_membership=None, committee_leadership=None,
                congress=None, congress_grades=None):
        self.leadership_df = leadership_df
        self.committee_membership = committee_membership
        self.committee_leadership = committee_leadership
        self.congress = congress
        self.congress_grades = congress_grades

class Ideology(object):
    """
    This class will be used to classify ideologies. It will be used for
    reps and users. In order to idendify which bills are predictive
    we need to observe from reps, then pass those bills to users to
    vote on.
    
    Right now this is starting with house. I don't know how it will change things
    looking at the senate. Theoretically bills that are polarizing for the house
    should also be for the senate. More works needs to be done to verify that though.
    
    Attributes:
    
    """
    
    def get_ideology_stats(self):    
        """Find the ideology stats to remove the inputs needed."""

        self.ideology_df = pd.read_sql_query("""SELECT * 
        FROM representatives_ideology_stats
        WHERE ideology_type = '{}'""".format(self.ideology.lower()), open_connection())

    def get_votes_to_predict_ideology(self):
        if self.ideology.lower() == 'women and minority rights':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(policy_area) ilike '%' || 'minority issues' || '%'
                OR lower(policy_area) ilike '%' || 'disabled' || '%'
                OR lower(policy_area) ilike '%' || 'women' || '%')
                AS wm_rights
                LEFT JOIN bill_sponsors
                ON wm_rights.issue_link = bill_sponsors.url
            ;""", open_connection())
        elif self.ideology.lower() == 'immigration':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(policy_area) ilike '%' || 'immigration' || '%')
                AS immigration
                LEFT JOIN bill_sponsors
                ON immigration.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'abortion':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(policy_area) ilike '%' || 'abortion' || '%'
                OR (lower(title_description) ilike '%' || 'abortion' || '%'
                AND lower(policy_area) ilike '%' || 'health' || '%')
                OR (lower(title_description) ilike '%' || 'abortion' || '%') 
                OR (lower(title_description) ilike '%' || 'born-alive' || '%')
                OR (lower(title_description) ilike '%' || 'unborn child' || '%')
                OR (lower(title_description) ilike '%' || ' reproductive' || '%')
                OR (lower(title_description) ilike '%' || 'planned parenthood' || '%'))
                AS abortion
                LEFT JOIN bill_sponsors
                ON abortion.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'environmental protection':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(policy_area) ilike '%' || 'environmental protection' || '%')
                AS environment
                LEFT JOIN bill_sponsors
                ON environment.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'second amendment':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(title_description) ilike '%' || 'second amendment' || '%'
                OR lower(title_description) ilike '%' || 'gun' || '%'
                OR lower(title_description) ilike '%' || 'firearm' || '%')
                AS second_amendment
                LEFT JOIN bill_sponsors
                ON second_amendment.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'obamacare':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(title_description) ilike '%' || 'obamacare' || '%'
                OR lower(title_description) ilike '%' || 'affordable care act' || '%'
                OR lower(title_description) ilike '%' || 'individual mandate' || '%'
                OR lower(title_description) ilike '%' || ' aca ' || '%')
                AS obama
                LEFT JOIN bill_sponsors
                ON obama.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'lgbt rights':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(title_description) ilike '%' || 'same-sex' || '%'
                OR lower(title_description) ilike '%' || 'defense of marriage act' || '%'
                OR lower(title_description) ilike '%' || ' doma ' || '%'
                OR lower(title_description) ilike '%' || 'lesbian' || '%'
                OR lower(title_description) ilike '%' || ' bisexual ' || '%'
                OR lower(title_description) ilike '%' || ' transgender ' || '%'
                OR lower(title_description) ilike '%' || 'lgbt' || '%'
                OR lower(title_description) ilike '%' || 'don''t ask, don''t tell' || '%')
                AS lgbt
                LEFT JOIN bill_sponsors
                ON lgbt.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'taxes':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(title_description) ilike '%' || 'progressive tax' || '%'
                OR lower(title_description) ilike '%' || 'flat tax' || '%'
                OR lower(title_description) ilike '%' || 'fair tax' || '%'
                OR lower(title_description) ilike '%' || 'fairtax' || '%'
                OR lower(title_description) ilike '%' || 'death tax' || '%'
                OR lower(title_description) ilike '%' || 'marriage penalty' || '%'
                OR lower(title_description) ilike '%' || 'tax breaks' || '%'
                OR lower(title_description) ilike '%' || 'capital gains' || '%'
                OR lower(title_description) ilike '%' || 'estate tax' || '%'
                OR lower(title_description) ilike '%' || 'alternative minimum tax' || '%'
                OR lower(title_description) ilike '%' || 'corporate tax' || '%'
                OR lower(title_description) ilike '%' || 'monetary policy reform' || '%'
                OR lower(title_description) ilike '%' || 'amt relief' || '%'
                OR lower(title_description) ilike '%' || 'offshore deferred compensation' || '%'
                )
                AS taxes
                LEFT JOIN bill_sponsors
                ON taxes.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'homeland security':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE lower(title_description) ilike '%' || 'protect america act' || '%'
                OR (lower(title_description) ilike '%' || 'patriot act' || '%'
                AND (policy_area ilike '%' || 'Armed Forces and National Security' || '%'
                OR policy_area ilike '%' || 'International Affairs' || '%'))
                OR lower(title_description) ilike '%' || '9/11 Commission report' || '%'
                OR lower(title_description) ilike '%' || 'aviation security' || '%'
                OR lower(title_description) ilike '%' || 'al Qaeda' || '%'
                OR lower(title_description) ilike '%' || 'war on terror' || '%'
                OR lower(title_description) ilike '%' || 'guantanamo' || '%'
                OR lower(title_description) ilike '%' || ' ISIS ' || '%'
                OR lower(title_description) ilike '%' || ' ISIL ' || '%'
                OR ((lower(title_description) ilike '%' || 'Iraq' || '%'
                OR lower(title_description) ilike '%' || 'Afghanistan' || '%')
                AND (policy_area ilike '%' || 'Armed Forces and National Security' || '%'
                OR policy_area ilike '%' || 'International Affairs' || '%'))
                OR (lower(title_description) ilike '%' || 'licens' || '%'
                AND lower(title_description) ilike '%' || 'terror' || '%')
                )
                AS homeland_sec
                LEFT JOIN bill_sponsors
                ON homeland_sec.issue_link = bill_sponsors.url
                ;""", open_connection())
        elif self.ideology.lower() == 'stimulus or market led recovery':
            bills = pd.read_sql_query("""
                SELECT * FROM (
                SELECT * FROM all_legislation
                WHERE (lower(title_description) ilike '%' || 'Economic Recovery Act' || '%'
                AND (policy_area ilike '%' || 'Finance and Financial Sector' || '%'
                OR policy_area ilike '%' || 'Taxation' || '%'
                OR policy_area ilike '%' || 'Commerce' || '%'
                OR policy_area ilike '%' || 'Economics and Public Finance' || '%'
                OR policy_area ilike '%' || 'Government Operations and Politics' || '%'))
                OR lower(title_description) ilike '%' || 'Full Faith and Credit Act' || '%'
                OR lower(title_description) ilike '%' || 'Omnibus Appropriations' || '%'
                OR lower(title_description) ilike '%' || 'Helping Families Save Their Homes' || '%'
                OR lower(title_description) ilike '%' || 'Homeownership preservation' || '%'
                OR lower(title_description) ilike '%' || ' prevent mortgage foreclosures ' || '%'
                OR lower(title_description) ilike '%' || 'American Recovery and Reinvestment' || '%'
                OR lower(title_description) ilike '%' || 'Unemployment Insurance Modernization Act' || '%'
                OR lower(title_description) ilike '%' || 'Auto Industry Financing and Restructuring Act' || '%'
                OR lower(title_description) ilike '%' || 'Job Creation and Unemployment Relief Act' || '%'
                OR lower(title_description) ilike '%' || 'supplemental appropriations for job' || '%'
                OR lower(title_description) ilike '%' || 'Mortgage Reform' || '%'
                )
                AS stimulus
                LEFT JOIN bill_sponsors
                ON stimulus.issue_link = bill_sponsors.url
                ;""", open_connection())

        else:
            print 'incorrect ideology'
            return

        """
        In this section I'm going to collect and clean
        the data for the ideology in question.
        """
        ## Query the roll call bills
        sql_query = 'SELECT * FROM house_vote_menu'
        counter = 0
        for url in bills['issue_link']:
            if counter != 0:
                sql_query += " OR issue_link = '{}'".format(url)
            elif counter == 0:
                sql_query += " WHERE issue_link = '{}'".format(url)
            counter += 1

        sql_query += ';'
        df_house = pd.read_sql_query(sql_query, open_connection())


        ## Query the roll call votes
        ## If there are a lot of bills it goes slow
        ## Query in batches
        predictive_bills_votes_house = pd.DataFrame()
        for i in range(0, len(df_house), 5):
            sql_query = 'SELECT * FROM house_votes_tbl'
            for j in range(i, i+5):
                if j != i:
                    try:
                        sql_query += " OR roll_id = {}".format(df_house.loc[j, 'roll_id'])
                    except:
                        "doesn't go that high"
                elif j == i:
                    sql_query += " WHERE roll_id = {}".format(df_house.loc[j, 'roll_id'])
            sql_query += ";"

            x = pd.read_sql_query(sql_query, open_connection())
            predictive_bills_votes_house = predictive_bills_votes_house.append(x)


        predictive_bills_votes_house.loc[:, 'chamber'] = 'house'
        predictive_bills_votes_house = predictive_bills_votes_house.drop('role', 1)
        predictive_bills_votes_house = predictive_bills_votes_house.loc[predictive_bills_votes_house['bioguide_id'] != 'None']
        predictive_bills_votes_house = predictive_bills_votes_house.reset_index(drop=True)


        ## Query the roll call votes
        """
        Find votes for senate. Because senate roll_call
        doesn't have bioguide_ids I need to find them
        from the congress_bio table
        """

        ## Convert issues column
        for i in range(len(bills)):
            issue = re.split('(\d+)', bills.loc[i, 'issue'])
            issue_split = ''
            for word in issue:
                if len(word) > 0:
                    issue_split += word
                    issue_split += ' '
            bills.loc[i, 'issue'] = (issue_split).strip().lower()

        ## Find bills from Senate vote menu
        sql_query = 'SELECT * FROM senate_vote_menu'
        for i in range(len(bills)):
            if i != 0:
                sql_query += " OR (lower(issue) = '{}' AND congress = {})".format(bills.loc[i, 'issue'],
                                                                bills.loc[i, 'congress'])
            elif i == 0:
                sql_query += " WHERE (lower(issue) = '{}' AND congress = {})".format(bills.loc[i, 'issue'],
                                                                bills.loc[i, 'congress'])

        sql_query += ';'
        df_senate = pd.read_sql_query(sql_query, open_connection())


        ## Find the roll_ids from the senate vote table
        predictive_bills_votes_senate = pd.DataFrame()
        for i in range(0, len(df_senate), 5):
            sql_query = 'SELECT * FROM senator_votes_tbl'
            for j in range(i, i+5):
                if j != i:
                    try:
                        sql_query += " OR roll_id = {}".format(df_senate.loc[j, 'roll_id'])
                    except:
                        "doesn't go that high"
                elif j == i:
                    sql_query += " WHERE roll_id = {}".format(df_senate.loc[j, 'roll_id'])
            sql_query += ";"

            x = pd.read_sql_query(sql_query, open_connection())
            predictive_bills_votes_senate = predictive_bills_votes_senate.append(x)
        find_senator = pd.read_sql("""
        SELECT DISTINCT name,
        bioguide_id,
        state
        FROM (
        SELECT * FROM 
        congress_bio 
        WHERE chamber = 'senate'
        ORDER BY year_elected)
        AS bio_sorted;""", open_connection())
        find_senator['last_name'] = find_senator['name'].apply(lambda x: x.split(',')[0].split(' ')[0])
        find_senator = find_senator.drop_duplicates(['bioguide_id']).reset_index(drop=True)
        predictive_bills_votes_senate.loc[:, 'state_long'] = predictive_bills_votes_senate.loc[:, 'state'].apply(lambda x: str(us.states.lookup(x)))
        predictive_bills_votes_senate = pd.merge(predictive_bills_votes_senate, find_senator, how='left', 
                           left_on=['state_long', 'last_name'],
                          right_on=['state', 'last_name'])
        predictive_bills_votes_senate = predictive_bills_votes_senate.loc[predictive_bills_votes_senate['bioguide_id'].notnull()]
        predictive_bills_votes_senate = predictive_bills_votes_senate[['member_full', 'bioguide_id', 'party',
                                                                       'state_x','vote_cast', 'year', 'roll',
                                                                       'congress', 'session', 'date', 'roll_id']]
        predictive_bills_votes_senate.columns = ['member_full', 'bioguide_id', 'party', 'state',
                                      'vote', 'year', 'roll', 'congress', 'session', 'date', 'roll_id']
        predictive_bills_votes_senate.loc[:, 'chamber'] = 'senate'
        predictive_bills_votes_senate = predictive_bills_votes_senate.loc[predictive_bills_votes_senate['bioguide_id'] != 'None']
        predictive_bills_votes_senate = predictive_bills_votes_senate.reset_index(drop=True)


        ## Put house and senate votes together

        self.predictive_bills_votes = predictive_bills_votes_senate.append(predictive_bills_votes_house).reset_index(drop=True)
        self.bills_df = bills
        
    def find_question_breakdown(self):
        ## Subset the roll call vote and take only needed data
        test_scores = self.predictive_bills_votes.loc[((self.predictive_bills_votes['roll_id'] == self.roll_id) &
                                                 (self.predictive_bills_votes['chamber'] == self.chamber))].reset_index(drop=True)
        test_scores = test_scores[['member_full', 'vote', 'bioguide_id']]

        ## Subset by ideology testing
        ideolog_df_subset = self.ideology_df.loc[self.ideology_df['ideology_type'] == 
                                 self.ideology, ['bioguide_id', 'tally_score']]

        ## Add ideology to votes
        test_scores = pd.merge(test_scores, ideolog_df_subset[['tally_score', 'bioguide_id']],
                 how='left', on='bioguide_id')

        ## Convert numeric ideology to categorical
        """
        Since the columns are floats then I want
        to take anything that is between -1 and 1 and
        make them neutral."
        """
        test_scores.loc[test_scores['tally_score'] < -.5, 'ideology'] = 'l'
        test_scores.loc[test_scores['tally_score'] > .5, 'ideology'] = 'c'
        test_scores.loc[((test_scores['tally_score'] < .5) &
                         (test_scores['tally_score'] > -.5)), 'ideology'] = 'n'

        ## Group categoritycal ideology and only look at actual votes
        x = test_scores.groupby(['ideology', 'vote']).count()['bioguide_id'].reset_index(drop=False)
        x = x.loc[x['vote'] != 'Not Voting'].reset_index(drop=True)
        x = x.loc[x['vote'] != 'Present'].reset_index(drop=True)

        ## Get ideology breakdown by vote type
        """
        This section is very important. Breakdown here will be used 
        to generate the tally ideology. Each vote will has a probability
        for the ideology. The ideologies will be add up. And in the end 
        I will compare how conservative someone is compared to how liberal
        they are. By summing the probabilities it will account for weight
        issues. For example, if a person votes on a bill the way a conservatives 
        votes we could say they are 100% in agreement with how the majority
        of conservatives vote. But eventually we get into a problem of comparing
        someone who has voted once and someone who has voted 100 times. If they
        both vote the way the majority of conservatives then they will have even
        scores. However we know that the person that voted 100 times is more
        likey to be conservative than the person who voted once. Using addative
        probability will discriminate against these two.
        """
        if len(np.unique(x['vote'])) > 1:
            for vote in np.unique(x['vote']):
                try:
                    x.loc[x['vote'] == vote, 'ideology_vote_percent'] = (x.loc[x['vote'] == vote, 'bioguide_id']/
                                                                         x.loc[x['vote'] == vote, 'bioguide_id'].sum())
                except:
                    'no scores at that number'

            x.columns = ['ideology',
                        'vote',
                        'vote_count', 
                        'percent_breakdown']
            x.loc[:, 'roll_id'] = self.roll_id
            x.loc[:, 'chamber'] = self.chamber
            return x
        else:
            return pd.DataFrame()
        
    def make_master_ideology(self):
        """
        This function is going to make a master ideology data set.
        It will pass each unique roll_id to the "find_question_breakdown"
        function and append the results to a master data set.
        The master data set will be a vote breakdown for each bill.
        It will show the breakdown of how each ideology voted on a bill.
        The point will be to use this data set to determine someone's
        ideology through addative probability. e.g. If someone votes 
        'Yea' on a bill then they will be assignes 3 scores, 
        a liberal (l) score, a conservative (c) score, and a
        neutral (n) score. 

        Inputs
        df: the data set that has the votes
        finalized_ideology_stats: list of each persons ideology
        ideology: the ideology you want to score

        Output
        A master ideology data set.
        """

        ## Create variable for the master ideology stats
        master_ideology_explore = pd.DataFrame()

        ## Subset dataset to only look at unique roll call votes
        df_subset = self.predictive_bills_votes.loc[:, ['roll_id', 'chamber']].drop_duplicates().reset_index(drop=True)
        for i in range(len(df_subset)):
            self.roll_id = df_subset.loc[i, 'roll_id']
            self.chamber = df_subset.loc[i, 'chamber']
            x = Ideology.find_question_breakdown(self)
            master_ideology_explore = master_ideology_explore.append(x)

        self.master_ideology = master_ideology_explore.reset_index(drop=True)
        
    def find_partisan_bills(self):
        """
        This function looks through the vote ideology breakdown in order
        to find pratisan bills. Partisan bills are import to find because
        they are the bills that will offer the most predictive power.

        In this function I am only looking at conservative and liberal ideologies.
        There's not much use knowing the neutral breakdown because I want to find 
        the most divisive bills. I will then count the number of conservative
        and liberal votes per bill. If the number for either ideology is less
        than 25 I don't use it because there are not enough data points to offer
        statistical power. I then get the percent breakdown for how each 
        ideology voted. The remaining data represent the majority of how the
        ideology voted on a bill.

        After the ideology voting stats have been made I check to see if the 
        most common vote for each ideology per bill match. If they match then
        the bill is not divisive and is therefore not partisan. However, if the
        highest percent vote by ideology per bill does not match then it is a 
        divisive bill and is partisan. I then keep only the bills that are
        labeled as partisan.

        Input
        ideology_vote_breakdown: The data set created from make_master_ideology.

        Output
        A list of partisan bills to use to predict ideology.

        """

        ideology_vote_breakdown = self.master_ideology.loc[(self.master_ideology['ideology'] != 'n')]
        ideology_vote_breakdown.loc[:, 'ideology_vote_percent'] = None

        bill_finder = ideology_vote_breakdown.loc[:, ['roll_id', 'chamber']].drop_duplicates().reset_index(drop=True)
        for i in range(len(bill_finder)):
            roll_id = int(bill_finder.loc[i, 'roll_id'])
            chamber = bill_finder.loc[i, 'chamber']

            ## Count the number of votes by ideology per bill
            total_l_votes = ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                                        (ideology_vote_breakdown['chamber'] == chamber) &
                                                        (ideology_vote_breakdown['ideology'] == 'l')), 'vote_count'].sum()
            total_c_votes = ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                                        (ideology_vote_breakdown['chamber'] == chamber) &
                                                        (ideology_vote_breakdown['ideology'] == 'c')), 'vote_count'].sum()

            ## If there are more than 25 data points for l and c votes than stats can be used
            if (total_l_votes >= 10) & (total_c_votes >= 10):

                ## Percent of votes by ideology
                ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                            (ideology_vote_breakdown['chamber'] == chamber) &
                                            (ideology_vote_breakdown['ideology'] == 'l')), 'ideology_vote_percent'] = (
                    ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                                (ideology_vote_breakdown['chamber'] == chamber) &
                                                (ideology_vote_breakdown['ideology'] == 'l')), 'vote_count']/total_l_votes)

                ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                            (ideology_vote_breakdown['chamber'] == chamber) &
                                            (ideology_vote_breakdown['ideology'] == 'c')), 'ideology_vote_percent'] = (
                    ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                                (ideology_vote_breakdown['chamber'] == chamber) &
                                                (ideology_vote_breakdown['ideology'] == 'c')), 'vote_count']/total_c_votes)




        partisan_bills_only = pd.DataFrame()
        for i in range(len(bill_finder)):
            roll_id = int(bill_finder.loc[i, 'roll_id'])
            chamber = bill_finder.loc[i, 'chamber']
            try:
                c_vote = ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                                    (ideology_vote_breakdown['chamber'] == chamber) &
                                                    (ideology_vote_breakdown['ideology'] == 'c'))].sort_values('ideology_vote_percent',
                                                    ascending=False).reset_index(drop=True).loc[0, 'vote']

                l_vote = ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                                    (ideology_vote_breakdown['chamber'] == chamber) &
                                                    (ideology_vote_breakdown['ideology'] == 'l'))].sort_values('ideology_vote_percent', ascending=False).reset_index(drop=True).loc[0, 'vote']

                if (c_vote == l_vote):
                    'nonpartisan'
                else:
                    partisan_bills_only = partisan_bills_only.append(
                        ideology_vote_breakdown.loc[((ideology_vote_breakdown['roll_id'] == roll_id) &
                                                    (ideology_vote_breakdown['chamber'] == chamber))])
            except:
                'Only one type of vote'

        partisan_bills_only = partisan_bills_only.loc[partisan_bills_only['ideology_vote_percent'].notnull()]
        self.partisan_bills_only = partisan_bills_only.reset_index(drop=True)
        
    def get_probs(self):

        """
        This function will create a raw total conservative
        and liberal probabilities based on previous votes.
        It will then generate a ideology probability which
        is the conservative probability subracted by the
        liberal probability.

        Input
        df: Dataset with the raw votes
        partisan_bill_stats: output of find_partisan_bills

        Output
        ideology probability and tally score

        """

        ## Make master data frame
        ideology_stats_by_rep = self.predictive_bills_votes[['bioguide_id', 'vote', 'roll_id', 'chamber']]
        ideology_stats_by_rep = ideology_stats_by_rep.loc[ideology_stats_by_rep['vote'] != 'Not Voting']
        ideology_stats_by_rep = ideology_stats_by_rep.loc[ideology_stats_by_rep['vote'] != 'Present']
        ideology_stats_by_rep.loc[:, 'roll_id'] = ideology_stats_by_rep.loc[:, 'roll_id'].astype(int)
        ideology_stats_by_rep.loc[:, 'c_prob'] = None
        ideology_stats_by_rep.loc[:, 'l_prob'] = None
        x = pd.DataFrame(np.unique(self.partisan_bills_only['roll_id']), columns=['roll_id'])
        x.loc[:, 'roll_id'] = x.loc[:, 'roll_id'].astype(int)
        ideology_stats_by_rep = pd.merge(x, ideology_stats_by_rep, how='left', on='roll_id')


        ## For each bill and each vote get probability breakdown and save it to reps

        bill_finder = self.partisan_bills_only.loc[:, ['roll_id', 'chamber']].drop_duplicates().reset_index(drop=True)
        for i in range(len(bill_finder)):
            roll_id = int(bill_finder.loc[i, 'roll_id'])
            chamber = bill_finder.loc[i, 'chamber']
            bill_finder_sub = self.partisan_bills_only.loc[((self.partisan_bills_only['roll_id'] == roll_id) & 
                (self.partisan_bills_only['chamber'] == chamber))]
            for vote in np.unique(bill_finder_sub.loc[:, 'vote']):
                ## Get probs by vote per roll_id
                if (len(self.partisan_bills_only.loc[((self.partisan_bills_only['roll_id'] ==  roll_id) &
                                (self.partisan_bills_only['chamber'] ==  chamber) & 
                                (self.partisan_bills_only['vote'] == vote) &
                                (self.partisan_bills_only['ideology'] == 'c'))])) > 0:
                    c_prob = float(self.partisan_bills_only.loc[((self.partisan_bills_only['roll_id'] ==  roll_id) &
                                    (self.partisan_bills_only['chamber'] ==  chamber) &
                                    (self.partisan_bills_only['vote'] == vote) &
                                    (self.partisan_bills_only['ideology'] == 'c')), 'ideology_vote_percent'].reset_index(drop=True)[0])
                else:
                    c_prob = 0

                if (len(self.partisan_bills_only.loc[((self.partisan_bills_only['roll_id'] ==  roll_id) &
                                (self.partisan_bills_only['chamber'] ==  chamber) &
                                (self.partisan_bills_only['vote'] == vote) &
                                (self.partisan_bills_only['ideology'] == 'l'))])) > 0:
                    l_prob = float(self.partisan_bills_only.loc[((self.partisan_bills_only['roll_id'] ==  roll_id) &
                                    (self.partisan_bills_only['chamber'] ==  chamber) &
                                    (self.partisan_bills_only['vote'] == vote) &
                                    (self.partisan_bills_only['ideology'] == 'l')), 'ideology_vote_percent'].reset_index(drop=True)[0])
                else:
                    l_prob = 0


                ## Save each reps conservative and liberal probability per vote
                ideology_stats_by_rep.loc[((ideology_stats_by_rep['roll_id'] == int(roll_id)) &
                                        (ideology_stats_by_rep['chamber'] == chamber) & 
                                        (ideology_stats_by_rep['vote'] == vote)), 'c_prob'] = c_prob
                ideology_stats_by_rep.loc[((ideology_stats_by_rep['roll_id'] == int(roll_id)) &
                                        (ideology_stats_by_rep['chamber'] == chamber) &
                                        (ideology_stats_by_rep['vote'] == vote)), 'l_prob'] = l_prob


        ideology_stats_by_rep_sums = ideology_stats_by_rep[['bioguide_id', 'c_prob', 'l_prob']].groupby(['bioguide_id']).sum().reset_index(drop=False)
        ideology_stats_by_rep_sums.loc[:, 'ideology_prob'] = (ideology_stats_by_rep_sums['c_prob'] - ideology_stats_by_rep_sums['l_prob'])

        ## Add the number of votes the rep partook in
        total_votes_df = ideology_stats_by_rep.groupby('bioguide_id').count()['roll_id'].reset_index(drop=False)
        total_votes_df.columns = ['bioguide_id', 'total_votes']

        ideology_stats_by_rep_sums = pd.merge(ideology_stats_by_rep_sums,total_votes_df,how='left', on='bioguide_id')

        
        
        master_df = pd.DataFrame()

        for i in range(len(self.bills_df)):
            master_df = master_df.append(Ideology.bill_stance(self, i))
            
        scores_df = master_df.groupby(['bioguide_id']).sum().reset_index(drop=False)[['bioguide_id', 'bill_score']]
        scores_df.loc[:, 'total'] = master_df.groupby(['bioguide_id']).count().reset_index(drop=True)['bill_score']
        scores_df.columns = ['bioguide_id', 'sponsorship_score', 'total_sponsorship']
        
        ideology_stats_by_rep_sums = pd.merge(scores_df, 
                                              ideology_stats_by_rep_sums, 
                                              how='outer', on='bioguide_id').fillna(0)
        
        ideology_stats_by_rep_sums['s_v_score'] = (ideology_stats_by_rep_sums['sponsorship_score'] + 
                                                   ideology_stats_by_rep_sums['ideology_prob'])
        ideology_stats_by_rep_sums['total_actions'] = (ideology_stats_by_rep_sums['total_sponsorship'] + 
                                                       ideology_stats_by_rep_sums['total_votes'])
        ideology_stats_by_rep_sums = ideology_stats_by_rep_sums.loc[ideology_stats_by_rep_sums['total_actions'] > 2].reset_index(drop=True)
        ideology_stats_by_rep_sums.loc[:, 'neutral_index'] = abs(ideology_stats_by_rep_sums['s_v_score']/
                                                       ideology_stats_by_rep_sums['total_actions'])
        ideology_stats_by_rep_sums['s_v_score_normalized'] =  (ideology_stats_by_rep_sums['s_v_score']/
                                                               ideology_stats_by_rep_sums['total_actions'])

        ## Assign z-scores
        ## call it "ideology_prob_x_zero_mean"
        mew = float(ideology_stats_by_rep_sums.loc[ideology_stats_by_rep_sums['neutral_index'] == 
                                                   ideology_stats_by_rep_sums['neutral_index'].min(), 
                                                   's_v_score_normalized'].reset_index(drop=True)[0])
        standard_d = np.std(ideology_stats_by_rep_sums['s_v_score_normalized'])
        ideology_stats_by_rep_sums.loc[:, 'z_scores'] = ideology_stats_by_rep_sums.loc[:, 's_v_score_normalized'].apply(lambda x: (x - mew)/standard_d)

        ## Stretch z-scores to be between -3 and 3
        f_max = ideology_stats_by_rep_sums['z_scores'].max()
        f_min = ideology_stats_by_rep_sums['z_scores'].min()
        f_bar = ((f_max + f_min)/2)
        A = (2/(f_max - f_min))
        ideology_stats_by_rep_sums.loc[:, 'tally_score'] = ideology_stats_by_rep_sums.loc[:, 'z_scores'].apply(lambda x: round(A*(x - f_bar), 4) * 3)

        ideology_stats_by_rep_sums.loc[:, 'ideology_type'] = self.ideology
        ideology_stats_by_rep_sums.loc[:, 'type'] = self.ideology_type
        
        self.ideology_stats_by_rep_sums = ideology_stats_by_rep_sums

    def bill_stance(self, index):
        try:
            df_people = self.bills_df.loc[index, 'cosponsor_bioguide_id']
            df_people = df_people.strip('[]')
            df_people = pd.DataFrame(df_people.split(', '), columns=['bioguide_id'])
            df_people.loc[len(df_people), 'bioguide_id'] = self.bills_df.loc[index ,'bioguide_id']

            df_people = pd.merge(df_people, self.ideology_df[['bioguide_id', 'tally_score']],
                     how='left', on='bioguide_id')

            df_people = df_people.loc[df_people['tally_score'].notnull()]

            if len(df_people) > 9:
                bill_score = (df_people.loc[:, 'tally_score'].mean()) / 3
                df_people.loc[:, 'bill_score'] = bill_score
                return df_people

            else:
                return None
        except:
            return None
    
        
    def make_tally_score(self):
        """
        This method will be used to do all the
        work to make ideologies. After this method
        is call I'll need to pass the created ideologies
        through a method to put them to sql.
        """

        ## Grab the ideology stats
        Ideology.get_ideology_stats(self)

        ## Grab votes
        Ideology.get_votes_to_predict_ideology(self)

        ## Add probabilities
        Ideology.make_master_ideology(self)

        ## Add partisan stats
        Ideology.find_partisan_bills(self)

        ## Make the tally scores
        Ideology.get_probs(self)
        
    def put_finalized_ideology_stats_into_sql(self):
        connection = open_connection()
        cursor = connection.cursor()

        self.ideology_stats_by_rep_sums = self.ideology_stats_by_rep_sums[['bioguide_id', 'sponsorship_score', 
                                            'total_sponsorship', 'c_prob', 'l_prob', 'ideology_prob', 
                                            'total_votes', 'ideology_type', 's_v_score', 'total_actions', 
                                            'neutral_index', 's_v_score_normalized', 'z_scores', 'tally_score',
                                            'type']]
        
        ## Put data into table
        for i in range(len(self.ideology_stats_by_rep_sums)):
            x = list(self.ideology_stats_by_rep_sums.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO representatives_ideology_stats (
                bioguide_id,
                sponsorship_score,
                total_sponsorship,
                c_prob,
                l_prob,
                ideology_prob,
                total_votes,
                ideology_type,
                s_v_score,
                total_actions,
                neutral_index,
                s_v_score_normalized,
                z_scores,
                tally_score,
                type)
                VALUES ('{bioguide_id}', '{sponsorship_score}', '{total_sponsorship}',
                '{c_prob}', '{l_prob}', '{ideology_prob}', '{total_votes}', '{ideology_type}', 
                '{s_v_score}', '{total_actions}', '{neutral_index}', '{s_v_score_normalized}', 
                '{z_scores}', '{tally_score}', '{type}');"""


                sql_command = format_str.format(bioguide_id=p[0], sponsorship_score=p[1], 
                    total_sponsorship=int(p[2]), c_prob=p[3], l_prob=p[4], ideology_prob=p[5],
                    total_votes=int(p[6]), ideology_type=p[7], s_v_score=p[8], total_actions=int(p[9]), 
                    neutral_index=p[10], s_v_score_normalized=p[11], z_scores=p[12], tally_score=p[13],
                    type=p[14])

                try:
                    # Try to insert, if it can't inset then it should update
                    cursor.execute(sql_command)
                    connection.commit()
                except:
                    connection.rollback()
                    # If the update breaks then something is wrong
                    sql_command = """UPDATE representatives_ideology_stats 
                    SET  
                    sponsorship_score = {},
                    total_sponsorship = {},
                    c_prob = {},
                    l_prob = {},
                    ideology_prob = {},
                    total_votes = {},
                    s_v_score = {},
                    total_actions = {},
                    neutral_index = {},
                    s_v_score_normalized = {},
                    z_scores = {},
                    tally_score = {},
                    type='{}'
                    where (bioguide_id = '{}' AND ideology_type = '{}');""".format(
                    self.ideology_stats_by_rep_sums.loc[i, 'sponsorship_score'],
                    int(self.ideology_stats_by_rep_sums.loc[i, 'total_sponsorship']),
                    self.ideology_stats_by_rep_sums.loc[i, 'c_prob'],
                    self.ideology_stats_by_rep_sums.loc[i, 'l_prob'],
                    self.ideology_stats_by_rep_sums.loc[i, 'ideology_prob'],
                    int(self.ideology_stats_by_rep_sums.loc[i, 'total_votes']),
                    self.ideology_stats_by_rep_sums.loc[i, 's_v_score'],
                    self.ideology_stats_by_rep_sums.loc[i, 'total_actions'],
                    self.ideology_stats_by_rep_sums.loc[i, 'neutral_index'],
                    self.ideology_stats_by_rep_sums.loc[i, 's_v_score_normalized'],
                    self.ideology_stats_by_rep_sums.loc[i, 'z_scores'],
                    self.ideology_stats_by_rep_sums.loc[i, 'tally_score'],
                    self.ideology_stats_by_rep_sums.loc[i, 'type'],
                    self.ideology_stats_by_rep_sums.loc[i, 'bioguide_id'],
                    self.ideology_stats_by_rep_sums.loc[i, 'ideology_type'])

                    cursor.execute(sql_command)
                    connection.commit()

        ## Close yo shit
        connection.close()
    
    def __init__(self, ideology=None, roll_id=None, chamber=None, ideology_df=None, predictive_bills_votes=None,
                master_ideology=None, partisan_bills_only=None, ideology_stats_by_rep_sums=None, bills_df=None,
                ideology_type=None):
        self.ideology = ideology
        self.roll_id = roll_id
        self.chamber = chamber
        self.ideology_df = ideology_df
        self.predictive_bills_votes = predictive_bills_votes
        self.master_ideology = master_ideology
        self.partisan_bills_only = partisan_bills_only
        self.ideology_stats_by_rep_sums = ideology_stats_by_rep_sums
        self.bills_df = bills_df
        self.ideology_type = ideology_type

class Campaign_contributions(object):
    """
    This class will be used to collect, 
    store, and interact with campaign
    finance data. The data will be 
    collected from the FEC and stored
    in our postgres database.
    
    
    Attributes:
    data_set_url - The FEC data we want to collect
    df - The FEC data that was collected
    """
    
    
    def collect_data(self):
        """
        This method will be used to collect 
        finance data and use the method
        to put the collected data to the
        db.
        """
        if ((self.data_set_url == 'ftp://ftp.fec.gov/FEC/2018/cm18.zip') |
            (self.data_set_url == 'ftp://ftp.fec.gov/FEC/2016/cm16.zip')):
            cols = ['CMTE_ID',
                 'CMTE_NM',
                 'TRES_NM',
                 'CMTE_ST1',
                 'CMTE_ST2',
                 'CMTE_CITY',
                 'CMTE_ST',
                 'CMTE_ZIP',
                 'CMTE_DSGN',
                 'CMTE_TP',
                 'CMTE_PTY_AFFILIATION',
                 'CMTE_FILING_FREQ',
                 'ORG_TP',
                 'CONNECTED_ORG_NM',
                 'CAND_ID']
        elif ((self.data_set_url == "ftp://ftp.fec.gov/FEC/2018/cn18.zip") |
              (self.data_set_url == "ftp://ftp.fec.gov/FEC/2016/cn16.zip")):
            cols = ['CAND_ID', 
                    'CAND_NAME', 
                    'CAND_PTY_AFFILIATION', 
                    'CAND_ELECTION_YR',
                    'CAND_OFFICE_ST', 
                    'CAND_OFFICE', 
                    'CAND_OFFICE_DISTRICT', 
                    'CAND_ICI',
                    'CAND_STATUS', 
                    'CAND_PCC', 
                    'CAND_ST1', 
                    'CAND_ST2', 
                    'CAND_CITY',
                    'CAND_ST', 
                    'CAND_ZIP']
        elif ((self.data_set_url == "ftp://ftp.fec.gov/FEC/2018/ccl18.zip") |
              (self.data_set_url == "ftp://ftp.fec.gov/FEC/2016/ccl16.zip")):
            cols = ['CAND_ID',
                    'CAND_ELECTION_YR',
                    'FEC_ELECTION_YR',
                    'CMTE_ID',
                    'CMTE_TP',
                    'CMTE_DSGN',
                    'LINKAGE_ID']
        elif ((self.data_set_url == "ftp://ftp.fec.gov/FEC/2018/oth18.zip") |
              (self.data_set_url == "ftp://ftp.fec.gov/FEC/2016/oth16.zip")):
            cols = ['CMTE_ID',
                    'AMNDT_IND',
                    'RPT_TP',
                    'TRANSACTION_PGI',
                    'IMAGE_NUM',
                    'TRANSACTION_TP',
                    'ENTITY_TP',
                    'NAME',
                    'CITY',
                    'STATE',
                    'ZIP_CODE',
                    'EMPLOYER',
                    'OCCUPATION',
                    'TRANSACTION_DT',
                    'TRANSACTION_AMT',
                    'OTHER_ID',
                    'TRAN_ID',
                    'FILE_NUM',
                    'MEMO_CD',
                    'MEMO_TEXT',
                    'SUB_ID']
        elif ((self.data_set_url == "ftp://ftp.fec.gov/FEC/2018/indiv18.zip")| 
            (self.data_set_url == "ftp://ftp.fec.gov/FEC/2016/indiv16.zip")):
            cols = ['CMTE_ID',
                    'AMNDT_IND',
                    'RPT_TP',
                    'TRANSACTION_PGI',
                    'IMAGE_NUM',
                    'TRANSACTION_TP',
                    'ENTITY_TP',
                    'NAME',
                    'CITY',
                    'STATE',
                    'ZIP_CODE',
                    'EMPLOYER',
                    'OCCUPATION',
                    'TRANSACTION_DT',
                    'TRANSACTION_AMT',
                    'OTHER_ID',
                    'TRAN_ID',
                    'FILE_NUM',
                    'MEMO_CD',
                    'MEMO_TEXT',
                    'SUB_ID']
        

        ## Request zipped data
        r = urllib2.urlopen(self.data_set_url).read()
        file = ZipFile(StringIO(r))

        ## Get all data
        for f_open in file.namelist():
            print "made it"
            unzipped = file.open(f_open)
            print "now got here"
            # self.df = pd.read_csv(unzipped, sep="|", header=None, low_memory=False)
            print "load a lot of data"
            loaded_data = unzipped.readlines()
            print "{} loops to load".format(len(loaded_data))
            for i in range(640000, len(loaded_data)):
                if i % 1000 == 0:
                    print 'Loaded {}'.format(i)
                self.df = pd.DataFrame(data=(loaded_data[i].split("|"))).transpose()
                self.df.loc[0, len(self.df.columns)-1] = self.df.loc[0, len(self.df.columns)-1].replace('\n', '')
                self.df.columns = [cols]
                Campaign_contributions.contributions_to_sql(self)
            
    def contributions_to_sql(self):

        """This method will be used to put finance
        data to the db"""

        connection = open_connection()
        cursor = connection.cursor()

        ## Put data into table
        for i in range(len(self.df)):
            x = list(self.df.loc[i,])
            string_1 = """INSERT INTO {} (""".format(self.db_tbl)
            string_2 = """ VALUES ("""

            for j in range(len(self.df.columns)):
                string_1 += "\n{},".format(self.df.columns[j].lower())
                ## Clean the data
                if (("cand_election_yr" == self.df.columns[j].lower()) |
                    ("fec_election_yr" == self.df.columns[j].lower())):
                    string_2 += "'{}', ".format(int(x[j]))
                elif "transaction_dt"  == self.df.columns[j].lower():
                    try:
                        string_2 += "'{}', ".format(int(x[j]))
                    except:
                        string_2 += "'{}', ".format(0)
                elif "transaction_amt" == self.df.columns[j].lower():
                    string_2 += "'{}', ".format(float(x[j]))
                else:
                    string_2 += "'{}', ".format(sanitize(x[j]).replace('.0', ''))

            string_1 = string_1[:-1] + ")"
            string_2 = string_2[:-2] + ");"
            sql_command = string_1 + string_2

            try:
                # Try to insert, if it can't inset then it should update
                cursor.execute(sql_command)
                connection.commit()
            except:
                connection.rollback()
                ## If the update breaks then something is wrong
                string_1 = """UPDATE {} 
                SET""".format(self.db_tbl)
                string_2 = """ WHERE ("""

                for j in range(len(self.df.columns)):
                    if self.df.columns[j].lower() != self.unique_id:
                        ## Clean the data
                        if (("cand_election_yr" == self.df.columns[j].lower()) |
                            ("fec_election_yr" == self.df.columns[j].lower())):
                            string_1 += "\n{}='{}', ".format(self.df.columns[j].lower(),
                                                             int(x[j]))
                        elif "transaction_dt" == self.df.columns[j].lower():
                            try:
                                string_1 += "\n{}='{}', ".format(self.df.columns[j].lower(),
                                                                 int(x[j]))
                            except:
                                string_1 += "\n{}='{}', ".format(self.df.columns[j].lower(), 0)
                        elif "transaction_amt" == self.df.columns[j].lower():
                            string_1 += "\n{}='{}', ".format(self.df.columns[j].lower(),
                                                             float(x[j]))
                        else:
                            string_1 += "\n{}='{}', ".format(self.df.columns[j].lower(),
                                                             sanitize(x[j]).replace('.0', ''))
                        
                    elif self.df.columns[j].lower() == self.unique_id:
                        string_2 += "{} = '{}'".format(self.df.columns[j].lower(),
                                                       sanitize(x[j]).replace('.0', ''))

                string_1 = string_1[:-2]
                string_2 += ");"

                sql_command = string_1 + string_2
                cursor.execute(sql_command)
                connection.commit()

        ## Close yo shit
        connection.close()

    def __init__(self, data_set_url=None, df=None, db_tbl=None, unique_id=None):
        self.data_set_url = data_set_url
        self.df = df
        self.db_tbl = db_tbl
        self.unique_id = unique_id

class Congressional_report_collector(object):
    def collect_record_link(self, year, month, day, chamber):
        url = "https://www.congress.gov/congressional-record/{}/{}/{}/{}-section".format("{}".format(year).zfill(4),
                                                                     "{}".format(month).zfill(2), 
                                                                     "{}".format(day).zfill(2),
                                                                    chamber)
        print url
        r = requests.get(url)
        if r.status_code == 200:
            page = BeautifulSoup(r.content, 'lxml')
            page_target = page.find('div', class_='tntFormWrapper')
            pdf = []
            for a in page_target.findAll('a'):
                pdf.append("https://www.congress.gov" + a.get('href'))
            if len(pdf) > 0:
                return pdf
            
    def to_sql(self, chamber):
        connection = open_connection()
        cursor = connection.cursor()

        sql_command = """
        INSERT INTO congressional_record_{} (
        date,
        links,
        pdf_str)
        VALUES ('{}', '{}', '{}');""".format(chamber,
            self.df.loc[0, 'date'], 
            sanitize(self.df.loc[0, 'links']), 
            self.df.loc[0, 'pdf_str'])

        try:
            # Try to insert, if it can't inset then it should update
            cursor.execute(sql_command)
            connection.commit()
        except:
            connection.rollback()
            ## If the update breaks then something is wrong
            sql_command = """UPDATE congressional_record_{} 
            SET  
            links='{}', 
            pdf_str='{}'
            where (date = '{}');""".format(
                chamber,
                sanitize(self.df.loc[0, 'links']),
                self.df.loc[0, 'pdf_str'],
                self.df.loc[0, 'date'])

            cursor.execute(sql_command)
            connection.commit()
            
    def pdf_from_url_to_txt(self):
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        ## Open the url provided as an argument to the function and read the content
        r = requests.get(self.url)
        f = r.content
        ## Cast to StringIO object
        fp = StringIO(f)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        password = ""
        maxpages = 0
        caching = True
        pagenos = set()
        for page in PDFPage.get_pages(fp,
                                      pagenos,
                                      maxpages=maxpages,
                                      password=password,
                                      caching=caching,
                                      check_extractable=True):
            interpreter.process_page(page)
        fp.close()
        device.close()
        str = retstr.getvalue()
        retstr.close()
        return str
            
    def collect_and_house(self, year, month, day, chamber):
        ## Collect urls
        x = Congressional_report_collector.collect_record_link(self, year, month, day, chamber)
        ## Save links associated to date
        links_df = pd.DataFrame()
        links_df.loc[0, 'date'] = "{}".format(year).zfill(4) + "-{}-".format(month).zfill(2) + "{}".format(day).zfill(2)
        links_df.loc[0, 'links'] = None
        links_df['links'] = links_df['links'].astype(object)
        try:
            ## Save links
            links_df.set_value(0, 'links', list(x))
            ## Search through all links and save text to var
            pdf_str = ''
            for link in links_df.loc[0, 'links']:
                print link
                self.url = link
                pdf_str += Congressional_report_collector.pdf_from_url_to_txt(self)
            ## Sanitize the text and save
            links_df.loc[0, 'pdf_str'] = sanitize(unidecode(pdf_str.decode('utf-8')).replace('--', ' ').strip())
        except:
            ## If no links found fuck it
            links_df.loc[0, 'links'] = None
            links_df.loc[0, 'pdf_str'] = None
        
        self.df = links_df
    
    @staticmethod
    def date_list(chamber):
        max_date = pd.read_sql_query("""
                    SELECT max(date) FROM congressional_record_{}
                    ;
                    """.format(chamber), open_connection()).loc[0, 'max']

        ## Starting varibles
        year, month, day = max_date.year, max_date.month, max_date.day

        ## Ending variable
        today = str(datetime.datetime.today()).split(' ')[0]

        ## Array of dates to collect
        date_list = []

        ## Collect array of dats
        for i in range(month, 13):
            if i == month:
                ## Do not neet to collect for what we have;
                ## Add one to prevent.
                min_day = day + 1
            else:
                min_day = 1
            max_day = calendar.monthrange(year,i)[1]
            for j in range(min_day, max_day+1):
                search_date = '{}-{}-{}'.format(year, '{}'.format(i).zfill(2), '{}'.format(j).zfill(2))

                ## If it get's to today return the list of dates
                if search_date == today:
                    return date_list

                ## Convert to date time when saving to array
                date_list.append(pd.to_datetime(search_date))
                
    def collect_missing_reports(self, chamber):
        date_array = Congressional_report_collector.date_list(chamber)
        for date in date_array:
            print date.year
            print date.month
            print date.day
            Congressional_report_collector.collect_and_house(self, date.year, date.month, date.day, chamber)
            Congressional_report_collector.to_sql(self, chamber)
        print 'done!'

                
    ########################################
    #### The following methods are used ####
    #### to find which reps spoke (with ####
    #### bioguide id link) what they and ###
    #### spoke about.                   ####
    ########################################
    
    def get_sub_clean_text(self):
        """
        Input:
        Block of text form a section of the 
        congressional report. The required
        block of text is the text found between
        sections.

        Outputs:
        None

        Attributes:
        Section Title - The extracted title from
        the block of text
        Body Text - Original input text stripped
        of the section title and other special
        stop words.
        """

        mystr = ' '.join(self.text.replace('-\n\n','').replace('-\n','').replace("''", '"').split())
        mystr = mystr.replace("\'", "'")
        mystr = mystr.replace(" E T A N E S h t i w D O R P 1 N V T P S 4 K S D n o CONGRESSIONAL RECORD SENATE ", "")
        mystr = mystr.replace(" E T A N E S h t i w D O R P 1 N V T P S 4 K S D n o CONGRESSIONAL RECORD HOUSE ", "")

        mystr = mystr.replace("E S U O H h t i w D O R P 1 N V T P T 7 K S D n o k c i r e d e r f r", "")
        mystr = mystr.replace("E S U O H h t i w D O R P 1 N V T P T 7 K S D n o", "")
        mystr = mystr.replace("k c i r e d e r f r", "")


        mystr = mystr.replace("E T A N E S h t i w D O R P 1 N V T P V 5 K S D n o r e t t o l", "")
        mystr = mystr.replace("E T A N E S h t i w D O R P 1 N V T P V 5 K S D n o", "")
        mystr = mystr.replace("r e t t o l", "")

        mystr = mystr.replace("E T A N E S h t i [?]", "")
        mystr = mystr.replace("w D O R P 1 N V T P V 5 K S D n o", "")
        mystr = mystr.replace("[?]", "")

        mystr = re.sub("b \d+ AFTER RECESS", "AFTER RECESS", mystr)
        wordList = re.sub(" ", " ",  mystr).split()
        count = 0
        add_more = True
        section_title = ''

        try:
            ## Sometimes the secion is blank. Just how it comes back
            while add_more == True:
                if ((wordList[count].isupper() == True) | (wordList[count].isdigit() == True)):
                    section_title += wordList[count]
                    count +=1 
                elif (wordList[count][-1] in string.punctuation) & ((wordList[count][:-1].isupper() == True) | (wordList[count][:-1].isdigit() == True)):
                    section_title += wordList[count]
                    count +=1 
                else:
                    add_more = False
                section_title += ' '

            self.section_title = section_title.strip(' ')
            mystr = mystr.replace(section_title.strip(' ') + ' ', "")
            mystr = mystr.replace("GENERAL LEAVE ", "")
            self.section = mystr
        except IndexError:
            "no words"
        
    def whatd_they_say(self, chamber):
        if len(self.section) > 0:
            ## Sometimes the section is blank. It's a bug from the pdf conversion 
            ## (or at least I think it is).

            # Import peeps
            all_reps = pd.read_sql_query("""
            SELECT * FROM congress_bio
            WHERE chamber = '{}'
            AND served_until = 'Present'
            ;
            """.format(chamber), open_connection())
            all_reps.loc[:, 'last_name'] = all_reps['name'].apply(lambda x: x.split(', ')[0])


            ## Set up vars
            x = tokenize.sent_tokenize(self.section)
            titles = ['Mr.',
            'Mrs.',
            'Miss',
            'Ms.',
            'Madam',
            'SPEAKER',
            'ACTING PRESIDENT',
            'PRESIDENT',
            'pro tempore']
            speakers = []
            b_ids = []


            ## Find when peopl are talking and tag their name and b_ids
            for i in range(len(x)):
                sentence = ""
                sentence_2 = ""
                appeard = 0
                peeps = []
                reg_search = re.search(r'Mr|Mrs|Miss|Ms|Madam|SPEAKER|ACTING PRESIDENT|PRESIDENT|pro tempore', x[i])
                try:
                    for title in titles:
                        if title in reg_search.string:
                            sentence += '{} '.format(title)
                    for j in range(len(all_reps)):
                        if all_reps.loc[j, 'last_name'].upper().replace('MCC', 'McC') in reg_search.string:
                            appeard += 1
                            peeps.append(j)
                    if appeard > 1:
                        for _index in peeps:
                            if all_reps.loc[_index, 'state'] in reg_search.string:
                                sentence += '{} '.format(all_reps.loc[_index, 'last_name'].upper().replace('MCC', 'McC'))
                                sentence += 'of {}.'.format(all_reps.loc[_index, 'state'])
                                b_id = all_reps.loc[_index, 'bioguide_id']
                    elif appeard == 1:
                        sentence += '{}.'.format(all_reps.loc[peeps[0], 'last_name'].upper().replace('MCC', 'McC'))
                        sentence_2 = sentence[:-1] + ' of {}.'.format(all_reps.loc[peeps[0], 'state'])
                        b_id = all_reps.loc[peeps[0], 'bioguide_id']
                    sentence = sentence.replace("PRESIDENT PRESIDENT", "PRESIDENT")
                    if sentence == reg_search.string:
                        speakers.append(sentence)
                        b_ids.append(b_id)
                    elif sentence_2 == reg_search.string:
                        speakers.append(sentence_2)
                        b_ids.append(b_id)
                    elif ("The {}.".format(sentence.strip(' '))) == reg_search.string:
                        speakers.append("The {}.".format(sentence.strip(' ')))
                        b_ids.append("speaker")
                except:
                    'nada'

            ## Make df of speakers
            speaking_df = pd.DataFrame(data=[speakers, b_ids]).transpose().drop_duplicates().reset_index(drop=True)
            speaking_df.columns = ['speaker', 'b_id']

            ## Make df of sentences
            sentences_df = pd.DataFrame(x, columns=['speaker'])
            sentences_df.loc[:, 'speaker_trigger'] = False

            ## Tag where the speakers began speaking
            ## column for sentence is speaker bc it will be that at the end
            for i in range(len(speaking_df)):
                speaker = speaking_df.loc[i, 'speaker']
                b_id = speaking_df.loc[i, 'b_id']

                sentences_df.loc[sentences_df['speaker'] == speaker, 'speaker_trigger'] = True
                sentences_df.loc[sentences_df['speaker'] == speaker, 'bioguide_id'] = b_id

            ## Find all of the text between speakers and attribute to speaker
            indexes = sentences_df.loc[sentences_df['speaker_trigger'] == True].index

            for i in range(len(indexes)):
                try:
                    body = ''
                    for j in range(indexes[i]+1, indexes[i+1]):
                        body += sentences_df.loc[j, 'speaker'] + ' '
                    sentences_df.loc[indexes[i], 'speaker_text'] = body.strip(' ')
                except:
                    for j in range(indexes[i]+1, len(sentences_df)):
                        body += sentences_df.loc[j, 'speaker'] + ' '
                    sentences_df.loc[indexes[i], 'speaker_text'] = body.strip(' ')

            ## Add what its about
            sentences_df.loc[:, 'subject'] = self.section_title

            ## Return clean df
            return sentences_df.loc[sentences_df['speaker_trigger'] == True].reset_index(drop=True)
    
    def transcript_to_sql(self):
        connection = open_connection()
        cursor = connection.cursor()
        
        self.df.loc[:, 'id'] = (self.df['date'].apply(lambda x: str(x).replace('-','')) + 
                                self.df['chamber'] + self.df.index.astype(str))

        for i in range(len(self.df)):
            sql_command = """
            INSERT INTO congressional_record_transcripts (
            speaker, 
            bioguide_id, 
            speaker_text, 
            subject,
            date,
            chamber,
            id)
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}');""".format(
                sanitize(self.df.loc[i, 'speaker']), 
                self.df.loc[i, 'bioguide_id'], 
                sanitize(self.df.loc[i, 'speaker_text']),
                sanitize(self.df.loc[i, 'subject']), 
                self.df.loc[i, 'date'], 
                self.df.loc[i, 'chamber'],
                self.df.loc[i, 'id'])

            try:
                # Try to insert, if it can't inset then it should update
                cursor.execute(sql_command)
                connection.commit()
            except:
                ## I don't want to update anything if it already exists
                connection.rollback()
                print 'not work: {}'.format(i)
        connection.close()

    def clean_transcripts(self, chamber):
        transcript_max = pd.read_sql_query("""
        SELECT max(date) FROM congressional_record_transcripts
        WHERE chamber = '{}';
        """.format(chamber), open_connection())
        
        df = pd.read_sql_query("""
        SELECT * FROM congressional_record_{}
        WHERE date > '{}'
        ;""".format(chamber, transcript_max.loc[0, 'max']), open_connection())
        
        for i in df.index:
            if df.loc[i, 'pdf_str'] != 'nan':
                print 'has: {}'.format(i)
                by_section = df.loc[i, 'pdf_str'][28:].split(' \n\nf ')
                master_df = pd.DataFrame()

                for section in by_section:
                    self.text = section.replace('- \n', '-')
                    Congressional_report_collector.get_sub_clean_text(self)
                    clean_df = Congressional_report_collector.whatd_they_say(self, chamber)
                    if len(clean_df) > 0:
                        master_df = master_df.append(clean_df).reset_index(drop=True)

                if len(master_df) > 0:
                    ## Add column for date and drop speaker trigger
                    master_df.loc[:, 'date'] = df.loc[i, 'date']
                    master_df = master_df.drop(['speaker_trigger'], 1)
                    master_df.loc[:, 'chamber'] = chamber

                    self.df = master_df
                    Congressional_report_collector.transcript_to_sql(self)
            else:
                'not: {}'.format(i)
                
            
    def __init__(self):
        self.df = pd.DataFrame()
        self.text = None
        self.section_title = None
        self.section = None

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

class Ip_Spoofer(object):
    """
    This will be used to find a random IP and Port to proxy to.
    The need for this is because congress Blacklists a website when
    it looks like a bot, and doesn't allow blacklisted IPs to access 
    their website. So to get aroudn this I am using a proxy IP address.
    
    """    
    
    def free_proxy_list_net(self):

        s = requests.session()
        url = "https://free-proxy-list.net/"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        s.headers.update(headers)
        r = s.get(url)
        if r.status_code != 200: 
            return False


        page = BeautifulSoup(r.content, 'lxml')
        table = page.find('table', id='proxylisttable')

        cols = []
        for t in table.find('thead').findAll('th'):
            cols.append(t.text)

        proxy_df = pd.DataFrame(data=[], columns=cols)
        body = table.find('tbody')
        tr_list = body.findAll('tr')

        for tr_num in range(len(tr_list)):
            length = len(proxy_df)
            row = tr_list[tr_num].findAll('td')

            for i in range(len(row)):
                proxy_df.loc[length, cols[i]] = row[i].text

        self.proxy_df = proxy_df.head(50)

    def hide_my_name(self):

        s = requests.session()
        url = "https://hidemy.name/en/proxy-list/"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        s.headers.update(headers)
        r = s.get(url)
        if r.status_code != 200: 
            return False


        page = BeautifulSoup(r.content, 'lxml')
        table = page.find('table', class_='proxy__t')

        cols = []
        for t in table.find('thead').findAll('th'):
            cols.append(t.text)

        proxy_df = pd.DataFrame(data=[], columns=cols)
        body = table.find('tbody')
        tr_list = body.findAll('tr')

        for tr_num in range(len(tr_list)):
            length = len(proxy_df)
            row = tr_list[tr_num].findAll('td')

            for i in range(len(row)):
                proxy_df.loc[length, cols[i]] = row[i].text

        self.proxy_df = proxy_df.head(50)
        
    @staticmethod
    def check_ip(ip, port):
        s = requests.session()
        proxies = {
          'http': '{}:{}'.format(ip, port),
        }
        s.proxies.update(proxies)    
        a = requests.adapters.HTTPAdapter(max_retries=5)
        s.mount('http://', a)
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        s.headers.update(headers)
        try:
            r = s.get('http://ip.42.pl/raw')
            if r.status_code == 200:
                print r.content
                print 'given: {}'.format(ip)
                return True
            else:
                return False
        except:
            return False
        
    @staticmethod
    def random_ip():
        spoof = Ip_Spoofer()
        num_list = [Ip_Spoofer.free_proxy_list_net, Ip_Spoofer.hide_my_name]
        rand_num = np.random.choice(len(num_list))
        num_list[rand_num](spoof)

        indexes = list(spoof.proxy_df.index)    
        while len(indexes) > 0:
            rand_num = np.random.choice(indexes)

            x = pd.DataFrame([spoof.proxy_df.loc[rand_num, spoof.proxy_df.columns[:2]]]).reset_index(drop=True)
            good_ip = Ip_Spoofer.check_ip(str(x.loc[0, x.columns[0]]), str(x.loc[0, x.columns[1]]))
            if good_ip == True:
                indexes = list(set(indexes) - set([rand_num]))
                return x
        return "No working IP"
    
    def __init__(self):
        self.proxy_df = pd.DataFrame()