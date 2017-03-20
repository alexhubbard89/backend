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
import datetime
import re
import us
from unidecode import unidecode
## algo to summarize
from gensim.summarization import summarize
import ast
from scipy import stats

urlparse.uses_netloc.append("postgres")
url = urlparse.urlparse(os.environ["HEROKU_POSTGRESQL_BROWN_URL"])
    
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
            'state_short', 'state_long', 'district']])
        

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

        df.loc[0, 'district'] = user_info.get_district_from_address(self)

        return df

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
            district)
            VALUES ('{email}', '{password}', '{street}', '{zip_code}', '{city}', '{state_short}',
                    '{state_long}', '{first_name}', '{last_name}', 
                    '{gender}', '{dob}', '{district}');"""


        sql_command = format_str.format(email=self.user_df.loc[0, 'email'], 
            password=self.user_df.loc[0, 'password'], street=self.user_df.loc[0, 'street'], 
            zip_code=int(self.user_df.loc[0, 'zip_code']), city=self.user_df.loc[0, 'city'], 
            state_short=self.user_df.loc[0, 'state_short'], 
            state_long=self.user_df.loc[0, 'state_long'],  
            first_name=self.user_df.loc[0, 'first_name'], 
            last_name=self.user_df.loc[0, 'last_name'], 
            gender=self.user_df.loc[0, 'gender'], 
            dob=self.user_df.loc[0, 'dob'], 
            district=int(self.user_df.loc[0, 'district']))


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
        url = 'http://ziplook.house.gov/htbin/findrep?ADDRLK'
        form_data = {
            'street': self.street,
            'city': self.city,
            'state': state,
            'submit': 'FIND YOUR REP',
        }

        response = requests.request(method='POST', url=url, data=form_data, headers=headers)
        district = str(response.content.split('src="/zip/pictures/{}'.format(self.state_short.lower()))[1].split('_')[0])
        return int(district)
    
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
        if self.password_match == True:
            connection = open_connection()
            sql_command = """
            select * from  user_tbl
            where email = '{}'""".format(self.email)

            user_results = pd.read_sql_query(sql_command, connection)
            connection.close()
            return user_results[['user_id', 'city', 'state_short', 'state_long', 'first_name', 'last_name', 'district']]
        elif self.password_match == False:
            return "Check credentials frist"

    def get_congress_bio(self):
        ## Search for user's reps
        sql_command = """select * 
        from congress_bio 
        where state = '{}' 
        and served_until = 'Present'
        and ((chamber = 'senate') 
        or (chamber = 'house' and district = {}));""".format(self.state_long, self.district)

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
    
    def __init__(self, email=None, password=None, password_match=False, first_name=None,
                last_name=None, gender=None, dob=None, street=None, zip_code=None, user_df=None,
                state_long=None, district=None, bioguide_id_to_search=None, chamber=None,
                address_check=None, return_rep_list=None, city=None, state_short=None):
        self.email = email
        self.password = password
        self.password_match = password_match
        self.first_name = first_name
        self.last_name = last_name
        self.gender = gender
        self.dob = dob
        self.street = street
        self.zip_code = zip_code
        self.user_df = user_df
        self.state_long = state_long
        self.district = district
        self.bioguide_id_to_search = bioguide_id_to_search
        self.chamber = chamber
        self.address_check = address_check
        self.return_rep_list = return_rep_list
        self.city = city
        self.state_short = state_short


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
    
    
    def __init__(self, committee_links=None, subcommittee_links=None, all_committee_links=None, committee_membership=None):
        self.committee_links = committee_links
        self.subcommittee_links = subcommittee_links
        self.committee_membership = committee_membership

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
        
        ## Create url path
        r = requests.get('{}/cosponsors'.format(self.search_url))
        page = BeautifulSoup(r.content, "lxml")
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
        # except:
        #     """There was no sponsor. 
        #     Rare but not impossible."""
        #     sponsor = None
        #     sponsors_df = pd.DataFrame([self.search_url, sponsor]).transpose()
        #     sponsors_df.columns = ['url', 'bioguide_id']
        #     sponsors_df.loc[0, 'cosponsor_bioguide_id'] = None
        #     sponsors_df.loc[0, 'cosponsor_member_full'] = None
        #     sponsors_df.loc[0, 'date_cosponsored'] = None

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

        url = 'https://www.congress.gov/search?q=%7B"congress":"{}","source":"legislation"%7D&searchResultViewType=expanded&pageSize=250&page=1'.format(self.congress_search)
        print url
        r = requests.get(url)
        page = BeautifulSoup(r.content, 'lxml')

        max_page = int(page.find('div', 
              class_='nav-pag-top').find(
        'div', class_='pagination').find_all(
        'a')[-1].get('href').split('page=')[1])

        for i in range(1, max_page+1):
            page_df = pd.DataFrame()
            if i != 1:
                ## Request next page
                url = 'https://www.congress.gov/search?q=%7B"congress":"{}","source":"legislation"%7D&searchResultViewType=expanded&pageSize=250&page={}'.format(self.congress_search, i) 
                print url
                r = requests.get(url)
                page = BeautifulSoup(r.content, 'lxml')

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
                        'span', class_='result-item')[3].find(
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

class user_votes(object):
    """
    This class will be used to find
    legislation that a user can vote on
    and insert user votes to the db.
    
    Attributes:
    user_id - user_id number
    leg_for_user - what the user will vote on
    roll_id - what user voted on
    vote - how they voted
    insert - if insert was successful
    """
    
    def available_votes(self):
        """
        This method will be used to find
        the legislation that a user can vote on.
        It will only look for on passge bills
        that the user has not voted on before.

        Input:
        user_id - To find what a user has voted on

        REMEMBER THIS!
        CONGRESS IS HARD CODED FOR NOW. 
        REMEMBER TO FIX THIS IN THE FUTURE.
        """

        prev_voted = pd.read_sql_query("""SELECT * 
        FROM user_votes 
        where user_id = {}""".format(self.user_id), open_connection())


        """Build string to exclude roll_ids
        that the user has already voted on."""
        roll_id = ''
        for i in range(len(prev_voted)):
            if i > 0:
                roll_id += ' and roll_id != {}'.format(prev_voted.loc[i, 'roll_id'])
            if i == 0:
                roll_id += 'roll_id != {}'.format(prev_voted.loc[i, 'roll_id'])

        
        """Find anything for a user to vote on."""
        leg_for_user = pd.read_sql_query("""SELECT * FROM house_vote_menu 
            where congress = 115 
            and lower(question) ilike '%' || 'passage' || '%'
            and ({});""".format(roll_id), open_connection())
        
        """Find anything for a user to vote on."""
        predictive_leg = pd.read_sql_query("""SELECT * FROM predictive_legislation
            where predict_user_ideology = True 
            and ({});""".format(roll_id), open_connection())
        
        ## Append data sets together
        leg_for_user = leg_for_user.append(predictive_leg).reset_index(drop=True)
        
        ## Randomly select vote
        search_index = np.random.randint(len(leg_for_user))
        leg_for_user = pd.DataFrame(leg_for_user.loc[search_index]).transpose().reset_index(drop=True)

        ## Fix Date column
        leg_for_user['date'] = leg_for_user['date'].astype(str)

        ## Remove columns that will have nulls from predictive table.
        ## Eventually add main subject. But not for now
        print leg_for_user
        self.leg_for_user = leg_for_user.drop(['ideology_to_predict',
                                              'predict_user_ideology'], 1)

    def summarize_bill(self):
        url = self.leg_for_user.loc[0, 'issue_link']
        r = requests.get(url)
        page = BeautifulSoup(r.content, 'lxml')
        text = ''
        try:
            """If there is not bill-summary div then 
            there is no summary."""
            paragraph = page.find('div', id='bill-summary').findAll('p')

            for i in range(len(paragraph)):
                text += (str(unidecode(paragraph[i].text.strip())))

            """Gensim breaks if less than 3 sentances. When scraping
            the senetences lose period space. Add space to have more
            sentences."""
            text = text.replace('.', '. ').replace('.  ', '. ').replace('U. S. ', 'U.S. ').replace('H. R. ', 'H.R.')

            try:
                text_sum = summarize(text)
                """If no summary was made or it's really long
                then sumarize with a 50 word count"""
                if (len(text_sum) > 100):
                    text_sum = summarize(text, word_count=50)
                if (len(text_sum) == 0):
                    text_sum = summarize(text, word_count=100)
            except:
                print 'no summary'
                text_sum = ''


            if len(text_sum) > 0:
                return text_sum.strip().replace('\n', '').replace('\t', '').replace('\"', '"' )
            elif len(text) > 0:
                return text.strip().replace('\n', '').replace('\t', '').replace('\"', '"' )
            else:
                return 'No summary available'
        except:
            return 'No summary available'

        
    def vote_to_db(self):
        """
        This method is used to insert the user's vote
        on the roll_id to the user_votes table.        
        """
        
        connection = open_connection()
        cursor = connection.cursor()
        
        sql_command = """
        insert into user_votes (
        user_id,
        roll_id,
        vote) 
        VALUES ({}, {}, {});""".format(
        self.user_id,
        self.roll_id,
        self.vote)
        
        try:
            cursor.execute(sql_command)
            connection.commit()
            self.insert = True
        except:
            connection.rollback()
            self.insert = False
        connection.close()
    
    def __init__(self, user_id=None, leg_for_user=None,
                roll_id=None, vote=None, insert=None):
        self.user_id = user_id
        self.leg_for_user = leg_for_user
        self.roll_id = roll_id
        self.vote = vote
        self.insert = insert

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
        
        vote_dates = pd.read_sql_query("""
        SELECT COUNT(DISTINCT(date)) as total_work_days 
        FROM house_votes_tbl 
        WHERE congress = {};
        """.format(self.congress_num),open_connection())
        
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
        
        all_sponsored = all_sponsored.sort_values(['rep_sponsor', 'bioguide_id'], 
                                  ascending=[False, True]).reset_index(drop=True)

        all_sponsored['max_sponsor'] = all_sponsored['rep_sponsor'].max()
        all_sponsored['sponsor_percent'] = (all_sponsored['rep_sponsor']/all_sponsored['max_sponsor'])

        all_sponsored = all_sponsored.loc[all_sponsored['bioguide_id'] == self.bioguide_id].reset_index(drop=True)
        
        self.rep_sponsor_metrics = all_sponsored

    def membership_stats(self):
        if self.chamber.lower() == 'house':
            tbl = 'house_membership'
        elif self.chamber.lower() == 'senate':
            tbl = 'senate_membership'
            
        df = pd.read_sql_query("""
        SELECT * FROM {}""".format(tbl), open_connection())
        
        df = df.groupby(['bioguide_id']).count()['committee'].reset_index(drop=False)
        df.columns = ['bioguide_id', 'num_committees']
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

        self.policy_area_df = policy_area_df[['policy_area', 'percent']]

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
            AND served_until = 'Present';""", open_connection())

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
            AND served_until = 'Present';""", open_connection())

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
            WHERE served_until = 'Present';""", open_connection())

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

    
    def __init__(self, congress_num=None, bioguide_id=None, days_voted=None,
                rep_votes_metrics=None, rep_sponsor_metrics=None,
                chamber=None, membership_stats_df=None, policy_area_df=None,
                search_term=None):
        self.congress_num = congress_num
        self.bioguide_id = bioguide_id
        self.days_voted = days_voted
        self.rep_votes_metrics = rep_votes_metrics
        self.rep_sponsor_metrics = rep_sponsor_metrics
        self.chamber = chamber
        self.membership_stats_df = membership_stats_df
        self.policy_area_df = policy_area_df

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
        
    # def get_votes_to_predict_ideology(self):
    #     predictive_legislation_sql = pd.read_sql_query("""
    #         SELECT * 
    #         FROM predictive_legislation 
    #         WHERE ideology_to_predict = '{}'""".format(self.ideology.lower()),
    #                                                        open_connection())
    #     ## Build query to find bills that are predictive    
    #     search_query = ''
    #     for i in range(0, len(predictive_legislation_sql)):
    #         if i > 0:
    #             search_query += " or (roll_id = {})".format(
    #                 predictive_legislation_sql.loc[i, 'roll_id'])
    #         elif i == 0:
    #             search_query += "(roll_id = {})".format(
    #                 predictive_legislation_sql.loc[i, 'roll_id'])

    #     ## Query db for predictive bills
    #     self.predictive_bills_votes = pd.read_sql_query("""SELECT *, cast(roll as int) as roll_int 
    #     FROM house_votes_tbl 
    #     WHERE ({})""".format(search_query), open_connection())

    def get_votes_to_predict_ideology(self):
        predictive_legislation_sql = pd.read_sql_query("""
            SELECT * 
            FROM predictive_legislation_truncated 
            WHERE ideology_to_predict = '{}'""".format(self.ideology.lower()),
                                                           open_connection())
        
        predictive_legislation_sql_house = predictive_legislation_sql.loc[predictive_legislation_sql['chamber'] == 'house']
        predictive_legislation_sql_senate = predictive_legislation_sql.loc[predictive_legislation_sql['chamber'] == 'senate']
        master_df = pd.DataFrame()
        ## Build query to find bills that are predictive   
        if len(predictive_legislation_sql_house):
            search_query = ''
            for i in range(0, len(predictive_legislation_sql_house)):
                if i > 0:
                    search_query += " or (roll_id = {})".format(
                        predictive_legislation_sql_house.loc[i, 'roll_id'])
                elif i == 0:
                    search_query += "(roll_id = {})".format(
                        predictive_legislation_sql_house.loc[i, 'roll_id'])

            ## Query db for predictive bills
            predictive_bills_votes_house = pd.read_sql_query("""SELECT *, cast(roll as int) as roll_int 
            FROM house_votes_tbl 
            WHERE ({})""".format(search_query), open_connection())

            predictive_bills_votes_house.loc[:, 'chamber'] = 'house'
            
            self.predictive_bills_votes = master_df.append(predictive_bills_votes_house)
        
        ## Build query to find bills that are predictive    
        if len(predictive_legislation_sql_senate):
            search_query = ''
            for i in range(0, len(predictive_legislation_sql_senate)):
                if i > 0:
                    search_query += " or (roll_id = {})".format(
                        predictive_legislation_sql_senate.loc[i, 'roll_id'])
                elif i == 0:
                    search_query += "(roll_id = {})".format(
                        predictive_legislation_sql_senate.loc[i, 'roll_id'])

            ## Query db for predictive bills
            predictive_bills_votes_senate = pd.read_sql_query("""SELECT *, cast(roll as int) as roll_int 
            FROM senator_votes_tbl 
            WHERE ({})""".format(search_query), open_connection())

            predictive_bills_votes_senate.loc[:, 'chamber'] = 'senate'
        
            master_df = master_df.append(predictive_bills_votes_senate)
        return master_df
        
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


        """
        In this section I'm going to normalize the mean to zero,
        where the mean is the most neutral ideology_probabiity.
        The most neutral is defined as the rep's (c_pro - l_prob) 
        divided by total votes that's closest to zero. 

        I'm then scaling the c_prob and l_prob by the number of
        times the rep has voted. Someone who has voted more we can
        assume their ideology better. 

        Finally, I'm creating an ideolgy prob from the scaled probabilities
        and normalizing them with a mean of 0, and putting them on a scale
        of -3 to 3, liberal to conservative respectively.
    

        """


        ideology_stats_by_rep_sums.loc[:, 'most_neutral'] = abs(ideology_stats_by_rep_sums['ideology_prob']/
            ideology_stats_by_rep_sums['total_votes'])
        ideology_stats_by_rep_sums.loc[:, 'c_prob_x'] = (ideology_stats_by_rep_sums.loc[:, 'c_prob'] * 
            ideology_stats_by_rep_sums.loc[:, 'total_votes'])
        ideology_stats_by_rep_sums.loc[:, 'l_prob_x'] = (ideology_stats_by_rep_sums.loc[:, 'l_prob'] * 
            ideology_stats_by_rep_sums.loc[:, 'total_votes'])
        ideology_stats_by_rep_sums.loc[:, 'ideology_prob_x'] = (ideology_stats_by_rep_sums.loc[:, 'c_prob_x'] - 
            ideology_stats_by_rep_sums.loc[:, 'l_prob_x'])


        ## Assign z-scores
        ## call it "ideology_prob_x_zero_mean"
        mew = float(ideology_stats_by_rep_sums.loc[
            ideology_stats_by_rep_sums['most_neutral'] == 
            ideology_stats_by_rep_sums['most_neutral'].min(), 'ideology_prob_x'].reset_index(drop=True)[0])
        standard_d = np.std(ideology_stats_by_rep_sums['ideology_prob_x'])
        ideology_stats_by_rep_sums.loc[:, 'ideology_prob_x_zero_mean'] = ideology_stats_by_rep_sums.loc[:, 'ideology_prob_x'].apply(lambda x: (x - mew)/standard_d)

        ## Stretch z-scores to be between -3 and 3
        f_max = ideology_stats_by_rep_sums['ideology_prob_x_zero_mean'].max()
        f_min = ideology_stats_by_rep_sums['ideology_prob_x_zero_mean'].min()
        f_bar = ((f_max + f_min)/2)
        A = (2/(f_max - f_min))
        ideology_stats_by_rep_sums.loc[:, 'tally_score'] = ideology_stats_by_rep_sums.loc[:, 'ideology_prob_x_zero_mean'].apply(lambda x: round(A*(x - f_bar), 4) * 3)


        ideology_stats_by_rep_sums['ideology_type'] = self.ideology
        self.ideology_stats_by_rep_sums = ideology_stats_by_rep_sums
        
    
    def update_predictive_legislation(self):
        
        if self.ideology.lower() == 'women and minority rights':
            df = pd.read_sql_query("""SELECT * FROM all_legislation
            WHERE lower(policy_area) ilike '%' || 'minority issues' || '%'
            OR lower(policy_area) ilike '%' || 'disabled' || '%'
            OR lower(policy_area) ilike '%' || 'women' || '%';""", open_connection())
        elif self.ideology.lower() == 'immigration':
            df = pd.read_sql_query("""SELECT * FROM all_legislation
                WHERE lower(policy_area) ilike '%' || 'immigration' || '%';""", open_connection())
        elif self.ideology.lower() == 'abortion':
            df = pd.read_sql_query("""SELECT * FROM all_legislation
                WHERE lower(policy_area) ilike '%' || 'abortion' || '%'
                OR (lower(title_description) ilike '%' || 'abortion' || '%'
                AND lower(policy_area) ilike '%' || 'health' || '%')
                OR (lower(title_description) ilike '%' || 'abortion' || '%') 
                OR (lower(title_description) ilike '%' || 'born-alive' || '%')
                OR (lower(title_description) ilike '%' || 'unborn child' || '%')
                OR (lower(title_description) ilike '%' || ' reproductive' || '%')
                OR (lower(title_description) ilike '%' || 'planned parenthood' || '%');""", open_connection())
        elif self.ideology.lower() == 'environmental protection':
            df = pd.read_sql_query("""SELECT * FROM all_legislation
                WHERE lower(policy_area) ilike '%' || 'environmental protection' || '%';""", open_connection())
        elif self.ideology.lower() == 'second amendment':
            df = pd.read_sql_query("""SELECT * FROM all_legislation
                        WHERE lower(title_description) ilike '%' || 'second amendment' || '%'
                        OR lower(title_description) ilike '%' || 'gun' || '%'
                        OR lower(title_description) ilike '%' || 'firearm' || '%';""", open_connection())
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
        for url in df['issue_link']:
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
        for i in range(len(df)):
            issue = re.split('(\d+)', df.loc[i, 'issue'])
            issue_split = ''
            for word in issue:
                if len(word) > 0:
                    issue_split += word
                    issue_split += ' '
            df.loc[i, 'issue'] = (issue_split).strip().lower()

        ## Find bills from Senate vote menu
        sql_query = 'SELECT * FROM senate_vote_menu'
        for i in range(len(df)):
            if i != 0:
                sql_query += " OR (lower(issue) = '{}' AND congress = {})".format(df.loc[i, 'issue'],
                                                                df.loc[i, 'congress'])
            elif i == 0:
                sql_query += " WHERE (lower(issue) = '{}' AND congress = {})".format(df.loc[i, 'issue'],
                                                                df.loc[i, 'congress'])

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
        predictive_bills_votes = predictive_bills_votes_senate.append(predictive_bills_votes_house).reset_index(drop=True)
        self.predictive_bills_votes = predictive_bills_votes

        ## Grab the ideology stats
        Ideology.get_ideology_stats(self)

        ## Make ideology from roll call votes
        Ideology.make_master_ideology(self)

        ## Find partisan bills from roll call votes
        Ideology.find_partisan_bills(self)

        """
        The partisan bills are the predictive bills.
        Join the partisan/predictive bills with the original
        vote menu collected.
        """

        df = self.partisan_bills_only[['roll_id', 'chamber']]#.drop_duplicates().reset_index(drop=True)
        df.loc[:, 'roll_id'] = df.loc[:, 'roll_id'].astype(int)
        df.loc[:, 'ideology_to_predict'] = self.ideology 
        
        """
        Now that the data is clean I can 
        put it into sql
        """
        connection = open_connection()
        cursor = connection.cursor()

        ## Put data into table
        for i in range(len(df)):
            x = list(df.loc[i,])
            for p in [x]:
                format_str = """
                INSERT INTO predictive_legislation_truncated (
                roll_id,
                chamber,
                ideology_to_predict)
                VALUES ('{roll_id}', '{chamber}', '{ideology_to_predict}');"""


                sql_command = format_str.format(roll_id=p[0], chamber=p[1], ideology_to_predict=p[2])
            ## Commit to sql
            try:
                cursor.execute(sql_command)
                connection.commit()
            except:
                ## Update what I got
                connection.rollback()
        connection.close()
        
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

        ## Put data into table
        for i in range(len(self.ideology_stats_by_rep_sums)):
            x = list(self.ideology_stats_by_rep_sums.loc[i,])

            for p in [x]:
                format_str = """
                INSERT INTO representatives_ideology_stats (
                bioguide_id, 
                c_prob, 
                l_prob, 
                ideology_prob,
                total_votes,
                most_neutral,
                c_prob_x,
                l_prob_x,
                ideology_prob_x,
                ideology_prob_x_zero_mean,
                tally_score, 
                ideology_type)
                VALUES ('{bioguide_id}', '{c_prob}', '{l_prob}', '{ideology_prob}',
                 '{total_votes}', '{most_neutral}', '{c_prob_x}', '{l_prob_x}', 
                 '{ideology_prob_x}', '{ideology_prob_x_zero_mean}',
                  '{tally_score}', '{ideology_type}');"""


                sql_command = format_str.format(bioguide_id=p[0], c_prob=p[1], 
                    l_prob=p[2], ideology_prob=p[3], total_votes=p[4], most_neutral=p[5],
                    c_prob_x=p[6], l_prob_x =p[7], ideology_prob_x=p[8],
                    ideology_prob_x_zero_mean=p[9], tally_score=p[10], ideology_type=p[11])

                try:
                    # Try to insert, if it can't inset then it should update
                    cursor.execute(sql_command)
                    connection.commit()
                except:
                    connection.rollback()
                    ## If the update breaks then something is wrong
                    sql_command = """UPDATE representatives_ideology_stats 
                    SET  
                    c_prob = {},
                    l_prob = {},
                    ideology_prob = {},
                    total_votes = {},
                    most_neutral = {},
                    c_prob_x = {},
                    l_prob_x = {},
                    ideology_prob_x = {},
                    ideology_prob_x_zero_mean = {},
                    tally_score = {}
                    where (bioguide_id = '{}' AND ideology_type = '{}');""".format(
                    self.ideology_stats_by_rep_sums.loc[i, 'c_prob'],
                    self.ideology_stats_by_rep_sums.loc[i, 'l_prob'],
                    self.ideology_stats_by_rep_sums.loc[i, 'ideology_prob'],
                    self.ideology_stats_by_rep_sums.loc[i, 'total_votes'],
                    self.ideology_stats_by_rep_sums.loc[i, 'most_neutral'],
                    self.ideology_stats_by_rep_sums.loc[i, 'c_prob_x'],
                    self.ideology_stats_by_rep_sums.loc[i, 'l_prob_x'],
                    self.ideology_stats_by_rep_sums.loc[i, 'ideology_prob_x'],
                    self.ideology_stats_by_rep_sums.loc[i, 'ideology_prob_x_zero_mean'],
                    self.ideology_stats_by_rep_sums.loc[i, 'tally_score'],
                    self.ideology_stats_by_rep_sums.loc[i, 'bioguide_id'],
                    self.ideology_stats_by_rep_sums.loc[i, 'ideology_type'],)

                    cursor.execute(sql_command)
                    connection.commit()

        ## Close yo shit
        connection.close()
    
    def __init__(self, ideology=None, roll_id=None, chamber=None, ideology_df=None, predictive_bills_votes=None,
                master_ideology=None, partisan_bills_only=None, ideology_stats_by_rep_sums=None):
        self.ideology = ideology
        self.roll_id = roll_id
        self.chamber = chamber
        self.ideology_df = ideology_df
        self.predictive_bills_votes = predictive_bills_votes
        self.master_ideology = master_ideology
        self.partisan_bills_only = partisan_bills_only
        self.ideology_stats_by_rep_sums = ideology_stats_by_rep_sums

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

        #### Is number a zipcode
        try:
            ## remove word zipcode and strip empty space
            search_term_zip = search_term.replace('zip', '').replace('code', '').strip(' ')

            ## length of term == 5
            if len(search_term_zip) == 5:
                try:
                    ## can it convert to number
                    self.zip_code = int(search_term_zip)
                    Search.check_zip_code(self)
                    if self.zip_code_check == True:
                        return Search.find_dist_by_zip(self).to_dict(orient='records')
                except:
                    "move on"
                
        except:
            "move on"
            
        try:
            search_term_dist = search_term.replace('district', '')
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
                        search_term_dist = str(us.states.lookup(search_term_dist)).lower()
                    except:
                        'dont change it'
                        
                    x = search_term_dist.split(' ')
                    search_term_query = ''
                    for i in range(len(x)):
                        search_term_query += """AND (lower(name) ilike '%' || '{}' || '%'
                        OR lower(state) ilike '%' || '{}' || '%'
                        OR lower(party) ilike '%' || '{}' || '%') """.format(x[i], x[i], x[i])
                        
                    return pd.read_sql_query("""
                    SELECT DISTINCT name,
                    bioguide_id,
                    state,
                    district,
                    party,
                    chamber,
                    photo_url
                    FROM congress_bio
                    WHERE served_until = 'Present'
                    {}
                    AND ({})
                    """.format(
                        search_term_query,
                        dist_search[4:]), open_connection()).to_dict(orient='records')
                else:
                    return pd.read_sql_query("""
                    SELECT DISTINCT name,
                    bioguide_id,
                    state,
                    district,
                    party,
                    chamber,
                    photo_url
                    FROM congress_bio
                    WHERE served_until = 'Present'
                    AND ({})
                    """.format(
                        dist_search[4:]), open_connection()).to_dict(orient='records')

            """
            If you make it here then 
            none of the other stuff worked.
            Just search what you originally got.
            """
            try:
                search_term = str(us.states.lookup(search_term)).lower()
            except:
                'dont change it'
            
            x = search_term.split(' ')
            search_term_query = ''
            for i in range(len(x)):
                search_term_query += """AND (lower(name) ilike '%' || '{}' || '%'
                OR lower(state) ilike '%' || '{}' || '%'
                OR lower(party) ilike '%' || '{}' || '%') """.format(x[i], x[i], x[i])
                
            return pd.read_sql_query("""
            SELECT DISTINCT name,
            bioguide_id,
            state,
            district,
            party,
            chamber,
            photo_url
            FROM congress_bio
            WHERE served_until = 'Present'
            {}
            """.format(
                search_term_query
                ), open_connection()).to_dict(orient='records')
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
            SELECT distinct name, 
            bioguide_id, 
            state, 
            district, 
            party, 
            chamber,
            photo_url
            FROM congress_bio
            WHERE (({})
            AND served_until = 'Present')
            OR (state = '{}' AND served_until = 'Present' AND chamber = 'senate')""".format(dist_query, dist.loc[i, 'state_long'])

            return pd.read_sql_query(sql_query, open_connection())
    
    
    def __init__(self, search_term=None, zip_code=None):
        self.search_term = search_term
        self.zip_code = zip_code