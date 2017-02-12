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

class user_info(object):
    """
    This will be used to store users to db and and test login credentials.
    
    Attributes: email, password, if password is correct, name, gender, dob,
    street, zip_code, and user_df
    """
    
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
        df.loc[0, 'district'] = user_info.get_district_from_address(self, df.loc[0, 'city'], df.loc[0, 'state_short'],
                                                          df.loc[0, 'state_long'])

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
        except IntegrityError as e:
            """duplicate key value violates unique constraint "user_tbl_user_name_key"
            DETAIL:  Key (user_name)=(user_test) already exists."""
            connection.rollback()
            user_made = False
        connection.close()
        return user_made    

    def get_district_from_address(self, city, state_short, state_long):
        import requests
        import us

        state = '{}{}'.format(state_short, state_long)

        s = requests.Session()
        s.auth = ('user', 'pass')
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        }
        url = 'http://ziplook.house.gov/htbin/findrep?ADDRLK'
        form_data = {
            'street': self.street,
            'city': city,
            'state': state,
            'submit': 'FIND YOUR REP',
        }

        response = requests.request(method='POST', url=url, data=form_data, headers=headers)
        district = str(response.content.split('src="/zip/pictures/{}'.format(state_short.lower()))[1].split('_')[0])
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
            return user_results
        elif self.password_match == False:
            return "Check credentials frist"
        
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
    
    def __init__(self, email=None, password=None, password_match=False, first_name=None,
                last_name=None, gender=None, dob=None, street=None, zip_code=None, user_df=None):
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
             left_on=['state_long', 'district_num'], right_on=['state', 'district']).drop_duplicates()
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
        try:
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
        except:
            """There was no sponsor. 
            Rare but not impossible."""
            sponsor = None
            sponsors_df = pd.DataFrame([self.search_url, sponsor]).transpose()
            sponsors_df.columns = ['url', 'bioguide_id']
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
        for url in unique_legislation:
            self.search_url = url
            master_sponsors = master_sponsors.append(sponsorship_collection.get_sponsor_data(self))
        
        self.master_sponsors = master_sponsors.reset_index(drop=True)
        print 'To the database!'
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
                congress)
                VALUES ('{issue_link}', '{issue}', '{title_description}',
                        '{committees}', '{tracker}', '{congress}');"""


            sql_command = format_str.format(issue_link=p[0], issue=p[1], title_description=p[2],
                                           committees=p[3], tracker=p[4], congress=p[5])
            ## Commit to sql
            try:
                cursor.execute(sql_command)
                connection.commit()
                new_data += 1
            except:
                print 'duplicate'
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
        
    def __init__(self, legislation_by_congress=None, congress_search=None,
                new_data=None, updated_data=None):
        self.legislation_by_congress = legislation_by_congress
        self.congress_search = congress_search
        self.new_data = new_data
        self.updated_data = updated_data