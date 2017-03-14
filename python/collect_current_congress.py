from __future__ import division
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import urlparse
import psycopg2
import os
import sys
from datetime import datetime
from json import dumps
import math
import re
import time


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

class bio_data_collector(object):
    """
    This will be used to collet information for each person in congress.
    
    Attributes:
    current_congress - what number congress
    house_df - scraped house data
    senate_df - scraped senate data
    overall_df - merged house and senate data
    status_code - status code for scraping
    collect_all - do I have up-to-date data
    """
    

    def most_recent_congress_number(self):
        ## Get current congress
        url = 'https://www.congress.gov/members'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        }
        r = requests.get(url, headers=headers)
        page = BeautifulSoup(r.content, 'lxml')

        self.current_congress = int(str(page.find_all('ul', id='innerbox_congress')).split('facetItemcongress')[1].split('__')[0])
        return self.current_congress


    def get_congress_by_gov(self, congress_num=None, chamber=None):
        df = pd.DataFrame()

        url = 'https://www.congress.gov/search?searchResultViewType=expanded&q=%7B"source":["members"],"congress":"{}","chamber":"{}"%7D&pageSize=250&page={}'.format(congress_num, chamber.title(), 1)
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        }
        r = requests.get(url, headers=headers)
        page = BeautifulSoup(r.content, 'lxml')
        page_max = int(math.ceil(
                int(str(page.find_all('span', id='facetItemsourceMemberscount')).split('>[')[1].split(']<')[0])/250))
        print page_max

        for i in range(1,page_max+1):

            url = 'https://www.congress.gov/search?searchResultViewType=expanded&q=%7B"source":["members"],"congress":"{}","chamber":"{}"%7D&pageSize=250&page={}'.format(congress_num, chamber.title(), i)
            headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
            }
            r = requests.get(url, headers=headers)
            if r.status_code == 403:
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
                # Adding empty header as parameters are being sent in payload
                headers = {}
                r = requests.get(url, data=dumps(payload), headers=headers)
                if r.status_code == 403:
                    if chamber.lower() == 'house':
                        self.house_df = df
                        self.status_code = 403
                    elif chamber.lower() == 'senate':
                        self.senate_df = df
                        self.status_code = 403

            page = BeautifulSoup(r.content, 'lxml')

            ## Just keep the data I need
            x = page.find_all('ol', class_='basic-search-results-lists expanded-view')
            split_data = str(x).split('<li class="expanded">')

            ## Extract only what I need. Helps performance
            for j in range(1, len(split_data)):
                num_times_served = len(split_data[j].split('member-served')[1].split('{}: '.format(chamber.title()))[1].split('</li>')[0].split(', '))
                for times in range(num_times_served):
                    ## Because the loop goes more than once I have to fix the index
                    ## Also because someone can serve more than onces I need to include that
                    index_num = len(df)
                    try:
                        df.loc[index_num, 'name'] = str(split_data[j]).split('<img alt="')[1].split('"')[0]
                    except:
                        df.loc[index_num, 'name'] = str(split_data[j]).split(
                            'href="https://www.congress.gov/member')[1].split(
                            '">')[1].split('</a>')[0].replace('Representative ', '').replace('Senator ', '')
                    df.loc[index_num, 'bioguide_id'] = split_data[j].split('<a href="')[1].split('">')[0].split('/')[-1].split('?')[0]
                    df.loc[index_num, 'state'] = split_data[j].split('State:')[1].split('<span>')[1].split('</span>')[0]
                    try: 
                        df.loc[index_num, 'district'] = split_data[j].split('District:')[1].split('<span>')[1].split('</span>')[0]
                    except:
                        df.loc[index_num, 'district'] = 0
                    df.loc[index_num, 'party'] = split_data[j].split('Party:')[1].split('<span>')[1].split('</span>')[0]
                    df.loc[index_num, 'year_elected'] = split_data[j].split('member-served')[1].split('{}: '.format(chamber.title()))[1].split('</li>')[0].split(', ')[times].split('-')[0]
                    try:
                        df.loc[index_num, 'served_until'] = split_data[j].split('member-served')[1].split('{}: '.format(chamber.title()))[1].split('</li>')[0].split(', ')[times].split('-')[1]
                    except: 
                        df.loc[index_num, 'served_until'] = df.loc[index_num, 'year_elected'] 
                    try:
                        df.loc[index_num, 'photo_url'] = "congress.gov/img/member/{}".format(str(split_data[j]).split('/img/member/')[1].split('"')[0])
                    except:
                        df.loc[index_num, 'photo_url'] = None
                    df.loc[index_num, 'congress_url'] = split_data[j].split('<a href="')[1].split('?r=')[0]
                    df.loc[index_num, 'chamber'] = chamber

        if chamber.lower() == 'house':
            self.house_df = df.reset_index(drop=True)
            self.status_code = 200
        elif chamber.lower() == 'senate':
            self.senate_df = df.reset_index(drop=True)
            self.status_code = 200
    
    def get_bio_text(self):
        print 'total {}'.format(len(self.overall_df))
        ## Loop thorugh every senator to get bios
        for i in range(len(self.overall_df)):
            ## Go to url of each senator
            url = 'http://bioguide.congress.gov/scripts/biodisplay.pl?index={}'.format(self.overall_df.loc[i, 'bioguide_id'])
            r = requests.get(url)
            c = r.content
            soup = BeautifulSoup(c, "lxml")

            ## Save bio text in data set
            try:
                bio_text = str(soup.findAll('p')[0])
            except:
                bio_text = ''
            ## Remove html tags
            self.overall_df.loc[i, 'bio_text'] = re.sub("<[^>]*>","",bio_text).replace('\r','').replace('\n','')
        print 'done'

    def get_info_from_congress_page(self):
        for i in range(len(self.overall_df['congress_url'])):
            url = self.overall_df.loc[i, 'congress_url']
            print url
            
            r = requests.get(url)
            page = BeautifulSoup(r.content, 'lxml')
            x = page.find_all('div', class_='member_profile')
            try:
                self.overall_df.loc[i, 'leadership'] = str(x[0]).split('<h4>')[1].split('</h4>')[0]
            except:
                self.overall_df.loc[i, 'leadership'] = None
            try:
                self.overall_df.loc[i, 'website'] = x[0].find('a').get('href')
            except:
                self.overall_df.loc[i, 'website'] = None
            try:
                self.overall_df.loc[i, 'address'] = str(x[0].find('th', class_='member_contact').find_next()).split('\n')[1].split('<br')[0].replace('            ', '')
            except:
                self.overall_df.loc[i, 'address'] = None
            try:
                self.overall_df.loc[i, 'phone'] = str(x[0].find('th', class_='member_contact').find_next()).split('<br/>')[1].split('        ')[0].replace(' ', '')
            except:
                self.overall_df.loc[i, 'phone'] = None
        print 'done'
        
    def find_social_media(self):
        ## add payload so to fake stack information.
        ## Senate tries to only allow people and not robots.
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
        # Adding empty header as parameters are being sent in payload
        headers = {}
        ## get indices to search
        websites_search = self.overall_df.loc[self.overall_df['website'].notnull()].index

        for i in range(len(websites_search)):
            try:
                url = self.overall_df.loc[websites_search[i], 'website']
                print url
                r = requests.get(url, data=dumps(payload), headers=headers)
                page = BeautifulSoup(r.content, 'lxml')
                
                if 'http-equiv="refresh"' in str(page):
                    url = str(page.find('meta')).split('URL=')[1].split('"')[0]
                    r = requests.get(url, data=dumps(payload), headers=headers)
                    page = BeautifulSoup(r.content, 'lxml')

                twitter_array = []
                facebook_array = []

                for href in page.find_all('a'):
                    if href.get('href') != None:
                        if 'twitter' in href.get('href').lower():
                             twitter_array.append(href.get('href'))
                        if 'facebook' in href.get('href').lower():
                             facebook_array.append(href.get('href'))

                try:
                    twitter_df = pd.DataFrame(pd.DataFrame(twitter_array)[0].str.replace('http://', '').str.replace('https://', ''))
                    twitter_url = twitter_df[0].value_counts().reset_index().loc[0, 'index']
                    twitter_url = twitter_url.strip("javaScript:openWin('").strip(")'").replace('witter', 'twiter').replace('ttwitter', 'twiter')
                    twitter_url = twitter_url.replace('//tt', 't').replace('http://', '').replace('https://', '').replace('//', '')
                    twitter_url = 'https://{}'.format(twitter_url)
                    try:
                        twitter_handle = '@{}'.format(twitter_url.split('.com/')[1].replace('#', '').replace('!','')).replace('@/','@')
                    except:
                        twitter_handle = '@{}'.format(twitter_url.split('.com')[1])
                    self.overall_df.loc[websites_search[i], 'twitter_handle'] = twitter_handle
                    self.overall_df.loc[websites_search[i], 'twitter_url'] = twitter_url
                except:
                    self.overall_df.loc[websites_search[i], 'twitter_handle'] = None
                    self.overall_df.loc[websites_search[i], 'twitter_url'] = None

                try:
                    facebook_df = pd.DataFrame(pd.DataFrame(facebook_array)[0].str.replace('http://', '').str.replace('https://', ''))
                    facebook_url = facebook_df[0].value_counts().reset_index().loc[0, 'index']
                    facebook_url = facebook_url.strip("javaScript:openWin('").strip(")'")
                    self.overall_df.loc[websites_search[i], 'facebook'] = facebook_url
                except:
                    self.overall_df.loc[websites_search[i], 'facebook'] = None
            except:
                "some urls might not work at all"
                self.overall_df.loc[websites_search[i], 'twitter_handle'] = None
                self.overall_df.loc[websites_search[i], 'twitter_url'] = None
                self.overall_df.loc[websites_search[i], 'facebook'] = None
                
    def update_sql(self):
        connection = open_connection()
        cursor = connection.cursor()


        self.overall_df = self.overall_df[['name', 'state', 'district', 'party', 'served_until', 'photo_url',
        'congress_url', 'bioguide_id', 'chamber', 'year_elected']] 
                        
        for i in range(len(self.overall_df)):
            try:
                self.overall_df.loc[i, 'name'] = self.overall_df.loc[i, 'name'].replace("'", "''")
            except:
                'hold'
            try:
                self.overall_df.loc[i, 'name'] = str(self.overall_df.loc[i, 'name'].decode('unicode_escape').encode('ascii','ignore'))
            except:
                'hold'
            try:
                ## Update what I got
                sql_command = """UPDATE congress_bio 
                SET
                name = '{}',
                state = '{}',
                district = '{}',
                party = '{}',
                served_until = '{}',
                photo_url = '{}',
                congress_url = '{}'
                WHERE (
                bioguide_id = '{}' 
                and chamber = '{}'
                and year_elected = '{}');""".format(
                self.overall_df.loc[i, 'name'],
                self.overall_df.loc[i, 'state'],
                int(self.overall_df.loc[i, 'district']),
                self.overall_df.loc[i, 'party'],
                self.overall_df.loc[i, 'served_until'],
                self.overall_df.loc[i, 'photo_url'],
                self.overall_df.loc[i, 'congress_url'],
                self.overall_df.loc[i, 'bioguide_id'],
                self.overall_df.loc[i, 'chamber'],
                int(self.overall_df.loc[i, 'year_elected']))    
                cursor.execute(sql_command)
                connection.commit()
            except:
                print '{} - {} had a problem'.format(i, self.overall_df.loc[i, 'name'])
                connection.rollback()
        # Close
        connection.close()


    def put_into_sql_congress(self):
        connection = open_connection()
        cursor = connection.cursor()

        ## Put data into table
        for i in range(len(self.overall_df)):
            try:
                self.overall_df.loc[i, 'bio_text'] = self.overall_df.loc[i, 'bio_text'].replace("'", "''")
            except:
                'hold'
            try:
                self.overall_df.loc[i, 'bio_text'] = str(self.overall_df.loc[i, 'bio_text'].decode('unicode_escape').encode('ascii','ignore'))
            except:
                'hold'

            try:
                self.overall_df.loc[i, 'name'] = self.overall_df.loc[i, 'name'].replace("'", "''")
            except:
                'hold'
            try:
                self.overall_df.loc[i, 'name'] = str(self.overall_df.loc[i, 'name'].decode('unicode_escape').encode('ascii','ignore'))
            except:
                'hold'

            x = list(self.overall_df.loc[i,])

            for p in [x]:
                format_str = """INSERT INTO congress_bio (
                name, 
                bioguide_id,  
                state, 
                district, 
                party, 
                year_elected, 
                served_until,
                photo_url,
                congress_url,
                chamber,
                bio_text,
                leadership_position,
                website,
                address,
                phone,
                twitter_handle,
                twitter_url,
                facebook)
                VALUES ('{name}', '{bioguide_id}', '{state}', '{district}', '{party}', '{year_elected}', 
                '{served_until}', '{photo_url}', '{congress_url}', '{chamber}', '{bio_text}', '{leadership_position}', 
                '{website}', '{address}', '{phone}', '{twitter_handle}', '{twitter_url}', '{facebook}');"""

                sql_command = format_str.format(name=p[0], bioguide_id=p[1], state=p[2], district=p[3], 
                                                party=p[4], year_elected=p[5], served_until=p[6], 
                                                photo_url=p[7], congress_url=p[8], chamber=p[9], bio_text=p[10], 
                                                leadership_position=p[11], website=p[12],
                                                address=p[13], phone=p[14], twitter_handle=p[15],
                                               twitter_url=p[16], facebook=p[17])
                try:
                    cursor.execute(sql_command)
                    connection.commit()

                except:
                    ## Update what I got
                    connection.rollback()
                    sql_command = """UPDATE congress_bio 
                    SET
                    district = '{}',
                    party = '{}',
                    served_until = '{}',
                    photo_url = '{}',
                    congress_url = '{}', 
                    bio_text = '{}', 
                    leadership_position = '{}', 
                    website = '{}', 
                    address = '{}',
                    phone = '{}',
                    twitter_handle = '{}',
                    twitter_url = '{}',
                    facebook ='{}'
                    WHERE (
                    bioguide_id = '{}' 
                    and chamber = '{}'
                    and year_elected = '{}');""".format(
                    int(self.overall_df.loc[i, 'district']),
                    self.overall_df.loc[i, 'party'],
                    self.overall_df.loc[i, 'served_until'],
                    self.overall_df.loc[i, 'photo_url'],
                    self.overall_df.loc[i, 'congress_url'],
                    self.overall_df.loc[i, 'bio_text'],
                    self.overall_df.loc[i, 'leadership'],
                    self.overall_df.loc[i, 'website'],
                    self.overall_df.loc[i, 'address'],
                    self.overall_df.loc[i, 'phone'],
                    self.overall_df.loc[i, 'twitter_handle'],
                    self.overall_df.loc[i, 'twitter_url'],
                    self.overall_df.loc[i, 'facebook'],
                    self.overall_df.loc[i, 'bioguide_id'],
                    self.overall_df.loc[i, 'chamber'],
                    int(self.overall_df.loc[i, 'year_elected']))    
                    cursor.execute(sql_command)
                    connection.commit()
        # never forget this, if you want the changes to be saved:
        connection.close()

    ## Should I do more data collection?
    def create_new_table_check(self):
        connection = open_connection()
        cursor = connection.cursor()


        df_checker = pd.read_sql_query("""SELECT * FROM congress_bio WHERE served_until = 'Present'""", connection)
        connection.close()

        df = self.overall_df.loc[self.overall_df['served_until'] == 'Present'].reset_index(drop=True)
        df.loc[:,'duplicate'] = df.loc[:,'bioguide_id'].apply(lambda x: len(df_checker.loc[df_checker['bioguide_id'].astype(str) == str(x)]) > 0)
        if len(df.loc[df['duplicate']==False]) == 0:
            self.collect_all = False
        elif len(df.loc[df['duplicate']==False]) > 0:
            self.collect_all = True
            
    def collect_current_congress(self):
            """This script will collect data on current
            congression people, create a table in the database,
            and store them in said table. If I do not have the most
            up-to-date data then it will recollect all congress
            data from 1989 until today."""

            print 'get current congress number'
            missing_years = ''
            bio_data_collector.most_recent_congress_number(self)

            print 'getting data 1'
            ## Collect current house reps
            bio_data_collector.get_congress_by_gov(self, congress_num=self.current_congress, chamber='house')
            ## If I get a bad status code try a few more times.
            print self.status_code
            if self.status_code == 403:
                scraper_counter = 0
                while self.status_code == 403:
                    if scraper_counter == 10:
                        return "But it got a status code of 403 Forbidden HTTP" 
                    elif scraper_counter < 10:
                        bio_data_collector.get_congress_by_gov(self, congress_num=self.current_congress, chamber='house')
                        print status_code

            ## Collect current senators
            bio_data_collector.get_congress_by_gov(self, congress_num=self.current_congress, chamber='senate')
            ## If I get a bad status code try a few more times.
            print self.status_code
            if self.status_code == 403:
                scraper_counter = 0
                while self.status_code == 403:
                    if scraper_counter == 10:
                        return "But it got a status code of 403 Forbidden HTTP" 
                    elif scraper_counter < 10:
                        bio_data_collector.get_congress_by_gov(self, congress_num=self.current_congress, chamber='senate')
                        print self.status_code

            ## Check if the current congress is up to date
            self.overall_df = self.house_df.append(self.senate_df).reset_index(drop=True)
            bio_data_collector.create_new_table_check(self)
            print 'should I keep scraping? {}'.format(self.collect_all)
            if self.collect_all == False:
                print 'I have the most up to date data!'
                print 'update database'
                bio_data_collector.update_sql(self)
                return 'No New Data was collected'
            elif self.collect_all == True:
                ## If it's not up to date then collect data to replace current congress
                print 'getting data 2: Bio text'
                scraper_counter = 0
                try_scrape = True
                while try_scrape == True:
                    ## Try the scraper. If it breaks b/c connection issues
                    ## try again. Max scrape tries is 3.
                    try:
                        if scraper_counter == 3:
                            return "broken"
                        elif scraper_counter < 3:
                            bio_data_collector.get_bio_text(self)
                            try_scrape = False
                    except:
                        print 'I tried'
                        scraper_counter += 1
                        time.sleep(5)


                print 'getting data 3: Contact and leadership'
                scraper_counter = 0
                try_scrape = True
                while try_scrape == True:
                    ## Try the scraper. If it breaks b/c connection issues
                    ## try again. Max scrape tries is 3.
                    try:
                        if scraper_counter == 3:
                            return "broken"
                        elif scraper_counter < 3:
                            bio_data_collector.get_info_from_congress_page(self)
                            try_scrape = False
                    except:
                        print 'I tried'
                        scraper_counter += 1
                        time.sleep(5)


                print 'getting data 4: Social media'
                scraper_counter = 0
                try_scrape = True
                while try_scrape == True:
                    ## Try the scraper. If it breaks b/c connection issues
                    ## try again. Max scrape tries is 3.
                    try:
                        if scraper_counter == 3:
                            return "broken"
                        elif scraper_counter < 3:
                            bio_data_collector.find_social_media(self)
                            try_scrape = False
                    except:
                        print 'I tried'
                        scraper_counter += 1
                        time.sleep(5)

                print 'clean zee data'
                ## If a column has null then be explicit. This is helpful to putting 
                ## data in the db.
                clean_columns = ['photo_url', 'congress_url',
                                 'chamber', 'bio_text', 'leadership', 'website', 'address',
                                 'phone', 'twitter_handle', 'twitter_url', 'facebook']
                for column in clean_columns:
                    self.overall_df.loc[self.overall_df[column].isnull(), column] = None

                ## District should be an int
                self.overall_df.loc[self.overall_df['district'].isnull(), 'district'] = 100
                self.overall_df.loc[:, 'district'] = self.overall_df.loc[:, 'district'].astype(int)

                print 'put into sql'
                bio_data_collector.put_into_sql_congress(self)
                print 'donezo!'
                if len(missing_years) > 0:
                    return 'Data was collected but - {}'.format(missing_years)
                else:
                    return 'All Data was collected!'
            
    def __init__(self, current_congress=None, house_df=None, senate_df=None, status_code=None, overall_df=None):
        self.current_congress = current_congress
        self.house_df = house_df
        self.senate_df = senate_df
        self.overall_df = overall_df
        self.status_code = status_code
        self.collect_all = False