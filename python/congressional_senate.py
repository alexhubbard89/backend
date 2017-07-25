import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

def free_proxy_list_net():

    s = requests.session()
    url = "https://free-proxy-list.net/"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    s.headers.update(headers)
    r = s.get(url)
    if r.status_code != 200: 
        return False


    page = BeautifulSoup(r.content, 'html5lib')
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
            
    return proxy_df

def hide_my_name():

    s = requests.session()
    url = "https://hidemy.name/en/proxy-list/"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    s.headers.update(headers)
    r = s.get(url)
    if r.status_code != 200: 
        return False


    page = BeautifulSoup(r.content, 'html5lib')
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
            
    return proxy_df

def gimmeproxy():
    s = requests.session()
    url = "https://gimmeproxy.com/api/getProxy"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    s.headers.update(headers)
    r = s.get(url)
    
    return pd.DataFrame(r.json()).reset_index(drop=True).drop(['anonymityLevel'], 1).drop_duplicates(['ip'])

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
            return True
        else:
            return False
    except:
        return False
        

def random_ip():
    num_list = [free_proxy_list_net, hide_my_name]
    rand_num = np.random.choice(len(num_list))
    df = num_list[rand_num]().head(50)
    
    indexes = list(df.index)    
    while len(indexes) > 0:
        rand_num = np.random.choice(indexes)
    
        x = pd.DataFrame([df.loc[rand_num, df.columns[:2]]]).reset_index(drop=True)
        good_ip = check_ip(str(x.loc[0, x.columns[0]]), str(x.loc[0, x.columns[1]]))
        if good_ip == True:
            indexes = list(set(indexes) - set([rand_num]))
            return x
    return "No working IP"
            

import psycopg2
import urlparse
import os
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import psycopg2
import urlparse
import os
import datetime
import calendar
from nltk import tokenize
import re

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
        '(',
        ')',
        'file://',
        'input://'
    ]
    for badstr in badstrings:
        if badstr in sanitized:
            sanitized = sanitized.replace(badstr, '')
    return sanitized

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

class Congressional_report_collector(object):
    @staticmethod
    def collect_subjets_and_links(self, year, month, day, chamber, ip, port):
        """
        Looks for congressional record information for 
        a given chamber. If there is a record for that
        day it will find all of the subjects discussed
        as well as the links for the txt files.

        Inputs:
        year, month, day: The date to look for
        chamber: The congressional chamber to look for

        Outputs:
        Array of subjects
        Array of urls for txt

        ----------------------------------------------

        If no congressional record exists then it will
        return two null values.
        """

        url = "https://www.congress.gov/congressional-record/{}/{}/{}/{}-section".format("{}".format(year).zfill(4),
                                                                     "{}".format(month).zfill(2), 
                                                                     "{}".format(day).zfill(2),
                                                                    chamber.lower())
        
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
            r = s.get(url)
            page = BeautifulSoup(r.content, 'lxml')

            try:
                body = page.find("tbody")
                subjects_raw = body.findAll('tr')

                for i in range(0, len(subjects_raw)):
                    self.subjects.append('. '.join(unidecode(subjects_raw[i].text).split('. ')[1:]).split(' |')[0])
                    self.links.append('https://www.congress.gov' + subjects_raw[i].find('a').get('href'))
                ## was data found?
                return True
            except:
                ## was data found?
                date = pd.to_datetime("{}-{}-{}".format("{}".format(year).zfill(4),
                      "{}".format(month).zfill(2), 
                      "{}".format(day).zfill(2)))
                self.record_df = pd.DataFrame(data=[[date, None, None, None, chamber.lower()]], 
                                  columns=['date', 'url', 'text', 'subject', 'chamber'])
                return False
        except:
            return "ip expired"
    
    def collect_text(self, index, date, chamber, ip, port, first=False):
        url = self.links[index]
        print url
        
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
            r = s.get(url)
        
            page = BeautifulSoup(r.content, 'lxml')
            page = page.find('div', class_='txt-box')

            try:
                ## Locate where report starts and ends
                if first == False:
                    page_text = page.text.split('[www.gpo.gov]\n\n')[1].split('____________________')[0]
                elif first == True:
                    try:
                        page_text = page.text.split('-----------------------------------------------------------------------')[1].split('____________________')[0]
                    except:
                        page_text = page.text.split('[www.gpo.gov]\n\n')[1].split('____________________')[0]


                ## If exists clean out page numbers
                try:
                    start_int = int(page_text.split('[[Page S')[1].split(']]')[0])
                    end_int = int(page_text.split('[[Page S')[len(page_text.split('[[Page S')) -1].split(']]')[0])
                    for i in range(start_int, end_int+1):
                        page_text = page_text.replace('\n\n[[Page S{}]]\n\n'.format(i), ' ')
                except:
                    "No page numbers"

                df = pd.DataFrame(data=[[date, self.links[index], page_text, self.subjects[index], chamber.lower()]], 
                                  columns=['date', 'url', 'text', 'subject', 'chamber'])

            except:
                df = pd.DataFrame(data=[[date, self.links[index], "NO TEXT FOUND", self.subjects[index], chamber.lower()]], 
                                  columns=['date', 'url', 'text', 'subject', 'chamber'])
            self.record_df = self.record_df.append(df).reset_index(drop=True)
            return True
        except:
            return "ip expired"
        
    def record_to_sql(self, tbl, uid):
        ## uid must be an array
        if isinstance(uid, list) == False:
            return "uid must be an array"

        ## Open Connection
        connection = open_connection()
        cursor = connection.cursor()

        for col in self.record_df.columns:
            try:
                self.record_df[col] = self.record_df[col].apply(lambda x: sanitize(unidecode(x)))
            except:
                try:
                    self.record_df[col] = self.record_df[col].apply(lambda x: sanitize(x))
                except:
                    "probs not a string"

        ## Loop through df
        for i in range(len(self.record_df)):
            ## set structure
            string_1 = """INSERT INTO {} (""".format(tbl)
            string_2 = """ VALUES ("""

            ## add data
            for col in self.record_df.columns:
                string_1 += "{}, ".format(col.lower())
                string_2 += "'{}', ".format(self.record_df.loc[i, col.lower()])
            string_1 = string_1[:-2] + ")"
            string_2 = string_2[:-2] + ");"
            sql_command = string_1 + string_2

            try:
                # Try to insert
                cursor.execute(sql_command)
                connection.commit()
            except:
                connection.rollback()
                for j in range(len(self.record_df)):
                    ## set structure
                    string_1 = """UPDATE {} SET """.format(tbl)
                    string_2 = """ WHERE ("""

                    ## add data
                    for col in self.record_df.columns:
                        if col not in uid:
                            string_1 += "{} = '{}', ".format(col.lower(), self.record_df.loc[j, col.lower()])
                        elif col in uid:
                            if len(string_2) > 8:
                                string_2 += " AND {} = '{}'".format(col.lower(), self.record_df.loc[j, col.lower()])
                            else:
                                string_2 += "{} = '{}'".format(col.lower(), self.record_df.loc[j, col.lower()])
                    string_1 = string_1[:-2]
                    string_2 = string_2[:] + ");"
                    sql_command = string_1 + string_2

                    ## Update
                    cursor.execute(sql_command)
                    connection.commit()

        ## Close yo shit
        connection.close()
        
    
    def __init__(self):
        self.subjects = []
        self.links = []
        self.record_df = pd.DataFrame()



print "collet list of dates"
date_list = []
for year in range(1997, 2017):
    ## Collect array of dats
    for month in range(1, 13):
        num_days = calendar.monthrange(year, month)[1]
        for day in range(1, num_days+1):
            search_date = '{}-{}-{}'.format(year, '{}'.format(month).zfill(2), '{}'.format(day).zfill(2))
            ## Convert to date time when saving to array
            date_list.append(pd.to_datetime(search_date))

## Dont need the whole this for testing
# date_list = list(pd.DataFrame(pd.to_datetime(date_list)).head(20)[0])
print date_list
            
print 'find initial ip'
ip_df = random_ip()
print ip_df


while len(date_list) > 0:
    print 'lenght of date list: {}'.format(len(date_list))
    test_collection = Congressional_report_collector()
    chamber = 'senate'
    date = date_list[0]
    collection = Congressional_report_collector.collect_subjets_and_links(test_collection, date.year, date.month, date.day, chamber, str(ip_df.loc[0, ip_df.columns[0]]), str(ip_df.loc[0, ip_df.columns[1]]))
    print date
    print collection
    if collection != "ip expired":
        """If collection happened remove from date list
        do the rest of the collection aaannndd save to sql"""
        date_list = list(set(date_list) - set([date]))
        
        counter = 0
        while counter < len(test_collection.subjects):
            print counter
            print test_collection.subjects[counter]

            if counter > 0:
                collected = Congressional_report_collector.collect_text(test_collection, index=counter, date=date, chamber=chamber, ip=str(ip_df.loc[0, ip_df.columns[0]]), port=str(ip_df.loc[0, ip_df.columns[1]]))
            elif counter == 0:
                collected = Congressional_report_collector.collect_text(test_collection, index=counter, date=date, chamber=chamber, ip=str(ip_df.loc[0, ip_df.columns[0]]), port=str(ip_df.loc[0, ip_df.columns[1]]), first=True)

            if collected != "ip expired":
                """If the ip still works than advance the counter"""
                counter +=1
                
            else:
                print "find new ip"
                ip_df = random_ip()
            
        print "set index"
        ## index for sql prep
        for i in range(len(test_collection.record_df)):
            test_collection.record_df.loc[i, 'index'] = str(test_collection.record_df.loc[i, 'date']).replace('-', '').split(' ')[0] + '{}'.format(i)

        print "store data"
        ## Save data
        Congressional_report_collector.record_to_sql(test_collection, "congressional_record_{}".format(chamber), uid=['index'])
        
    else:
        print "find new ip"
        ip_df = random_ip()