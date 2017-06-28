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
    def date_list(chamber):
        """
        Checks for dates where there is no
        Congressional transcript date for a given
        chamber.
        
        
        Inputs:
        chamber: The congressional chamber to look for
        
        Outputs:
        Array of dates
        
        ----------------------------------------------
        
        If up to date then it returns a null value.
        """
        
        max_date = pd.read_sql_query("""
                    SELECT max(date) FROM congressional_record_{}
                    ;
                    """.format(chamber.lower()), open_connection()).loc[0, 'max']

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
    
    @staticmethod
    def collect_subjets_and_links(self, year, month, day, chamber):
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
        r = requests.get(url)
        page = BeautifulSoup(r.content, 'lxml')
        
        try:
            body = page.find("tbody")
            subjects_raw = body.findAll('tr')

            for i in range(0, len(subjects_raw)):
                self.subjects.append(' '.join(unidecode(subjects_raw[i].text).split('. ')[1:]).split(' |')[0])
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
        
    def collect_text(self, index, date, chamber, first=False):

        ## Request page
        print self.links[index]
        r = requests.get(self.links[index])
        page = BeautifulSoup(r.content, 'lxml')

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
        self.record_df = self.record_df.append(df).reset_index(drop=True)
        
    def record_to_sql(self, tbl):

        ## Open Connection
        connection = open_connection()
        cursor = connection.cursor()

        for col in self.record_df.columns:
            try:
                self.record_df[col] = self.record_df[col].apply(lambda x: sanitize(unidecode(x)))
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

        ## Close yo shit
        connection.close()

    @staticmethod   
    def collect_missing_records(chamber):
        chamber = chamber.lower()

        ## Find missing dates
        collect_dates = Congressional_report_collector.date_list(chamber)

        ## Loop though missing dates
        for date in collect_dates:
            ## Delete before push
            print date

            ## Find if record exists
            test_collection = Congressional_report_collector()
            record_exists = Congressional_report_collector.collect_subjets_and_links(test_collection, year=date.year, 
                                                      month=date.month, day=date.day, chamber=chamber)

            if record_exists == False:
                "There are no records"
            elif record_exists == True:
                ## Collect text
                for i in range(len(test_collection.subjects)):
                    print i
                    print test_collection.subjects[i]
                    if i > 0:
                        Congressional_report_collector.collect_text(test_collection, index=i, date=date, chamber=chamber)
                    elif i == 0:
                        Congressional_report_collector.collect_text(test_collection, index=i, date=date, chamber=chamber, first=True)

            ## index for sql prep
            for i in range(len(test_collection.record_df)):
                test_collection.record_df.loc[i, 'index'] = str(test_collection.record_df.loc[i, 'date']).replace('-', '').split(' ')[0] + '{}'.format(i)

            ## Save data
            Congressional_report_collector.record_to_sql(test_collection, "congressional_record_{}".format(chamber))

        
    def __init__(self):
        self.subjects = []
        self.links = []
        self.record_df = pd.DataFrame()