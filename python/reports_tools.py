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
    
    # @staticmethod
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
                                                                     "{}".format(month), 
                                                                     "{}".format(day),
                                                                    chamber.lower())
        print url
        try_again = True

        while try_again == True:
            s = requests.session()
            ## Get prxoy IP address
            spoof_df = Ip_Spoofer.random_ip()
            print spoof_df
            ip = str(spoof_df.loc[0, spoof_df.columns[0]])
            port = str(spoof_df.loc[0, spoof_df.columns[1]])
            proxies = {
              'http': '{}:{}'.format(ip, port),
            }
            s.proxies.update(proxies)
            a = requests.adapters.HTTPAdapter(max_retries=5)
            s.mount('https://', a)
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            s.headers.update(headers)
            r = s.get(url)
            print r.status_code
            if (r.status_code != 200) & (r.status_code != 404):
                print "bad status. trying get ip to collect sujects and links"
            else:
                try_again = False

        
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
        
    def collect_text(self, index, date, chamber, first=False):

        ## Request page
        print self.links[index]
        try_again = True

        while try_again == True:
            s = requests.session()
            ## Get prxoy IP address
            spoof_df = Ip_Spoofer.random_ip()
            print spoof_df
            ip = str(spoof_df.loc[0, spoof_df.columns[0]])
            port = str(spoof_df.loc[0, spoof_df.columns[1]])
            proxies = {
              'http': '{}:{}'.format(ip, port),
            }
            s.proxies.update(proxies)
            a = requests.adapters.HTTPAdapter(max_retries=5)
            s.mount('https://', a)
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            s.headers.update(headers)
            r = s.get(self.links[index])
            print r.status_code
            if r.status_code == 200:
                try_again = False
            else:
                print "bad status. trying get ip to collect text"
            
        print "fixed"

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

    @staticmethod   
    def collect_missing_records(chamber, type='new'):
        chamber = chamber.lower()

        ## Find missing dates
        if type == 'new':
            collect_dates = Congressional_report_collector.date_list(chamber)
        elif type.lower() == 'null':
            collect_dates = pd.read_sql_query("""
            SELECT date FROM congressional_record_{}
            WHERE text = 'None'
            AND date > '{}-01-01'
            ;
            """.format(chamber.lower(),
                datetime.datetime.now().year), open_connection())
            
            collect_dates = list(collect_dates['date'])

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
            Congressional_report_collector.record_to_sql(test_collection, "congressional_record_{}".format(chamber), uid=['index'])


    def whatd_they_say(self, chamber, year):
        if len(self.section) > 0:
            ## Sometimes the section is blank. It's a bug from the pdf conversion 
            ## (or at least I think it is).

            # # Import peeps
            all_reps = self.all_reps.loc[self.all_reps['chamber'] == chamber].reset_index(drop=True)
            all_reps.loc[:, 'last_name'] = all_reps['name'].apply(lambda x: x.split(', ')[0])

            ## subset for year
            all_reps.loc[all_reps['served_until'] == 'Present', 'served_until'] = 2017
            all_reps.loc[:, 'served_until'] = all_reps.loc[:, 'served_until'].astype(int)
            all_reps.loc[:, 'year_elected'] = all_reps.loc[:, 'year_elected'].astype(int)

            all_reps = all_reps.loc[((all_reps['year_elected'] <= int(year)) &
                (all_reps['served_until'] > int(year)))].reset_index(drop=True)


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

    def clean_text(self, df, row, chamber):

        self.section = df.loc[row, 'text']
        self.section_title = df.loc[row, 'subject']
        year = df.loc[row, 'date'].year

        subjects = []
        for i in range(1, len(self.section.split('\n\n\n'))):
            sub_subject = self.section.split('\n\n\n')[i].split('\n\n')[0].strip(' ')
            if sub_subject != self.section_title:
                subjects.append(sub_subject)

        for i in range(10):
            self.section = self.section.replace('  ', ' ')

        self.section = self.section.strip(' ')

        for subject in np.unique(subjects):
            self.section = self.section.replace('\n\n\n {}\n\n'.format(subject), '({}).  '.format(subject))

        self.section = self.section.replace('{}\n\n'.format(self.section_title), '')
        self.section = self.section.replace('\n', '').strip(' ')

        for i in range(10):
            self.section = self.section.replace('  ', ' ')

        x = Congressional_report_collector.whatd_they_say(self, chamber, year)

        try:
            x.loc[:, 'other_subject'] = None
            x.loc[:, 'chamber'] = chamber
            x.loc[:, 'index'] = df.loc[row, 'index']
            for subject in np.unique(subjects):
                sub_indexes = x.loc[x['speaker_text'].str.contains('({}).'.format(subject))].index
                for j in sub_indexes:
                    x.loc[j+1, 'other_subject'] = subject
                    x.loc[j, 'speaker_text'] = x.loc[j, 'speaker_text'].replace(' ({}).'.format(subject), '')
                    
            x.loc[x['other_subject'].isnull(), 'other_subject'] = 'None'
            
            for i in x.index:
                x.loc[i, 'index'] = '{}.{}'.format(x.loc[i, 'index'], i)
        except ValueError:
            x = pd.DataFrame()
            
                
        return x

    @staticmethod 
    def daily_text_clean(chamber, date):
        
        ## Get raw text
        df = pd.read_sql_query("""
        SELECT * FROM congressional_record_{}
        WHERE date = '{}'
        ;
        """.format(chamber, date), open_connection())
        
        ## Set obejct and clean for each subject
        test_cleaning = Congressional_report_collector()
        for i in df.index:
            try:
                test_cleaning.record_df = test_cleaning.record_df.append(Congressional_report_collector.clean_text(test_cleaning, df, i, chamber)).reset_index(drop=True)
            except:
                "Getting a weird ass error: 'error: cannot refer to open group'"

        ## If no speaking data found stop
        if len(test_cleaning.record_df) > 0:

            ## Add date column
            test_cleaning.record_df.loc[:, 'date'] = date

            ## Add to sql
            Congressional_report_collector.record_to_sql(test_cleaning, 'congressional_record_transcripts', ['index', 'chamber'])

    @staticmethod 
    def clean_missing_text(chamber):
        ########## Get date list to clean ##########
        cleaned_date = pd.read_sql_query("""
        SELECT chamber, date FROM congressional_record_transcripts
        WHERE chamber = '{}'
        AND date > '{}-01-01'
        """.format(chamber.lower(),
            datetime.datetime.now().year
            ), open_connection())

        all_dates = pd.read_sql_query("""
        SELECT DISTINCT date FROM congressional_record_{}
        WHERE text != 'None'
        AND date > '{}-01-01'
        ORDER BY date asc
        ;
        """.format(chamber.lower(), 
        datetime.datetime.now().year), open_connection())
        all_dates.loc[:, 'date'] = all_dates.loc[:, 'date'].astype(str)

        date_list = set(list(all_dates['date'])) - set(list(cleaned_date['date'].drop_duplicates()))
    
        ## For each date get the data        
        for date in date_list:
            print date
            try:
                Congressional_report_collector.daily_text_clean(chamber, date)
            except:
                print "not work"

        
    def __init__(self):
        self.subjects = []
        self.links = []
        self.record_df = pd.DataFrame()
        self.all_reps = pd.read_sql_query("""
            SELECT * FROM congress_bio
            ;
            """, open_connection())

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