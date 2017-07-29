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

    def check_ip(self, ip, port):
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
            good_ip = check_ip(str(x.loc[0, x.columns[0]]), str(x.loc[0, x.columns[1]]))
            if good_ip == True:
                indexes = list(set(indexes) - set([rand_num]))
                return x
        return "No working IP"
    
    def __init__(self):
        self.proxy_df = pd.DataFrame()