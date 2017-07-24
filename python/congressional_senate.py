from urllib2 import urlopen
import requests
from bs4 import BeautifulSoup


my_ip = urlopen('http://ip.42.pl/raw').read()
print "this is my ip address {}".format(my_ip)


url = 'https://www.congress.gov/congressional-record/1995/01/04/senate-section'


## First try
s = requests.session()
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
print "try with header"
s.headers.update(headers)
r = s.get(url, timeout=15.3)
print "trying to connect to url:\n{}".format(url)
print "response is: {}".format(r.status_code)
if r.status_code != 200:
    print "this is the content"
    print r.content


print '\n\n\n'


## Second try
print "try without header"
s = requests.session()
r = s.get(url, timeout=15.3)
print "trying to connect to url:\n{}".format(url)
print "response is: {}".format(r.status_code)
if r.status_code != 200:
    print "this is the content"
    print r.content