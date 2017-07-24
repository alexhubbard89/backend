from urllib2 import urlopen
import requests
from bs4 import BeautifulSoup


my_ip = urlopen('http://ip.42.pl/raw').read()

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'
    }

print "this is my ip address {}".format(my_ip)

url = 'https://www.congress.gov/congressional-record/1995/01/04/senate-section'



print "try with header"
r = requests.get(url, headers=headers)
print "trying to connect to url:\n{}".format(url)
print "resonse is: {}".format(r.status_code)
if r.status_code == 429:
    print "this is the content"
    print r.content


print '\n\n\n'

print "try without header"
r = requests.get(url)
print "trying to connect to url:\n{}".format(url)
print "resonse is: {}".format(r.status_code)
if r.status_code == 429:
    print "this is the content"
    print r.content