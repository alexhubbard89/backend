import smtplib
import os
import imp
collect_current_congress = imp.load_source('module', './python/collect_current_congress.py')


fromaddr = 'tallyscraper@gmail.com'
toaddrs  = 'alexhubbard89@gmail.com'

try:
    collect_current_congress.collect_current_congress_house()
    msg = "\r\n".join([
            "From: tallyscraper@gmail.com",
            "To: alexhubbard89@gmail.com",
            "Subject: All good!",
            "","You successfully scrapped data!"
        ])
except:
    msg = "\r\n".join([
            "From: tallyscraper@gmail.com",
            "To: alexhubbard89@gmail.com",
            "Subject: Something broke",
            "","""Something about the data scraper went bad :(
            collect_current_congress didn't work"""
        ])
username = 'tallyscraper@gmail.com'
password = os.environ["tallyscraper_password"]
print "password worked"
server = smtplib.SMTP('smtp.gmail.com:587')
server.ehlo()
server.starttls()
server.login(username,password)
server.sendmail(fromaddr, toaddrs, msg)
server.quit()