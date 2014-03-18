#!/usr/bin/env python

from time import sleep
from os import path, pardir, curdir, makedirs
from optparse import OptionParser, make_option
from sys import argv, exit
from impetus import Impetus
from base64 import b64encode, b64decode
from zlib import compress
from bs4 import BeautifulSoup
from threading import Lock
from random import randint


class Yelp(Impetus):

   domain= 'http://www.yelp.com'
   seed_urls= [
      "/search?find_desc=restaurants&find_loc=Manhattan%2C+NY",
      "/search?find_desc=restaurants&find_loc=San%20Francisco+CA",
      "/search?find_desc=restaurants&find_loc=Los%20Angeles+CA",
      "/search?find_desc=restaurants&find_loc=Chicago%2C+IL"
   ]
  
   user_agents= [
      'Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405',
      'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36',
      'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/532.2 (KHTML, like Gecko) ChromePlus/4.0.222.3 Chrome/4.0.222.3 Safari/532.2',
       'Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0',
       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
       'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7)',
       'Mozilla/5.0 (Windows; U; MSIE 9.0; WIndows NT 9.0; en-US))',
       'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0',
       'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0',
       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:25.0) Gecko/20100101 Firefox/25.0'
   ]

   def __init__(self, queue, authkey, taskdir= None, docdir= None, id= None, **properties):

      self.queue= queue
      super(Yelp, self).__init__(self.queue, authkey, taskdir, id, **properties)
      self.docdir= docdir

      self.crawl_file= path.join(self.docdir, "crawl_list.tab")
      self.crawl_list= open(self.crawl_file, "a+")
      try:
         self.document_id= int(self.crawl_list.readlines()[-1].split()[0])
         self.document_id+= 1
      except:
         self.document_id= 0

      self.lock= Lock()
   
   @Impetus.node
   def fetch_page(url):

      from urllib2 import HTTPRedirectHandler, build_opener, Request
      from urllib import urlencode
      from re import match

      def get_location_replace(html):

         for line in html.split("\n"):
            if "location.replace" in line:
               m= match(".*location\.replace\(\"(.*)\"\).*", str(line))
               return m.group(1)

      request= Request(url)
      request.add_header('User-Agent', self.user_agents[randint(0, 9)])
      opener= build_opener()
      response= opener.open(request)
      html= response.read()

      if 'location.replace' in html:
         url= get_location_replace(html)
         request= Request(url)
         request.add_header('User-Agent', self.user_agents[randint(0, 9)])
         opener= build_opener()
         response= opener.open(request)
         html= response.read()
      print "fetch page", url
      return (url, html)  

   @Impetus.node
   def get_listing_urls(html):

      from bs4 import BeautifulSoup

      urls= [listing['href'] for listing in  BeautifulSoup(html, "html").find_all("a", class_= "biz-name")]
      print "get listings urls", urls
      return urls

   @Impetus.node
   def fetch_next_url(html):

      from bs4 import BeautifulSoup
      from urllib2 import build_opener, Request
      from urllib import urlencode

      parser= BeautifulSoup(html, "html").find_all("a", class_= "page-option prev-next")
      url= None
      response= None
      for prevnext in parser:
         url= prevnext["href"]
         print "FOUND", url
      print "USING", url
      request= Request('http://yelp.com' + url)
      request.add_header('User-Agent', self.user_agents[randint(0, 9)])
      opener= build_opener()
      request= opener.open(request)
      response= request.read()

      return (url, response)

   @Impetus.node
   def fetch_menu(html):

      from bs4 import BeautifulSoup
      from urllib2 import build_opener, Request
      from urllib import urlencode

      parser= BeautifulSoup(html, "html").find("a", class_= "menu-explore")
      if parser:
         url= parser["href"]
         request= Request('http://yelp.com' + url)
         request.add_header('User-Agent', self.user_agents[randint(0, 9)])
         opener= build_opener()
         request= opener.open(request)
         response= request.read()
      else:
         url= None
         response= None

      return (url, response)


   def save_document(self, url, html):

      print "saving", url
      with self.lock:
         document_id= self.document_id
         self.crawl_list.write("%s\t%s\n" % (document_id, url))
         self.crawl_list.flush()
         self.document_id+= 1

      fh= open(path.join(self.docdir, "%s.doc" % (document_id)), 'wb')
      fh.write(compress(html))
      fh.close()

   def visited(self, url):

      found= False
      fh= open(self.crawl_file, "r")
      for line in fh.readlines():
         if url in line:
            found= True
      fh.close()
      return found

   @Impetus.startup
   def start(self):

      for url in self.seed_urls:
         self.fork(self.fetch_page, self.domain + url, delay= randint(5, 20))
         sleep(0.025)   

   @Impetus.process
   def extract_links(self, ready, errors):

      for job in ready:
         (url, html)= job.get("result")
         if html != None and self.visited(url) == False:
            print "extract links", url

            self.save_document(url, html)
            self.fork(self.fetch_next_url, html, callback= "extract_links", delay= randint(5, 20))
            self.fork(self.get_listing_urls, html, callback= "fetch_profiles")
         sleep(0.025)   

      for job in errors:
         print "ERROR", job.get("result")

   @Impetus.process
   def fetch_profiles(self, ready, errors):
      print "in fetch profiles"
      for job in ready:
         for url in job.get("result"):
            print "fetch profile", url
            self.fork(self.fetch_page, self.domain + url, callback= "process_profiles", delay= randint(5, 20))
            sleep(0.025)

      for job in errors:
         print "ERROR", job.get("result")

   @Impetus.process
   def process_profiles(self, ready, errors):
      print "in process profiles"
      for job in ready:
         (url, html)= job.get("result")
         if html != None:
            print "saving profile", url
            self.save_document(url, html)
            print "profile page", url, len(html)
            self.fork(self.fetch_menu, html, callback= "process_menus", delay= randint(5, 20))
         sleep(0.025)

      for job in errors:
         print "ERROR", job.get("result")

   @Impetus.process
   def process_menus(self, ready, errors):
      print "in process menus"
      for job in ready:
         (url, html)= job.get("result")
         if html != None:
            print "saving menu",  url
            self.save_document(url, html)
         sleep(0.025)

      for job in errors:
         print "ERROR", job.get("result")

   @Impetus.shutdown
   def end(self, ready, errors, counter):

      print "bot completed!"


def parse_args(argv):
   '''
   Parses cmd line arguments
   '''

   opt_args= []

   optParser= OptionParser()
   [ optParser.add_option(opt) for opt in [
          make_option('-t', '--taskdir', default= path.join(pardir, 'tasks'), help= 'task directory'),
          make_option('-d', '--docdir', default= path.join(curdir, 'documents'), help= 'document directory'),
          make_option('-q', '--queue', default= 'localhost', help= 'ipaddr of queue to bind too'),
          make_option('-p', '--port', default= 50000, type= int, help= 'port of queue to bind too'),
          make_option('-a', '--authkey', default= "impetus", help= 'authorization key of queue')
   ]]

   optParser.set_usage('%%prog %s' % ('|'.join(opt_args)))

   (opts, args)= optParser.parse_args()

   return opts


if __name__ == '__main__':
 
   opts= parse_args(argv)
 
   yelp= Yelp(queue= (opts.queue, opts.port), authkey= opts.authkey, taskdir= opts.taskdir, docdir= opts.docdir)
   print "TaskId:", yelp.id

   yelp.run()

   print "TaskId:", yelp.id
 
