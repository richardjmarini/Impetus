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
      #"/search?find_desc=restaurants&find_loc=Manhattan%2C+NY",
      "/search?find_desc=restaurants&find_loc=San%20Francisco+CA",
      #"/search?find_desc=restaurants&find_loc=Los%20Angeles+CA",
      #"/search?find_desc=restaurants&find_loc=Chicago%2C+IL"
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

      from urllib2 import urlopen, HTTPRedirectHandler, build_opener, Request, build_opener, Request
      from urllib import urlencode
      from re import match

      def get_location_replace(html):

         for line in html.split("\n"):
            if "location.replace" in line:
               m= match(".*location\.replace\(\"(.*)\"\).*", str(line))
               return m.group(1)


      response= urlopen(url, timeout= 300)
      html= response.read()

      if 'location.replace' in html:
         url= get_location_replace(html)
         response= urlopen(url, timeout= 300)
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
      from urllib2 import urlopen
      from urllib import urlencode

      parser= BeautifulSoup(html, "html").find_all("a", class_= "page-option prev-next")
      url= None
      response= None
      for prevnext in parser:
         url= prevnext["href"]
         print "FOUND", url
      print "USING", url
      request= urlopen('http://yelp.com' + url, timeout= 300)
      response= request.read()

      return (url, response)

   @Impetus.node
   def fetch_menu(html):

      from bs4 import BeautifulSoup
      from urllib2 import urlopen
      from urllib import urlencode

      parser= BeautifulSoup(html, "html").find("a", class_= "menu-explore")
      if parser:
         url= parser["href"]
         request= urlopen('http://yelp.com' + url, timeout= 300)
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
         self.fork(self.fetch_page, self.domain + url, delay= randint(5, 15))
         sleep(0.025)   

   @Impetus.process
   def extract_links(self, ready, errors):

      for job in ready:
         (url, html)= job.get("result")
         if html != None and self.visited(url) == False:
            print "extract links", url

            self.save_document(url, html)
            self.fork(self.fetch_next_url, html, callback= "extract_links", delay= randint(5, 15))
            self.fork(self.get_listing_urls, html, callback= "fetch_profiles")
            sleep(0.025)   

   @Impetus.process
   def fetch_profiles(self, ready, errors):
         print "in fetch profiles"
         for job in ready:
            for url in job.get("result"):
               print "fetch profile", url
               self.fork(self.fetch_page, self.domain + url, callback= "process_profiles", delay= randint(5, 15))
               sleep(0.025)

   @Impetus.process
   def process_profiles(self, ready, errors):
         print "in process profiles"
         for job in ready:
            (url, html)= job.get("result")
            if html != None:
               print "saving profile", url
               self.save_document(url, html)
               print "profile page", url, len(html)
               self.fork(self.fetch_menu, html, callback= "process_menus", delay= randtime(5, 15))
               sleep(0.025)

   @Impetus.process
   def process_menus(self, ready, errors):
         print "in process menus"
         for job in ready:
            (url, html)= job.get("result")
            if html != None:
               print "saving menu",  url
               self.save_document(url, html)

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
 
