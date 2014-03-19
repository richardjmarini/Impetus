#!/usr/bin/env python

from os import curdir, pardir, path
from sys import stdout, stderr, argv
from optparse import OptionParser, make_option
from impetus import Impetus
from itertools import izip
from base64 import b64encode

class MarketPosition(Impetus):

   crawl_file= "crawl_list.tab"

   document_types= {
      'search': '/search',
      'profile': 'http://www.yelp.com/biz',
      'menu': '/menu'
   }
   
   attribution_types= [
       {'tag': 'img', 'class': 'menu-provider-attribution'},
       {'tag': 'p', 'class': 'business-provided-menu'}
   ]

   def __init__(self, address, authkey, taskdir= curdir, docdir= "documents", id= None, **properties):

      self.docdir= docdir
      self.address= address
      super(MarketPosition, self).__init__(self.address, authkey, taskdir, id, **properties)
    
      self.dtctr= dict(izip(self.document_types.keys(), [0] * len(self.document_types)))
      self.pctr= {}
      self.locctr= {}

   @Impetus.node
   def categorize(args):
   
      (index, document_types)= args

      for key, pattern in document_types.items():
         if pattern in index.get('url'):
            index['type']= key
            break

      return index 

   @Impetus.node
   def profile(args):

      from bs4 import BeautifulSoup
      from zlib import decompress
      from base64 import b64decode

      (index, html)= args
      html= b64decode(html)

      parser= BeautifulSoup(unicode(decompress(html), errors= 'ignore').encode('ascii'), "html")

      location= parser.find("span", itemprop= "addressRegion")
      index["location"]= location.getText()

      menu= parser.find("a", class_= "menu-explore")
      index["menu"]= menu.attrs.get("href") if menu else None

      return index

   @Impetus.node
   def attribution(args):

      from bs4 import BeautifulSoup
      from zlib import decompress
      from base64 import b64decode

      (index, attribution_types, html)= args
      html= b64decode(html)
      provider= "No Provider or Non-Yelp-Menu"

      parser= BeautifulSoup(unicode(decompress(html), errors= 'ignore').encode('ascii'), "html")
      for attribution_type in attribution_types:
         attribution= parser.find(attribution_type.get("tag"), class_= attribution_type.get("class"))
         if attribution:
            provider= attribution.attrs.get("alt") if attribution.name == "img" else "Business Provided"
            break

      index["provider"]= provider 
      return index
      
   @Impetus.startup
   def start(self):
   
      fh= open(path.join(self.docdir, self.crawl_file), "r")
      for index in fh.readlines():
         index= index[:-1]
         self.fork(self.categorize, args= (dict(izip(('document_id', 'url'), index.split())), self.document_types))

   @Impetus.process
   def process_index(self, ready, errors):

      if len(errors):
         print "WARNING!!!!!!!!!!", len(errors), "errors"

      for job in ready:
         index= job.get("result")
         self.dtctr[index.get("type")]+= 1
         if index.get("type") == "profile":
            fh= open(path.join(self.docdir, "%s.doc" % index.get("document_id")), "r")
            self.fork(self.profile, args= (index, b64encode(fh.read())))

   @Impetus.process
   def process_profile(self, ready, errors):

      if len(errors):
         print "WARNING!!!!!!!!!!", len(errors), "errors"

      for job in ready:
         index= job.get("result")

         try:
            self.locctr[index.get("location")]+= 1
         except KeyError:
            self.locctr[index.get("location")]= 1
 
         if index.get("menu"):
            menu= None
            fh= open(path.join(self.docdir, self.crawl_file), "r")
            for line in fh.readlines():
               if index.get("menu") in line:
                  menu= line
                  break

            if menu:
               (document_id, url)= menu.split()

               fh= open(path.join(self.docdir, "%s.doc" % document_id), "r")
               self.fork(self.attribution, args= (index, self.attribution_types, b64encode(fh.read())))
               fh.close()

   @Impetus.process
   def process_attribution(self, ready, errors):

      if len(errors):
         print "WARNING!!!!!!!!!!", len(errors), "errors"

      for job in ready:
         index= job.get("result")
         try:
            self.pctr[index.get("provider")]+= 1
         except KeyError:
            self.pctr[index.get("provider")]= 1

   @Impetus.shutdown
   def stop(self, ready, errors, progress):

      if len(errors):
         print "WARNING!!!!!!!!!!", len(errors), "errors"

      print "========================================"
      print "Document Types:"
      for key, val in self.dtctr.items():
         print "\t", key, val, (val / float(sum(self.dtctr.values()))) * 100
      print "\t", "Total", sum(self.dtctr.values())
      print
      print "Locations:"
      for key, val in self.locctr.items():
         print "\t", key, val, (val / float(self.dtctr["profile"])) * 100
      print "\t", "Total", sum(self.locctr.values())
      print
      print "Providers:"
      for key, val in self.pctr.items():
         print "\t", key, val, (val / float(self.dtctr['menu'])) * 100
      print "\t", "Total", sum(self.pctr.values())
      print
       

      print "All Complete!", self.id

def parse_args(argv):

   opt_args= []

   opt_parser= OptionParser()
   [ opt_parser.add_option(opt) for opt in [
      make_option("-d", "--docdir", default= path.join(pardir, "documents"), help= "documents directory"),
      make_option("-l", "--location", default= "*", help= "location to analyze defaults to '*' for all"),
      make_option("-q", "--queue", default= "localhost", help= "host of queue instance"),
      make_option("-p", "--port", default= 50000, type= int, help= "port of queue instance"),
      make_option("-a", "--authkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-t", "--taskdir", default= path.join(pardir, "tasks"), help= "task directory"),

   ]]

   opts, args= opt_parser.parse_args()

   return args, opts, opt_parser.print_usage

if __name__ == '__main__':

   args, opts, usage= parse_args(argv)

   market_position= MarketPosition((opts.queue, opts.port), opts.authkey, taskdir= opts.taskdir, docdir= opts.docdir)
   print "My Task Id:", market_position.id
   market_position.run()
