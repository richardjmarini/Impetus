#!/usr/bin/env python

from os import curdir, pardir, path
from sys import stdout, stderr, argv
from optparse import OptionParser, make_option
from impetus import Impetus
from itertools import izip

class MarketPosition(Impetus):

   crawl_file= "crawl_list.tab"

   document_types= {
      'search': '/search',
      'profile': 'http://www.yelp.com/biz',
      'menu': '/menu'
    }

   def __init__(self, address, authkey, taskdir= curdir, docdir= "documents", id= None, **properties):

      self.docdir= docdir
      self.address= address
      super(MarketPosition, self).__init__(self.address, authkey, taskdir, id, **properties)
    
      self.counter= dict(izip(self.document_types.keys(), [0] * len(self.document_types)))

   @Impetus.node
   def categorize(args):
   
      (index, document_types)= args

      for key, pattern in document_types.items():
         if pattern in index.get('url'):
            index['type']= key
            break

      return index 
      
   @Impetus.startup
   def start(self):
   
      fh= open(path.join(self.docdir, self.crawl_file), "r")
      for index in fh.readlines():
         index= index[:-1]
         self.fork(self.categorize, args= (dict(izip(('document_id', 'url'), index.split())), self.document_types))

   @Impetus.process
   def process_index(self, ready, errors):
      for job in ready:
         index= job.get("result")
         self.counter[index.get("type")]+= 1
         print job

   @Impetus.shutdown
   def stop(self, ready, errors, progress):
      print self.counter
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

   market_position= MarketPosition((opts.queue, opts.port), opts.authkey, taskdir= opts.authkey, docdir= opts.docdir)
   print "My Task Id:", market_position.id
   market_position.run()
