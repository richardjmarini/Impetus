#!/usr/bin/env python
#-*- coding:utf-8 -*-

from optparse import OptionParser, make_option
from os import curdir, pardir, path
from sys import stdout, stderr, argv
from impetus import Client
from time import sleep

class Helloworld(Client):

   def __init__(self, address, authkey, taskdir= curdir, id= None):

      self.address= address
      super(Helloworld, self).__init__(self.address, authkey, taskdir, id)

   @Client.node
   def pow(i):
      return i * i

   @Client.startup
   def start(self):

      for i in range(0, 100):
         self.fork(self.pow, args= i)
         sleep(0.25)

   @Client.process
   def stage1(self, ready, errors):

      total= 0
      for job in ready:
         total+= job.get('result')
         sleep(0.24)
 
      print "Total:", total
      print "Errors:", len(errors)

   @Client.shutdown
   def stop(self, ready, errors, progress):

      print "shutting down"


def parse_args(argv):

   opt_args= ['start', 'stop', 'restart', 'foreground']

   opt_parser= OptionParser()
   [ opt_parser.add_option(opt) for opt in [
      make_option("-q", "--queue", default= "localhost", help= "host of queue instance"),
      make_option("-p", "--port", default= 50000, type= int, help= "port of queue instance"),
      make_option("-a", "--authkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-t", "--taskdir", default= path.join(pardir, "tasks"), help= "task directory")
   ]]

   opt_parser.set_usage("%%prog %s" % ("|".join(opt_args)))

   opts, args= opt_parser.parse_args()

   return args, opts, opt_parser.print_usage

if __name__ == '__main__':

   args, opts, usage= parse_args(argv)

   helloworld= Helloworld((opts.queue, opts.port), opts.authkey, opts.taskdir)

   print "My Id:", helloworld.id
   helloworld.run()

