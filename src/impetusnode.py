#!/usr/bin/env python
#-*- coding:utf-8 -*-

from os import curdir, pardir, path
from optparse import OptionParser, make_option
from sys import stdout, stderr, argv
from impetus import Node

def parse_args(argv):

   opt_args= ['start', 'stop', 'restart', 'foreground']

   opt_parser= OptionParser()
   [ opt_parser.add_option(opt) for opt in [
      make_option("-q", "--queue", default= "localhost", help= "host of queue instance"),
      make_option("-p", "--port", default= 50000, type= int, help= "port of queue instance"),
      make_option("-a", "--authkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-i", "--piddir", default= path.join(pardir, "pid"), help= "pid file directory"),
      make_option("-l", "--logdir", default= path.join(pardir, "log"), help= "log file directory"),
      make_option("-m", "--mpps", default= 5, type= int, help= "max number of processes per stream")
   ]]

   opt_parser.set_usage("%%prog %s" % ("|".join(opt_args)))

   opts, args= opt_parser.parse_args()
   if len(args) == 0:
     opt_parser.print_usage()
     exit(-1)

   return args, opts, opt_parser.print_usage

if __name__ == "__main__":

   args, opts, usage= parse_args(argv)

   node= Node((opts.queue, opts.port), opts.authkey, opts.mpps, opts.logdir, opts.piddir)

   if "start" in args:
      print "starting node in daemon mode"
      node.start()
   elif "foreground" in args:
      print "starting node in foreground mode"
      node.run()
   elif "restart" in args:
      print "restarting node daemon"
      node.restart()
   elif "stop" in args:
      print "stopping node daemon"
      node.stop()
   else:
      print >> stderr, "unkown argument"
      usage()
      exit(-1)
  
   exit(0)


