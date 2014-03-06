#!/usr/bin/env python
#-*- coding:utf-8 -*-
#---------------------------------------------------------------------------
# Author: Richard J. Marini (richardjmarini@gmail.com)
# Date: 2/4/2014
# Name: Node
# Desciption: Processing node for the Impetus Framework.
#
# License:
#    Impetus is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 2 of the License, or
#    any later version.
#
#    Impetus is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Impetus.  If not, see <http://www.gnu.org/licenses/>.
#---------------------------------------------------------------------------

from os import curdir, pardir, path
from optparse import OptionParser, make_option
from sys import stdout, stderr, argv
from impetus import Node

def parse_args(argv):

   opt_args= ['start', 'stop', 'restart', 'foreground']

   opt_parser= OptionParser()
   [ opt_parser.add_option(opt) for opt in [
      make_option("-q", "--queue", default= "localhost", help= "host of queue instance"),
      make_option("-p", "--qport", default= 50000, type= int, help= "port of queue instance"),
      make_option("-a", "--qauthkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-d", "--dfs", default= None, help= "host of dfs instance"),
      make_option("-o", "--dport", default= 50001, type= int, help= "port of queue instance"),
      make_option("-u", "--dauthkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-i", "--piddir", default= path.join(pardir, "pid"), help= "pid file directory"),
      make_option("-l", "--logdir", default= path.join(pardir, "log"), help= "log file directory"),
      make_option("-m", "--mpps", default= 5, type= int, help= "max number of processes per stream"),
      make_option("-s", "--streams", default= {}, help= "key/value pairs of stream properties, eg id:<stream_id>,frequency:<stream_frequency>, etc..")
   ]]

   opt_parser.set_usage("%%prog %s" % ("|".join(opt_args)))

   opts, args= opt_parser.parse_args()
   if len(args) == 0:
     opt_parser.print_usage()
     exit(-1)

   if opts.streams:
      setattr(opts, "streams", dict([pair.split(':') for pair in opts.streams.split(',')]))

   return args, opts, opt_parser.print_usage

if __name__ == "__main__":

   args, opts, usage= parse_args(argv)

   node= Node((opts.queue, opts.qport), opts.qauthkey, opts.mpps, (opts.dfs, opts.dport), opts.dauthkey, opts.logdir, opts.piddir, **opts.streams)

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


