#!/usr/bin/env python
#-*- coding:utf-8 -*-
#---------------------------------------------------------------------------
# Author: Richard J. Marini (richardjmarini@gmail.com)
# Date: 2/4/2014
# Name: Queue
# Desciption: Queue for the Impetus frakework.
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
from impetus import Queue

def parse_args(argv):

   opt_args= ['start', 'stop', 'restart', 'foreground']

   opt_parser= OptionParser()
   [ opt_parser.add_option(opt) for opt in [
      make_option("-q", "--queue", default= "0.0.0.0", help= "address to bind queue instance to"),
      make_option("-p", "--port", default= 50000, type= int, help= "port to bind queue instance to"),
      make_option("-a", "--authkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-i", "--piddir", default= path.join(pardir, "pid"), help= "pid file directory"),
      make_option("-l", "--logdir", default= path.join(pardir, "log"), help= "log file directory")
   ]]

   opt_parser.set_usage("%%prog %s" % ("|".join(opt_args)))

   opts, args= opt_parser.parse_args()
   if len(args) == 0:
      opt_parser.print_usage()
      exit(-1)

   return args, opts, opt_parser.print_usage


if __name__ == "__main__":

   args, opts, usage= parse_args(argv)

   queue= Queue((opts.queue, opts.port), opts.authkey, opts.logdir, opts.piddir)
   if "start" in args:
      print "starting queue in daemon mode"
      queue.start()
   elif "foreground" in args:
      print "starting queue in foreground mode"
      queue.run()
   elif "restart" in args:
      print "restarting queue daemon"
      queue.restart()
   elif "stop" in args:
      print "stopping queue daemon"
      queue.stop()
   else:
      print >> stderr, "unkown argument"
      usage()
      exit(-1)

   exit(0)


