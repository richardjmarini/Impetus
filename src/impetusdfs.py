#!/usr/bin/env python
#-*- coding:utf-8 -*-
#---------------------------------------------------------------------------
# Author: Richard J. Marini (richardjmarini@gmail.com)
# Date: 2/4/2014
# Name: DFS
# Desciption: Dynamic Frequency Scaling (Auto-Scaler) for the Impetus Framework
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

from os import curdir, pardir, path, getenv
from optparse import OptionParser, make_option
from sys import stdout, stderr, argv
from impetus import DFS

def parse_args(argv):

   opt_args= ['start', 'stop', 'restart', 'foreground']

   opt_parser= OptionParser()
   [ opt_parser.add_option(opt) for opt in [
      make_option("-d", "--dfs", default= "localhost", help= "address to bind dfs instance to"),
      make_option("-o", "--port", default= 50001, help= "port to bind dfs instance to"),
      make_option("-u", "--authkey", default= "impetus", help= "authorization key for dfs"),
      make_option("-q", "--queue", default= "localhost", help= "host of queue instance"),
      make_option("-p", "--qport", default= 50000, type= int, help= "port of queue instance"),
      make_option("-a", "--qauthkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-i", "--piddir", default= path.join(pardir, "pid"), help= "pid file directory"),
      make_option("-l", "--logdir", default= path.join(pardir, "log"), help= "log file directory"),
      make_option("-e", "--ec2", default= None, help= "<access key>,<secret key>,<ami-id>,<security group>,<key name>,<instance type>"),
      make_option("-n", "--mnon", default= 3, type=int, help= "max number of nodes dfs can start"),
      make_option("-m", "--mpps", default= 5, type=int, help= "max number of processes per stream"),
      make_option("-k", "--deploykey", default= path.join(getenv("HOME"), ".ssh", "impetus_rsa"), help= "deploy key file"),
      make_option("-b", "--bootstrap", default= path.join(curdir,"bootstrap.sh"), help= "bootstrap file")
   ]]

   opt_parser.set_usage("%%prog %s" % ("|".join(opt_args)))

   opts, args= opt_parser.parse_args()
   if len(args) == 0:
      opt_parser.print_usage()
      exit(-1)

   return args, opts, opt_parser.print_usage


if __name__ == "__main__":

   args, opts, usage= parse_args(argv)

   dfs= DFS((opts.dfs, opts.port), opts.authkey, (opts.queue, opts.qport), opts.qauthkey, opts.mnon, opts.mpps, opts.ec2, opts.bootstrap, opts.deploykey, opts.logdir, opts.piddir)
   if "start" in args:
      print "starting dfs in daemon mode"
      dfs.start()
   elif "foreground" in args:
      print "starting dfs in foreground mode"
      dfs.run()
   elif "restart" in args:
      print "restarting dfs daemon"
      dfs.restart()
   elif "stop" in args:
      print "stopping dfs daemon"
      dfs.stop()
   else:
      print >> stderr, "unkown argument"
      usage()
      exit(-1)

   exit(0)


