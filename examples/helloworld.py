#!/usr/bin/env python
#-*- coding:utf-8 -*-

#---------------------------------------------------------------------------
# Author: Richard J. Marini (richardjmarini@gmail.com)
# Date: 2/4/2014
# Name: Helloworld
# Desciption: 
#    Sample of how to write a client application using the Impetus Framework.
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

from optparse import OptionParser, make_option
from os import curdir, pardir, path
from sys import stdout, stderr, argv
from impetus import Impetus
from time import sleep

class Helloworld(Impetus):

   def __init__(self, address, authkey, taskdir= curdir, id= None, **properties):

      self.address= address
      super(Helloworld, self).__init__(self.address, authkey, taskdir, id, **properties)

   @Impetus.node
   def pow(i):
      
      return i * i

   @Impetus.startup
   def start(self):

      for i in range(0, 1000):
         self.fork(self.pow, args= i)
         

   @Impetus.process
   def stage1(self, ready, errors):

      total= 0
      for job in ready:
         total+= job.get('result')
         sleep(0.025)
 
      print "Total:", total
      print "Errors:", len(errors)

   @Impetus.shutdown
   def stop(self, ready, errors, progress):

      print "shutting down"


def parse_args(argv):

   opt_args= ['start', 'stop', 'restart', 'foreground']

   opt_parser= OptionParser()
   [ opt_parser.add_option(opt) for opt in [
      make_option("-q", "--queue", default= "localhost", help= "host of queue instance"),
      make_option("-p", "--port", default= 50000, type= int, help= "port of queue instance"),
      make_option("-a", "--authkey", default= "impetus", help= "authorization key for queue instance"),
      make_option("-t", "--taskdir", default= path.join(pardir, "tasks"), help= "task directory"),
      make_option("-r", "--properties", default= {}, help= "key/valye pairs of stream properties, eg id:<stream_id>,freuqnecy:<stream_frequency>, etc...")
   ]]

   opt_parser.set_usage("%%prog %s" % ("|".join(opt_args)))

   opts, args= opt_parser.parse_args()

   if opts.properties:
      setattr(opts, "properties", dict([pair.split(':') for pair in opts.properties.split(',')]))

   return args, opts, opt_parser.print_usage

if __name__ == '__main__':

   args, opts, usage= parse_args(argv)

   helloworld= Helloworld((opts.queue, opts.port), opts.authkey, opts.taskdir, **opts.properties)

   print "My Id:", helloworld.id
   helloworld.run()

