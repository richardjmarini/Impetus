#!/usr/bin/env python

from Queue import Empty
from sys import stdout
from impq import Client


class MyBot(Client):

   def __init__(self, host, port, security_key, id= None):

      super(MyBot, self).__init__(host, port, security_key, id)

   @Client.node
   def pow(i):
      print "GOT IIIIIII", i
      return i * i

   @Client.startup
   def start(self):

      print "forking up"
      for i in range(0, 10):
         print "forking pow with", i
         self.fork(self.pow, args= i)
      print "fork complete"

   @Client.process
   def stage1(self, ready, errors):
      print "processing", len(ready), len(errors)
      for job in ready:
         print job

      for job in errors:
         print job

   @Client.shutdown
   def stop(self, progress):

      print "shutting down"


if __name__ == '__main__':

   bot= MyBot("localhost", 50000, "test")
   print "My Id:", bot.id
   bot.run()

