#!/usr/bin/env python

from Queue import Empty
from sys import stdout
from impq import Client


class MyBot(Client):

   def __init__(self, host, port, security_key, id= None):

      super(MyBot, self).__init__(host, port, security_key, id)

   @Client.node
   def pow(i):
  
      return i * i

   @Client.startup
   def start(self):

      print "starting up"
      for i in range(0, 10):
         self.fork(self.pow, args= i)

   @Client.process
   def add(self, ready, errors):

      for job_id in ready:
         print "processing", job.get(job_id)

   @Client.shutdown
   def stop(self):

      print "shutting down"


if __name__ == '__main__':

   bot= MyBot("localhost", 50000, "test")
   print "My Id:", bot.id
   bot.run()

