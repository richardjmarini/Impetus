#!/usr/bin/env python

from Queue import Empty
from sys import stdout
from impq import Client
from time import sleep

class MyBot(Client):

   def __init__(self, host, port, security_key, id= None):

      super(MyBot, self).__init__(host, port, security_key, id)

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

if __name__ == '__main__':

   bot= MyBot("localhost", 50000, "test")
   print "My Id:", bot.id
   bot.run()

