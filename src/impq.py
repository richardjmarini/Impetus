#!/usr/bin/env python

from copy import deepcopy
from os import path, makedirs
from sys import stdout, stderr, exc_info, exit
from multiprocessing import Process
from multiprocessing.managers import SyncManager, DictProxy, BaseProxy
from uuid import uuid1
from datetime import datetime
from itertools import izip
from time import sleep
from Queue import PriorityQueue , Empty
from marshal import dumps, loads
from json import dumps as json_dumps
from types import FunctionType
from threading import Thread, Lock, currentThread
from codecs import open as utf8open
from traceback import extract_tb
from socket import error as SocketError

class Autovivification(object):

   def __init__(self, **properties):

      for pname, pvalue in properties.items():

         if type(pvalue) == dict:
            setattr(self, pname, Autovivification(**pvalue))

         elif type(pvalue) in (list, tuple):
            setattr(self, pname, [Autovivification(**element) if type(element) == dict else element for element in pvalue])

         else:
            setattr(self, pname, pvalue)

class Channel(object):
 
   def __init__(self, **properties):

      super(Channel, self).__init__()

      self.properties= properties
      self.stream= PriorityQueue()
      self.store= {}

class Job(dict):

   def __init__(self, **kwargs):

      if not kwargs.get("id"):
         kwargs["id"]= str(uuid1())

      if not kwargs.get("created"):
         kwargs["created"]= datetime.utcnow()

      if not kwargs.get("status"):
         kwargs["status"]= "waiting"

      super(Job, self).__init__(**kwargs)

class Queue(object):

   def __init__(self, host, port, security_key):

      super(Queue, self).__init__()

      self.channels= {}

      self.manager= SyncManager(address= (host, int(port)), authkey= security_key)
      self.manager.register("create_channel", callable= self.create_channel)
      self.manager.register("delete_channel", callable= self.delete_channel)
      self.manager.register("get_channels", callable=  lambda: self.channels, proxytype= DictProxy)
      self.manager.register("get_store", callable=  lambda channel_id: self.channels[channel_id].store, proxytype= DictProxy)
      self.manager.register("get_stream", callable=  lambda channel_id: self.channels[channel_id].stream, proxytype= PriorityQueue)

   def create_channel(self, **properties):

      channel= Channel(**properties)
      channel_id= properties.get("channel_id")
      if not channel_id:
         channel_id= uuid1()

      self.channels[channel_id]= channel

   def delete_channel(self, **properties):

      channel_id= properties.get("channel_id")
      del self.channels[channel_id]

   def run(self):

      server= self.manager.get_server()
      print "running"
      server.serve_forever()


_thread_order= 0
class Client(object):

   statuses= ("forked", "processed")

   def __init__(self, host, port, security_key, id= None, task_dir= "tasks"):

      self.id= id if id else str(uuid1())
      self.task_dir= path.join(task_dir, self.id)

      self.impq= SyncManager(address= (host, port), authkey= security_key)
      self.impq.register("get_channels")
      self.impq.register("create_channel")
      self.impq.register("delete_channel")
      self.impq.register("get_store")
      self.impq.register("get_stream")
      self.impq.register("jobs")
      self.impq.connect()

      self.jobs= []
      self.impq.create_channel(channel_id= self.id)
      self.store= self.impq.get_store(channel_id= self.id)
      self.stream= self.impq.get_stream(channel_id= self.id)
      self.alive= True
      self._current_thread= None
      self._lock= Lock()
      self.threads= []
      self.errors= {}
      self.ready= {}
      self._progress= {}


      try:
         makedirs(self.task_dir)
      except:
         pass

   def __del__(self):

      self.impq.delete_channel(channel_id= self.id)

   @staticmethod
   def node(method):

      return method

   @staticmethod
   def startup(process):

      def _process(self):

         process(self)

      global _thread_order
      _process.order= _thread_order
      _thread_order+= 1
      return _process

   @staticmethod
   def shutdown(shutdown):
   
      def _shutdown(self):

         shutdown(self, self.ready, self.errors, self._progress)

      global _thread_order
      _shutdown.order= _thread_order
      return _shutdown

   @staticmethod
   def process(process):

      def _process(self):

         current_thread= currentThread()
         if current_thread.name == 'MainThread':
            return
         previous_thread= current_thread.previous_thread

         while self.alive:

            self._thread_regulator(current_thread, previous_thread)

            ready= []
            errors= []
            for job in self.store.values():
               if job.get("status") == "ready":
                  ready.append(job)
               elif job.get("status") == "error":
                  errors.append(job)
               else:
                  continue

               #print "killing", job.get("id")
               self.store.pop(job.get("id"))

            if len(ready) or len(errors):
               process(self, ready, errors)

            self._thread_progress(current_thread.name, "processed", len(ready) + len(errors))
            self._show_progress(current_thread)

            if len(self.store) == 0 and previous_thread != None and previous_thread.is_alive() == False:
               print "%s %s completed" % (datetime.utcnow(), current_thread.name)
               stdout.flush()
               self.alive= False

            sleep(0.01)
         
      global _thread_order
      _process.order= _thread_order
      _thread_order+= 1
       
      return _process

   def fork(self, method, args, callback= None, priority= None, job_id= None):

      current_thread= currentThread()
      
      job= Job(
         client= self.id,
         name= method.func_name,
         code= dumps(method.func_code),
         args= args,
         callback= callback.func_name if callback else current_thread.next_thread.name,
         result= None,
         transport= None
      )
      
      if priority:
         setattr(job, "priority", priority)

      self.store.update([(job.get("id"), job)])
      self.stream.put([(job.get("priority"), job.get("id"))])
  
      #print "forked", len(self.store)
      
      self.jobs.append(job.get("id"))
      
      self._thread_progress(current_thread.name, "forked", 1)
      
      return job.get("id")

   def _thread_progress(self, name, status, count):

      with self._lock:
 
         progress= self._progress.get(name, dict([(s, 0) for s in self.statuses]))
         progress.update([(status, progress.get(status, 0) + count)])
         self._progress.update([(name, progress)])

   def _show_progress(self, current_thread):
 
      msg= []
      with self._lock:
         for thread in self.threads:
            progress= self._progress.get(thread.name, dict([(s, 0) for s in self.statuses]))
            msg.append("%s %s/%s -> " % (thread.name, progress.get("forked"), progress.get("processed")))

      print "thread: %s via %s" % (''.join(msg)[:-4], current_thread.name)
         
   def _thread_regulator(self, current_thread, previous_thread):

      stall_time= 1
      while self._current_thread == current_thread:
         #print "stalling:", current_thread.name, stall_time
         sleep(stall_time)
         stall_time+= 1
         if stall_time >= 10:
            break

         if current_thread.name == self.threads[-1].name and previous_thread != None and previous_thread.is_alive() == False:
            with self._lock:
               self._current_thread= self.threads[0]

      with self._lock:
         #print "setting current thread", current_thread.name
         self._current_thread= current_thread

   def _create_thread(self, name, method):

      
      thread= Thread(target= method, name= name, args= (self, ))
      self.errors[name]= utf8open(path.join(self.task_dir, '.'.join((name, "err"))), 'ab+')
      self.ready[name]=  utf8open(path.join(self.task_dir, '.'.join((name, "ok"))), 'ab+')

      return thread
 
   def _link_threads(self, threads):
 
      for i in range(len(threads)):
 
        setattr(threads[i], "previous_thread", threads[i-1] if i > 0 else None)
        setattr(threads[i], "next_thread", threads[i+1] if i < len(threads)-1 else None)
  
      return threads[0]

   def _start_threads(self, threads):

      [thread.start() for thread in threads]
      [thread.join() for thread in threads]

   def run(self):

      self.threads= [self._create_thread(name, method) for (name, method) in sorted(filter(lambda (name, method): type(method) == FunctionType and method.__name__ == "_process", self.__class__.__dict__.items()), key= lambda (name, method): method.order)]
      self._current_thread= self._link_threads(self.threads)
      self._start_threads(self.threads)

      [method(self) for (name, method) in sorted(filter(lambda (name, method): type(method) == FunctionType and method.__name__ == "_shutdown", self.__class__.__dict__.items()), key= lambda (name, method): method.order)]



class Worker(Process):

   statuses= ("idle", "busy")

   def __init__(self, channel_id, stream, store):

      super(Worker, self).__init__()

      self.alive= True
      self.status= "idle"
      self.channel_id= channel_id
      self.stream= stream
      self.store= store

   def process(self):

      try:

         print "processing channel", self.channel_id
         (priority, job_id)= self.stream.get(block= True).pop()
         self.status= "busy"

         job= self.store.get(job_id)
         job.update([("status", "processing")])
         self.store.update([(job.get("id"), job)])

         print "processing job (%s): %s, %s, %s" % (self.pid, job.get("id"), job.get("name"), job.get("status"))

         method= FunctionType(loads(job.get("code")), globals(), job.get("name"))
         result= method(job.get("args"))
         job.update([("result", result), ("status", "ready")])
         self.store.update([(job.get("id"), job)])

         print "completed job (%s): %s, %s, %s" % (self.pid, job.get("id"), job.get("name"), job.get("status"))
      except Exception, e:

         (filename, linenumber, functionname, statement)= extract_tb(exc_info()[2])[-1]
         result= {"error": str(e), "name": functionname, "linenumber": linenumber, "statement": statement}

         print >> stderr, "error processing job:", self.pid, job.get("id"), job.get("name"), job.get("status"), str(e), functionname, linenumber, statement

         job.update([("result", result), ("status", "error")])
         self.store.update([(job.get("id"), job)])

      self.status= "idle"

   def run(self):

      while self.alive:

         try:
            self.process()
         except (UnboundLocalError, EOFError, IOError, SocketError) as e:
            print >> stderr, "worker communication error:", self.channel_id, str(e)
            self.status= "idle"
            self.alive=  False

         sleep(0.01)


class Node(object):

   def __init__(self, host, port, security_key, max_processes= 5):

      self.host= host
      self.port= port
      self.security_key= security_key
      self.max_processes= max_processes

      self.alive= True
      self.connect()

   def connect(self):

      # remove connection from cache:
      # BaseProxy class has thread local storage which caches the connection
      # which is reused for future connections causing "borken pipe" errors on 
      # creating new manager.  
      if (self.host, int(self.port)) in BaseProxy._address_to_local:
         del BaseProxy._address_to_local[(self.host, int(self.port))][0].connection

      # register handlers
      SyncManager.register("get_channels")
      SyncManager.register("get_stream")
      SyncManager.register("get_store")

      while self.alive:

         try:
            self.impq= SyncManager(address= (self.host, int(self.port)), authkey= self.security_key)
            self.impq.connect() 
            break
         except (EOFError, IOError, SocketError) as e:
            print "could not connect ...trying again", str(e)
            sleep(1)

   def update_streams(self, channels, streams):

      # update list of streams to track
      for channel_id in channels.keys():
         if channel_id not in streams.keys():
            print "tracking channel", channel_id
            streams.update([(channel_id, (self.impq.get_stream(channel_id), self.impq.get_store(channel_id)))])
          
      for channel_id in streams.keys():
         if channel_id not in channels.keys():
            print 'stopping tracking channel', channel_id
            streams.pop(channel_id)

   def process(self):

      print "started", self.max_processes

      workers= {}
      channels= self.impq.get_channels()
      streams= dict([(channel_id, (self.impq.get_stream(channel_id), self.impq.get_store(channel_id))) for channel_id in channels.keys()])

      while self.alive:

         self.update_streams(channels, streams)

         # stop tracking dead workers
         for (channel_id, worker) in workers.items():
            if not worker.is_alive():
               #print "dead", channel_id
               workers.pop(channel_id)

         #print "creating workers %s/%s/%s" % (len(streams.keys()), len(workers), self.max_processes)
         for (channel_id, (stream, store)) in streams.items():
            if channel_id not in workers.keys():
               worker= Worker(channel_id, stream, store)
               worker.start()
               workers.update([(channel_id, worker)])

      # wait for workers to finish before shutting down
      for (channel_id, worker) in workers.items():
         print "waiting for worker", channel_id
         worker.join()
 
      print "shutdown complete."

   def run(self):

      while self.alive:
         try:
            self.process()
         except Exception, e:
            print >> stderr, "node communication error:", str(e)
            self.connect()
         sleep(0.01)


if __name__ == "__main__":
   q= Queue("localhost", 50000, "test")
   q.run()
