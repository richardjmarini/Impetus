#!/usr/bin/env python

from os import path, makedirs
from sys import stdout, stderr, exc_info
from multiprocessing import Process
from multiprocessing.managers import SyncManager, DictProxy, BaseProxy
from uuid import uuid1
from datetime import datetime
from itertools import izip
from time import sleep
from Queue import PriorityQueue , Empty
from marshal import dumps, loads
from types import FunctionType
from threading import Thread, Lock, currentThread
from codecs import open as utf8open
from traceback import extract_tb

class Autovivification(object):

   def __init__(self, **properties):

      for pname, pvalue in properties.items():

         if type(pvalue) == dict:
            setattr(self, pname, Autovivification(**pvalue))

         elif type(pvalue) in (list, tuple):
            setattr(self, pname, [Autovivification(**element) if type(element) == dict else element for element in pvalue])

         else:
            setattr(self, pname, pvalue)

class Status(object):

   def __init__(self, *statuses):

      super(Status, self).__init__()
      self._statuses= statuses

      for id, label in enumerate(self._statuses):
         setattr(self, label, id)

      self.current= 0

   def get(self, status= None):

      if status == None:
         return self.current_status

      statuses= enumerate(self._statuses)
      if status in self._statuses:
         statuses= map(reversed, statuses)

      return dict(statuses).get(status)

   def flags(self):

      return self._statuses

   def __iter__(self):

      return self

   def next(self):
      if self.current >= len(self._statuses):
         self.current= len(self._statuses)-1

      self.current+= 1
      return self._statuses[self.current-1]

   def reset(self):

      self.current= 0

   def previous(self):

      self.current-= 1
      if self.current < 0:
         self.current= 0

      return self.current

   def __repr__(self):

      return repr(self.current)


class Job(Autovivification):

   statuses= ("waiting", "processing", "ready", "error")

   def __init__(self, **kwargs):

      id= kwargs.get("id")
      if not id:
         id= str(uuid1())
      else:
         del kwargs["id"]

      created= kwargs.get("created")
      if not created:
         created= datetime.utcnow()

      status= Status(*self.statuses)

      kwargs.update([("id", id), ("created", created), ("status", status)]) #, ("args", args)])

      super(Job, self).__init__(**kwargs)

   def promote(self, status= None):

      if status:
         while self.status.current != self.status.get(status):
            self.status.next()
      else:
         self.status.next()

   def reset(self):

      self.status.reset()

   def demote(self, status= None):

      if status:
         while self.status.current != self.status.get(status):
            self.status.previous()
      else:
         self.status.previous()

   def update(self, attributes):

      for (name, value) in attributes:
         setattr(self, name, value)

   def get(self, attribute, default= None):

      return getattr(self, attribute) if hasattr(self, attribute) else default

   def __repr__(self):

      return repr(self.__dict__)


class Channel(object):
 
   def __init__(self, **properties):

      super(Channel, self).__init__()

      self.properties= properties
      self.stream= PriorityQueue()
      self.store= {}

class Queue(object):

   def __init__(self, host, port, security_key):

      super(Queue, self).__init__()

      self.channels= {}

      self.manager= SyncManager(address= (host, int(port)), authkey= security_key)
      self.manager.register("create_channel", callable= self.create_channel)
      self.manager.register("delete_channel", callable= self.delete_channel)
      self.manager.register("put", callable= self.put)
      self.manager.register("kill", callable= self.kill)
      self.manager.register("get", callable= self.get, proxytype= Job)
      self.manager.register("jobs", callable= self.jobs, proxytype= DictProxy)
      self.manager.register("task_done", callable= self.task_done)
      self.manager.register("get_channels", callable= self.get_channels, proxytype= DictProxy)

   def create_channel(self, **properties):

      channel= Channel(**properties)
      channel_id= properties.get("channel_id")
      if not channel_id:
         channel_id= uuid1()

      self.channels[channel_id]= channel

      return channel_id

   def delete_channel(self, channel_id):  

      del self.channels[channel_id]

   def get_channels(self):

      channels= [channel_id for channel_id in self.channels.keys()]

      return channels


   def put(self, job, channel_id= None):

      if not channel_id:
         channel_id= uuid1()

      if channel_id not in self.channels:
         print >> stderr, "warning: creating non-existent channel [%s]" % (channel_id)
         self.create_channel(channel_id= channel_id)
 
      self.channels[channel_id].store.update([(job.get("id"), job)])
      self.channels[channel_id].stream.put([(job.get("priority"), job.get("id"))])

      return channel_id

   def get(self, channel_id, block= False):

      if channel_id not in self.channels:
         raise Exception("invalid channel_id [%s]" % (channel_id))

      try:
         
         (job_priority, job_id)= self.channels[channel_id].stream.get(block).pop()
         job= self.channels[channel_id].store.get(job_id)
         print "job picked up: ", job.get("id")
      except Empty:
         job= None

      return job

   def kill(self, job_id, channel_id):

      self.channels[channel_id].store.pop(job_id)

   def jobs(self, channel_id):

      if channel_id not in self.channels:
         raise Exception("invalid channel_id [%s]" % (channel_id))

      store= self.channels[channel_id].store

      return store

   def task_done(self, job, channel_id):

      if channel_id not in self.channels:
         raise Exception("invalid channel_id [%s]" % (channel_id))

      self.channels[channel_id].store.update([(job.get("id"), job)])
      self.channels[channel_id].stream.task_done()

   def run(self):

      server= self.manager.get_server()
      print "running"
      server.serve_forever()


_thread_order= 0
class Client(object):

   def __init__(self, host, port, security_key, id= None, task_dir= "tasks"):

      self.id= id if id else str(uuid1())
      self.task_dir= path.join(task_dir, self.id)
      self.status= Status("forked", "processed")

      self.impq= SyncManager(address= (host, port), authkey= security_key)
      self.impq.register("create_channel")
      self.impq.register("delete_channel")
      self.impq.register("put")
      self.impq.register("kill")
      self.impq.register("jobs")
      self.impq.connect()
      self.impq.create_channel(channel_id= self.id)

      self.jobs= []
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

         shutdown(selfs, self.progress)

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
         status= Status(*Job.statuses)

         while self.alive:

            self._thread_regulator(current_thread, previous_thread)

            jobs= self.impq.jobs(self.id)
            ready= []
            errors= []
            for job_id, job in jobs.items():
               if job.status.current == status.get("ready"):
                  ready.append(job)
               elif job.status.current == status.get("error"):
                  errors.append(job)
               else:
                  continue

               #print "killing", job_id
               self.kill(job_id)

            if len(ready) or len(errors):
               process(self, ready, errors)

            self._thread_progress(current_thread.name, "processed", len(ready) + len(errors))
            self._show_progress(current_thread)

            if len(jobs) == 0 and previous_thread != None and previous_thread.is_alive() == False:
               print "%s %s completed" % (datetime.utcnow(), current_thread.name)
               stdout.flush()
               self.alive= False

            sleep(0.1)
         
      global _thread_order
      _process.order= _thread_order
      _thread_order+= 1
       
      return _process

   def fork(self, method, args, callback= None, priority= None):

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

      self.impq.put(job, channel_id= self.id)
      self.jobs.append(job.get("id"))

      self._thread_progress(current_thread.name, "forked", 1)

      return job.get("id")

   def kill(self, job_id):

      self.impq.kill(job_id, channel_id= self.id)


   def _thread_progress(self, name, status, count):

      with self._lock:
 
         progress= self._progress.get(name, dict([(s, 0) for s in self.status.flags()]))
         progress.update([(status, progress.get(status, 0) + count)])
         self._progress.update([(name, progress)])

   def _show_progress(self, current_thread):
 
      msg= []
      with self._lock:
         for thread in self.threads:
            progress= self._progress.get(thread.name, dict([(s, 0) for s in self.status.flags()]))
            msg.append("%s %s/%s -> " % (thread.name, progress.get("forked"), progress.get("processed")))

      print "thread: %s %s" % (current_thread.name, ''.join(msg)[:-4])
         
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

      [method(self) in sorted(filter(lambda (name, method): type(method) == FunctionType and method.__name__ == "_shutdown", self.__class__.__dict__.items()), key= lambda (name, method): method.order)]


class Worker(Process):

   statuses= ("idle", "working")

   def __init__(self, host, port, security_key):

      super(Worker, self).__init__()

      SyncManager.register("get")
      SyncManager.register("peek")
      SyncManager.register("put")
      SyncManager.register("del")
      SyncManager.register("empty")
      SyncManager.register("qsize")
      SyncManager.register("task_done")
      SyncManager.register("get_channels")

      self.impq= SyncManager(address= (host, int(port)), authkey= security_key)
      self.impq.connect()

      self.alive= True
      self.status= Status(*self.statuses)

   def process(self, channel_id):

      print "processing channel", channel_id
      try:
         job= self.impq.get(channel_id)
         if job._getvalue() == None:
            print "No Jobs"
            return
      except Exception, e:
         print >> stderr, "error:", str(e)

      try:
         self.status.next()
         job.promote()

         print "processing job:", self.pid, self.status.current, job.get("id"), job.get("name"), job.get("status")
         #print job
         stdout.flush()

         method= FunctionType(loads(job.get("code")), globals(), job.get("name"))
         result= method(job.get("args"))
         job.update([("result", result)])
         job.promote()

         #self.impq.task_done(job)
         self.status.previous()

         print "completed job:", self.pid, self.status.current, job.get("id"), job.get("name"), job.get("status")

      except Exception, e:

         (filename, linenumber, functionname, statement)= extract_tb(exc_info()[2])[-1]
         result= {"error": str(e), "name": functionname, "linenumber": linenumber, "statement": statement}
         job.update([("result", result)])
         job.promote("error")

         print >> stderr, "error processing job:", self.pid, self.status.current, job.get("id"), job.get("name"), job.get("status"), str(e)


   def run(self):

      while self.alive:

         for channel_id in self.impq.get_channels():
            self.process(channel_id)

         sleep(1)

class Node(object):

   def __init__(self, host, port, security_key, max_processes= 5):

      self.host= host
      self.port= port
      self.security_key= security_key
      self.max_processes= 5

      self.alive= True
      self.workers= {}

   def run(self):

      for i in range(self.max_processes):
         worker= Worker(self.host, self.port, self.security_key)
         worker.start()
         self.workers.update([(worker.pid, worker)])

      while self.alive:
   
         for pid, worker in self.workers.items():
            print "process_id: %s, status: %s" % (pid, worker.status.current)
            stdout.flush()
            sleep(1)


if __name__ == "__main__":
   q= Queue("localhost", 50000, "test")
   q.run()
