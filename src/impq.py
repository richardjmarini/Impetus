#!/usr/bin/env python

from os import path, makedirs
from sys import stdout, stderr
from multiprocessing import Process
from multiprocessing.managers import SyncManager, DictProxy
from uuid import uuid1
from datetime import datetime
from itertools import izip
from time import sleep
from Queue import PriorityQueue , Empty
from marshal import dumps, loads
from types import FunctionType
from threading import Thread, Lock, currentThread
from codecs import open as utf8open

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

   def get(self, status):

      label= dict(enumerate(self._statuses)).get(status)

      return label

   def __iter__(self):

      return self

   def next(self):
      if self.current >= len(self._statuses):
         self.current= len(self._statuses)-1

      self.current+= 1
      return self._statuses[self.current-1]

   def previous(self):

      self.current-= 1
      if self.current < 0:
         self.current= 0

      return self.current

   def __repr__(self):

      return repr(self.current)


class Job(Autovivification):

   statuses= ("waiting", "running", "ready", "error")

   def __init__(self, *args, **kwargs):

      id= kwargs.get('id')
      if not id:
         id= str(uuid1())
      else:
         del kwargs['id']

      created= kwargs.get('created')
      if not created:
         created= datetime.utcnow()

      status= Status(*self.statuses)

      kwargs.update([("id", id), ("created", created), ("status", status), ("data", args)])

      super(Job, self).__init__(**kwargs)

   def promote(self):
  
      self.status.next()

   def demote(self):

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
      self.manager.register("put", callable= self.put)
      self.manager.register("get", callable= self.get)
      self.manager.register("status", callable= self.status)
      self.manager.register("task_done", callable= self.task_done)
      self.manager.register("get_channels", callable= self.get_channels, proxytype= DictProxy)

   def create_channel(self, **properties):

      channel= Channel(**properties)
      channel_id= properties.get('channel_id')
      if not channel_id:
         channel_id= uuid1()

      self.channels[channel_id]= channel

      return channel_id

   def get_channels(self):

      channels= [channel_id for channel_id in self.channels.keys()]

      return channels

   def put(self, job, channel_id= None):

      if not channel_id:
         channel_id= uuid1()

      if channel_id not in self.channels:
         print >> stderr, "warning: creating non-existent channel [%s]" % (channel_id)
         self.create_channel(channel_id= channel_id)
 
      self.channels[channel_id].store.update([(job.get('id'), job)])
      self.channels[channel_id].stream.put([(job.get('priority'), job.get('id'))])

      return channel_id

   def get(self, channel_id, block= False):

      if channel_id not in self.channels:
         raise Exception("invalid channel_id [%s]" % (channel_id))

      try:
         (job_priority, job_id)= self.channels[channel_id].stream.get(block)
         job= self.channels[channel_id].store.get(job_id)
         print "RETURNING JOB", job
      except Empty:
         job= None

      return job

   def status(self, channel_id):

      if channel_id not in self.channels:
         raise Exception("invalid channel_id [%s]" % (channel_id))

      store= self.channels[channel_id].store
      return len(store), store

   def task_done(self, job, channel_id):

      if channel_id not in self.channels:
         raise Exception("invalid channel_id [%s]" % (channel_id))

      self.channels[channel_id].store.update([(job.get('id'), job)])
      self.channels[channel_id].stream.task_done()

   def run(self):

      server= self.manager.get_server()
      print "running"
      server.serve_forever()


_thread_order= 0
class Client(object):

   def __init__(self, host, port, security_key, id= None, task_dir= 'tasks'):

      self.id= id if id else str(uuid1())
      self.task_dir= path.join(task_dir, self.id)
      self.status= Status("forked", "processed")

      self.impq= SyncManager(address= (host, port), authkey= security_key)
      self.impq.register('create_channel')
      self.impq.register('put')
      self.impq.register('status')
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

         shutdown(self)

      global _thread_order
      _shutdown.order= _thread_order
      return _shutdown

   @staticmethod
   def process(process):

      def _process(self):

         current_thread= currentThread()
         previous_thread= current_thread.previous_thread
         status= Status(*Job.statuses)

         while self.alive:

            self._thread_regulator(current_thread, previous_thread)

            print "%s %s" % (datetime.utcnow(), current_thread.name)

            (num_jobs, jobs)= self.impq.status(self.id)
            ready= filter(lambda job_id: self.jobs[job_id].status.current == status.get("ready"), self.jobs)
            error= filter(lambda job_id: self.jobs[job_id].status.current == status.get("error"), self.jobs)
    
            if len(ready) or len(errors):
               process(self, ready, errors)

            self._thread_progress(current_thread.name, self.status.processed, len(ready) + len(errors))
            self._show_progress(current_thread)

            if num_jobs and previous_thread != None and previous_thread.is_alive() == False:
               print "%s %s completed" % (datetime.utcnow(), current_thread.name)
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
         callback= callback.func_name if callback else None,
         result= None,
         transport= None
      )

      if priority:
         setattr(job, 'priority', priority)

      self.impq.put(job, channel_id= self.id)
      self.jobs.append(job.get("id"))

      self._thread_progress(current_thread.name, self.status.forked, 1)

      return job.get('id')

   def _thread_progress(self, name, status, count):

      with self._lock:
 
         progress= self._progress.get(name, dict(self.status.__dict__))
         progress.update([(status, progress.get(status, 0) + count)])
         self._progress.update([(name, progress)])

   def __show_progress(self, current_thread):
 
      msg= []
      with self._lock:
         for thread in self.threads:
            progress= self._progress.get(thread.name, dict(self.status.__dict__))
            msg.append("%s %s/%s -> " % (thread.name, counter.get(self.status.forked), counter.get(self.status.processed)))

      print "%s %s via %s" % (datetime.utcnow(), ''.join(msg)[:-4], current.thread_name)
         
   def _thread_regulator(self, current_thread, previous_thread):

      stall_time= 1
      while self._current_thread == current_thread:
         sleep(stall_time)
         stall_time+= 1
         if stall_time >= 10:
            break

         if current_thread.name == self.threads[-1].name and previous_thread != None and previous_thread.is_Alive() == False:
            while self._lock:
               self._current_thread= self.threads[0]

      while self._lock:
         self._current_thread= current_thread

   def _create_thread(self, name, method):

      
      thread= Thread(target= method, name= name, args= (self, ))
      self.errors[name]= utf8open(path.join(self.task_dir, '.'.join((name, 'err'))), 'ab+')
      self.ready[name]=  utf8open(path.join(self.task_dir, '.'.join((name, 'ok'))), 'ab+')

      return thread
 
   def _link_threads(self, threads):
 
      for i in range(len(threads)):
 
        setattr(threads[i], 'previous_thread', threads[i-1] if i > 0 else None)
        setattr(threads[i], 'next_thread', threads[i+1] if i < len(threads)-1 else None)
  
      return threads[0]

   def _start_threads(self, threads):

      [thread.start() for thread in threads]
      [thread.join() for thread in threads]

   def run(self):

      self.threads= [self._create_thread(name, method) for (name, method) in sorted(filter(lambda (name, method): type(method) == FunctionType and method.__name__ == '_process', self.__class__.__dict__.items()), key= lambda (name, method): method.order)]
      self._current_thread= self._link_threads(self.threads)
      self._start_threads(self.threads)

      [method(self, self.ready, self.errors, self._progress) in sorted(filter(lambda (name, method): type(method) == FunctionType and method.__name__ == '_shutdown', self.__class__.__dict__.items()), key= lambda (name, method): method.order)]


class Worker(Process):

   statuses= ("idle", "working")

   def __init__(self, host, port, security_key):

      super(Worker, self).__init__()

      SyncManager.register('get')
      SyncManager.register('peek')
      SyncManager.register('put')
      SyncManager.register('empty')
      SyncManager.register('qsize')
      SyncManager.register('task_done')
      SyncManager.register('get_channels')

      self.impq= SyncManager(address= (host, int(port)), authkey= security_key)
      self.impq.connect()

      self.alive= True
      self.status= Status(*self.statuses)

   def process(self, channel_id):

      print "processing channel", channel_id
      try:

         job= self.impq.get(channel_id)
         if not job:
            return

         self.status.next()
         job.promote()

         print self.pid, self.status.current, job.get('id'), job.get('name')
         stdout.flush()

         method= FunctionType(loads(job.get('code')), globals(), job.get('name'))
         result= method(job.get('args'))
         job.update([('result', result)])

         job.promote()

         self.impq.task_done(job)
         self.status.previous()

      except Exception, e:
         print str(e)


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
