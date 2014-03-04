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
from atexit import register

class Autovivification(object):

   def __init__(self, **properties):

      for pname, pvalue in properties.items():

         if type(pvalue) == dict:
            setattr(self, pname, Autovivification(**pvalue))

         elif type(pvalue) in (list, tuple):
            setattr(self, pname, [Autovivification(**element) if type(element) == dict else element for element in pvalue])

         else:
            setattr(self, pname, pvalue)

class Stream(object):
 
   def __init__(self, **properties):

      super(Stream, self).__init__()

      self.properties= properties
      self.queue= PriorityQueue()
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

      self.streams= {}

      self.manager= SyncManager(address= (host, int(port)), authkey= security_key)
      self.manager.register("create_stream", callable= self.create_stream)
      self.manager.register("delete_stream", callable= self.delete_stream)
      self.manager.register("get_streams", callable=  lambda: self.streams, proxytype= DictProxy)
      self.manager.register("get_store", callable=  lambda stream_id: self.streams[stream_id].store, proxytype= DictProxy)
      self.manager.register("get_queue", callable=  lambda stream_id: self.streams[stream_id].queue, proxytype= PriorityQueue)

   def create_stream(self, **properties):

      stream= Stream(**properties)
      stream_id= properties.get("stream_id")
      if not stream_id:
         stream_id= uuid1()

      self.streams[stream_id]= stream

   def delete_stream(self, **properties):

      stream_id= properties.get("stream_id")
      del self.streams[stream_id]

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
      self.impq.register("get_streams")
      self.impq.register("create_stream")
      self.impq.register("delete_stream")
      self.impq.register("get_store")
      self.impq.register("get_queue")
      self.impq.register("jobs")
      self.impq.connect()

      self.jobs= []
      self.impq.create_stream(stream_id= self.id)
      self.store= self.impq.get_store(stream_id= self.id)
      self.queue= self.impq.get_queue(stream_id= self.id)
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

      self.impq.delete_stream(stream_id= self.id)

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
      self.queue.put([(job.get("priority"), job.get("id"))])
  
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

   def __init__(self, stream_id, queue, store):

      super(Worker, self).__init__()

      self.alive= True
      self.status= "idle"
      self.stream_id= stream_id
      self.queue= queue
      self.store= store

   def process(self):

      try:

         print "processing stream", self.pid, self.stream_id
         job_id= None
         job= {}

         try:
            (priority, job_id)= self.queue.get(block= True, timeout= 30).pop()
         except Empty:
            print "stream idle", self.pid, self.stream_id
            self.alive= False
            return

         self.status= "busy"

         job= self.store.get(job_id)
         job.update([("status", "processing")])
         self.store.update([(job.get("id"), job)])

         print "processing job: %s,  %s, %s, %s" % (self.pid, job.get("id"), job.get("name"), job.get("status"))

         method= FunctionType(loads(job.get("code")), globals(), job.get("name"))
         result= method(job.get("args"))
         job.update([("result", result), ("status", "ready")])
         self.store.update([(job.get("id"), job)])

         print "completed job: %s, %s, %s, %s" % (self.pid, job.get("id"), job.get("name"), job.get("status"))
      except Exception, e:

         (filename, linenumber, functionname, statement)= extract_tb(exc_info()[2])[-1]
         result= {"error": str(e), "name": functionname, "linenumber": linenumber, "statement": statement}

         job= self.store.get(job_id)
         print >> stderr, "error processing job:", self.pid, job.get("id"), job.get("name"), job.get("status"), str(e), functionname, linenumber, statement

         job.update([("result", result), ("status", "error")])
         self.store.update([(job.get("id"), job)])

      self.status= "idle"

   def run(self):

      while self.alive:

         try:
            self.process()
         except (UnboundLocalError, EOFError, IOError, SocketError) as e:
            print >> stderr, "worker communication error:", self.stream_id, str(e)
            self.status= "idle"
            self.alive=  False

         sleep(0.01)


class Node(object):

   def __init__(self, host, port, security_key, max_processes= 5):

      self.host= host
      self.port= port
      self.security_key= security_key
      self.max_processes= max_processes

      self.workers= {}
      self.alive= True

      register(self.shutdown)
      self.connect()

   def connect(self):

      # remove connection from cache:
      # BaseProxy class has thread local storage which caches the connection
      # which is reused for future connections causing "borken pipe" errors on 
      # creating new manager.  
      if (self.host, int(self.port)) in BaseProxy._address_to_local:
         del BaseProxy._address_to_local[(self.host, int(self.port))][0].connection

      # register handlers
      SyncManager.register("get_streams")
      SyncManager.register("get_queue")
      SyncManager.register("get_store")

      while self.alive:

         try:
            self.impq= SyncManager(address= (self.host, int(self.port)), authkey= self.security_key)
            self.impq.connect() 
            break
         except (EOFError, IOError, SocketError) as e:
            print "could not connect ...trying again", str(e)
            sleep(1)

   def update_queues(self, streams, queues):

      # update list of queues to track
      for stream_id in streams.keys():
         if stream_id not in queues.keys():
            print "tracking stream", stream_id
            queues.update([(stream_id, (self.impq.get_queue(stream_id), self.impq.get_store(stream_id)))])
          
      for stream_id in queues.keys():
         if stream_id not in streams.keys():
            print 'stopped tracking stream', stream_id
            queues.pop(stream_id)


   def process(self):

      print "max workers per queue", self.max_processes

      streams= self.impq.get_streams()
      queues= dict([(stream_id, (self.impq.get_queue(stream_id), self.impq.get_store(stream_id))) for stream_id in streams.keys()])

      while self.alive:

         self.update_queues(streams, queues)

         # stop tracking dead workers
         for (pid, worker) in self.workers.items():
            if not worker.is_alive():
               print "worker dead", pid, worker.stream_id
               self.workers.pop(pid)

         for (stream_id, (queue, store)) in queues.items():

             workers= filter(lambda w: w.stream_id == stream_id, self.workers.values())
             num_workers=  self.max_processes - len(workers)
             if num_workers:
                print "creating %s workers for %s" % (num_workers, stream_id)

             for i in range(1, num_workers + 1):
                worker= Worker(stream_id, queue, store)
                worker.start()
                self.workers.update([(worker.pid, worker)])
                print "created worker", i, worker.pid, stream_id

      self.shutdown()

   def shutdown(self):

      # wait for workers to finish before shutting down
      print "shutting down node..."
      for (pid, worker) in self.workers.items():
         print "waiting for worker:", pid, worker.stream_id
         worker.join()
 
      print "node shutdown complete."

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
