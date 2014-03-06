#!/usr/bin/env python
#-*- coding:utf-8 -*-
#---------------------------------------------------------------------------
# Author: Richard J. Marini (richardjmarini@gmail.com)
# Date: 2/4/2014
# Name: Impetus
# Desciption: Impetus is an auto-scaling distributed processing framework.
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

from copy import deepcopy
from os import path, makedirs, getcwd, fork, chdir, setsid, umask, getpid, dup2, remove, kill, makedirs, pardir, stat, rename, curdir
from sys import stdin, stdout, stderr, exc_info, exit
from multiprocessing import Process
from multiprocessing.managers import SyncManager, DictProxy, BaseProxy
from uuid import uuid1
from datetime import datetime
from itertools import izip
from time import sleep
from Queue import PriorityQueue , Empty
from marshal import dumps as mdumps, loads as mloads
from json import dumps as jdumps, JSONEncoder
from types import FunctionType
from threading import Thread, Lock, currentThread, Event
from traceback import extract_tb
from socket import error as SocketError
from atexit import register
from codecs import open as open
from zlib import compress, decompress
from base64 import b64encode as encode, b64decode as decode
from signal import SIGTERM


class Autovivification(object):

   def __init__(self, **properties):

      for pname, pvalue in properties.items():

         if type(pvalue) == dict:
            setattr(self, pname, Autovivification(**pvalue))

         elif type(pvalue) in (list, tuple):
            setattr(self, pname, [Autovivification(**element) if type(element) == dict else element for element in pvalue])

         else:
            setattr(self, pname, pvalue)

class Stream(Autovivification):
 
   def __init__(self, **properties):

      id= properties.get("id")
      if not id:
         id= uuid1()
         properties["id"]= id

      super(Stream, self).__init__(**properties)

      self.properties= properties
      self.queue= PriorityQueue()
      self.store= {}

class Job(dict):

   def __init__(self, **kwargs):

      kwargs["id"]= kwargs.get("id", str(uuid1()))
      kwargs["created"]= kwargs.get("created", datetime.utcnow())
      kwargs["status"]= kwargs.get("status", "waiting")
      kwargs["delay"]= kwargs.get("delay", 0)

      super(Job, self).__init__(**kwargs)

class JobEncoder(JSONEncoder):

   def default(self, obj):
      if isinstance(obj, datetime):
         return str(obj)
      return JSONEncoder.default(self, obj)

class Daemon(object):
   '''
   Damonizes base object
   '''

   def __init__(self, pidfile, stdin= "/dev/null", stdout= "/dev/null", stderr= "/dev/null"):
      '''
      Initializes daemon
      '''
      self.stdin= stdin
      self.stdout= stdout
      self.stderr= stderr
      self.pidfile= pidfile
      super(Daemon, self).__init__()

   def fork(self):
      '''
      Forks off the the process into the background
      '''
      try:
         pid= fork()
         if pid > 0:
            exit(0)
      except OSError, e:
         exit(1)

   def daemonize(self):
      '''
      Forks then sets up the I/O stream for the daemon 
      '''
      self.fork()

      chdir(getcwd())
      setsid()
      umask(0)
  
      self.fork()
      stdout.flush()
      stderr.flush()

      si= file(self.stdin, 'w+')
      so= file(self.stdout, 'a+')
      se= file(self.stderr, 'a+', 0)

      dup2(si.fileno(), stdin.fileno())
      dup2(so.fileno(), stdout.fileno())
      dup2(se.fileno(), stderr.fileno())

      register(self.delPID)
      self.setPID()

   def setPID(self):
      '''
      Creates PID file for the current daemon on the filesystem
      '''
      pid= str(getpid())
      fh= open(self.pidfile, 'w')
      fh.write(pid)
      fh.close()

   def delPID(self):
      '''
       Removes the PID file from the filesystem
      '''
      remove(self.pidfile)

   def getPID(self):
      '''
      Reads the PID from the filesystem
      '''
      try:
          pid= int(open(self.pidfile, 'r').read())
      except IOError, e:
          pid= None

      return pid

   def start(self):
      '''
      Startup the daemon process
      '''
      pid= self.getPID()
      if pid:
         exit(1)

      self.daemonize()
      self.run()

   def stop(self):
      '''
      Stops the daemon process
      '''
      pid= self.getPID()
      if not pid:
         return

      try:
         kill(pid, SIGTERM)
         sleep(0.1)
         self.delPID()
      except OSError, e:
         if str(e).find('No such process') > 0:
            if path.exists(self.pidfile):
               self.delPID()
         else:
            exit(1)

   def restart(self):
      '''
      Restarts the daemon process
      '''
      self.stop()
      self.start()

   def run(self):
      '''
      Overridden in base class
      '''
      pass

   '''
   Damonizes base object
   '''

   def __init__(self, pidfile, stdin= "/dev/null", stdout= "/dev/null", stderr= "/dev/null"):
      '''
      Initializes daemon
      '''
      self.stdin= stdin
      self.stdout= stdout
      self.stderr= stderr
      self.pidfile= pidfile
      super(Daemon, self).__init__()

   def fork(self):
      '''
      Forks off the the process into the background
      '''
      try:
         pid= fork()
         if pid > 0:
            exit(0)
      except OSError, e:
         exit(1)

   def daemonize(self):
      '''
      Forks then sets up the I/O stream for the daemon 
      '''
      self.fork()

      chdir(getcwd())
      setsid()
      umask(0)
  
      self.fork()
      stdout.flush()
      stderr.flush()

      si= file(self.stdin, 'w+')
      so= file(self.stdout, 'a+')
      se= file(self.stderr, 'a+', 0)

      dup2(si.fileno(), stdin.fileno())
      dup2(so.fileno(), stdout.fileno())
      dup2(se.fileno(), stderr.fileno())

      register(self.delPID)
      self.setPID()

   def setPID(self):
      '''
      Creates PID file for the current daemon on the filesystem
      '''
      pid= str(getpid())
      fh= open(self.pidfile, 'w')
      fh.write(pid)
      fh.close()

   def delPID(self):
      '''
       Removes the PID file from the filesystem
      '''
      remove(self.pidfile)

   def getPID(self):
      '''
      Reads the PID from the filesystem
      '''
      try:
          pid= int(open(self.pidfile, 'r').read())
      except IOError, e:
          pid= None

      return pid

   def start(self):
      '''
      Startup the daemon process
      '''
      pid= self.getPID()
      if pid:
         exit(1)

      self.daemonize()
      self.run()

   def stop(self):
      '''
      Stops the daemon process
      '''
      pid= self.getPID()
      if not pid:
         return

      try:
         kill(pid, SIGTERM)
         sleep(0.1)
         self.delPID()
      except OSError, e:
         if str(e).find('No such process') > 0:
            if path.exists(self.pidfile):
               self.delPID()
         else:
            exit(1)

   def restart(self):
      '''
      Restarts the daemon process
      '''
      self.stop()
      self.start()

   def run(self):
      '''
      Overridden in base class
      '''
      pass

class Queue(Daemon):

   def __init__(self, address, authkey, logdir= curdir, piddir= curdir):

      self.streams= {}
      self.address= address
      self.manager= SyncManager(address= self.address, authkey= authkey)
      self.manager.register("create_stream", callable= self.create_stream)
      self.manager.register("delete_stream", callable= self.delete_stream)
      self.manager.register("get_streams", callable= lambda: self.streams, proxytype= DictProxy)
      self.manager.register("get_store", callable= lambda id: self.streams[id].store, proxytype= DictProxy)
      self.manager.register("get_queue", callable= lambda id: self.streams[id].queue, proxytype= PriorityQueue)
      self.manager.register("get_properties", callable= lambda id: self.streams[id].properties, proxytype= DictProxy)

      super(Queue, self).__init__(
         pidfile= path.join(piddir, self.__class__.__name__ + ".pid"),
         stdout= path.join(logdir, self.__class__.__name__ + ".out"),
         stderr= path.join(logdir, self.__class__.__name__ + ".err"),
         stdin= path.join(logdir, self.__class__.__name__ + ".in")
      )

      #self.monitor= Thread(target= self.monitor)
      #self.alive= Event()
      #register(self.alive.set)

   """
   def monitor(self):
     
      print "starting queue monitor"
      while self.alive.isSet():
         for (stream_id, stream) in self.streams.items():
            print "stream:", stream_id, stream.queue.qsize(), len(stream.store), self.alive.isSet()
         sleep(1)
      print "stopping queue monitor"

   """

   def create_stream(self, **properties):

      stream= Stream(**properties)

      self.streams[stream.id]= stream

   def delete_stream(self, id):

      del self.streams[id]

   def run(self):

      server= self.manager.get_server()
      print "running"
      #self.monitor.start()
      server.serve_forever()


_thread_order= 0
class Client(object):

   statuses= ("forked", "processed")

   def __init__(self, address, authkey, taskdir= "tasks", id= None, **properties):

      self.id= id if id else str(uuid1())
      self.address= address
      self.taskdir= path.join(taskdir, self.id)

      self.impq= SyncManager(address= self.address, authkey= authkey)
      self.impq.register("get_streams")
      self.impq.register("create_stream")
      self.impq.register("delete_stream")
      self.impq.register("get_store")
      self.impq.register("get_queue")
      self.impq.connect()

      self.jobs= []
      self.impq.create_stream(id= self.id, **properties)
      self.store= self.impq.get_store(id= self.id)
      self.queue= self.impq.get_queue(id= self.id)
      self.alive= True
      self._current_thread= None
      self._lock= Lock()
      self.threads= []
      self.errors= {}
      self.ready= {}
      self._progress= {}


      try:
         makedirs(self.taskdir)
      except:
         pass

   def __del__(self):

      self.impq.delete_stream(self.id)

   @staticmethod
   def node(target):

      return target

   @staticmethod
   def startup(target):

      def _process(self):

         target(self)

      global _thread_order
      _process.order= _thread_order
      _thread_order+= 1
      return _process

   @staticmethod
   def shutdown(target):
   
      def _shutdown(self):

         target(self, self.ready, self.errors, self._progress)

      global _thread_order
      _shutdown.order= _thread_order
      return _shutdown

   @staticmethod
   def process(target):

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
                  self.ready[current_thread.name].write(encode(compress(jdumps(job, cls= JobEncoder))) + "\n")
               elif job.get("status") == "error":
                  errors.append(job)
                  self.ready[current_thread.name].write(encode(compress(jdumps(job, cls= JobEncoder))) + "\n")
               else:
                  continue

               #print "killing", job.get("id")
               self.store.pop(job.get("id"))

            if len(ready) or len(errors):
               target(self, ready, errors)

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

   def fork(self, target, args, callback= None, priority= None, job_id= None, **properties):

      current_thread= currentThread()
      
      job= Job(
         client= self.id,
         name= target.func_name,
         code= encode(compress(mdumps(target.func_code))),
         args= args,
         callback= callback.func_name if callback else current_thread.next_thread.name,
         result= None,
         transport= None,
         **properties
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
      self.errors[name]= open(path.join(self.taskdir, '.'.join((name, "err"))), 'ab+')
      self.ready[name]= open(path.join(self.taskdir, '.'.join((name, "ok"))), 'ab+')

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

   def __init__(self, stream_id, queue, store, properties):

      super(Worker, self).__init__()

      self.alive= True
      self.status= "idle"
      self.stream_id= stream_id
      self.queue= queue
      self.store= store
      self.properties= properties

   def process(self):

      try:

         #print "processing stream", self.pid, self.stream_id
         job_id= None
         job= {}

         try:
            (priority, job_id)= self.queue.get(block= True, timeout= self.properties.get("timeout", 15)).pop()
         except Empty:
            #print "stream idle", self.pid, self.stream_id
            self.alive= False
            return

         self.status= "busy"

         job= self.store.get(job_id)
         job.update([("status", "processing")])
         self.store.update([(job.get("id"), job)])

         print "processing job: %s,  %s, %s, %s" % (self.pid, job.get("id"), job.get("name"), job.get("status"))

         sleep(job.get("delay"))
         method= FunctionType(mloads(decompress(decode(job.get("code")))), globals(), job.get("name"))
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
            self.alive= False

         sleep(self.properties.get("frequency", 0.01))

class Status(dict):

   def __init__(self, **kwargs):

      kwargs["timestamp"]= kwargs.get("timestamp", datetime.utcnow())
      kwargs["streams"]= kwargs.get("streams", 0)
      kwargs["active"]= kwargs.get("active", 0)
      kwargs["jobs"]= kwargs.get("jobs", 0)
      kwargs["mpps"]= kwargs.get("mpps", 0)

      super(Status, self).__init__(**kwargs)

      self["required"]= self["jobs"] - self["active"]


class Node(Daemon):

   def __init__(self, queue, qauthkey, mpps= 5, dfs= None, dauthkey= None, logdir= curdir, piddir= curdir, **properties):

      self.queue= queue
      self.qauthkey= qauthkey
      self.mpps= mpps
      self.dfs= dfs
      self.dauthkey= dauthkey
      self.properties= properties

      self.workers= {}
      self.alive= True

      register(self.shutdown)

      self.connect()
  
      super(Node, self).__init__(
         pidfile= path.join(piddir, self.__class__.__name__ + ".pid"),
         stdout= path.join(logdir, self.__class__.__name__ + ".out"),
         stderr= path.join(logdir, self.__class__.__name__ + ".err"),
         stdin= path.join(logdir, self.__class__.__name__ + ".in")
      )

   def connect(self):

      self.qconnect()
      if None not in self.dfs:
         self.dconnect()

   def qconnect(self):

      # remove connection from cache:
      # BaseProxy class has thread local storage which caches the connection
      # which is reused for future connections causing "borken pipe" errors on 
      # creating new manager.  
      if self.queue in BaseProxy._address_to_local:
         del BaseProxy._address_to_local[self.queue][0].connection

      # register handlers
      SyncManager.register("get_streams")
      SyncManager.register("get_queue")
      SyncManager.register("get_store")
      SyncManager.register("get_properties")

      print "connecting to queue", self.queue
      while self.alive:

         try:
            self.impq= SyncManager(address= self.queue, authkey= self.qauthkey)
            self.impq.connect() 
            break
         except (EOFError, IOError, SocketError) as e:
            print "could not connect ...trying again", str(e)
            sleep(1)

   def dconnect(self):

      # remove connection from cache:
      # BaseProxy class has thread local storage which caches the connection
      # which is reused for future connections causing "borken pipe" errors on
      # creating new manager.
      if self.dfs in BaseProxy._address_to_local:
         del BaseProxy._address_to_local[self.dfs][0].connection

      # register handlers
      SyncManager.register("get_nodes")

      print "connecting to dfs", self.dfs
      while self.alive:

         try:
            self.impd= SyncManager(address= self.dfs, authkey= self.dauthkey)
            self.impd.connect()
            break
         except (EOFError, IOError, SocketError) as e:
            print "could not connect ...trying again", str(e)
            sleep(1)

   def process(self):

      print "max processes per stream", self.mpps

      # get list of streams proxies
      streams= self.impq.get_streams()
      streams_tracking= {} 

      while self.alive:

         # get list of streams to track we are not currently tracking
         streams_to_track= filter(lambda stream_id: stream_id not in streams_tracking.keys(), streams.keys())

         # if we are only tracking streams with specific properties
         if len(self.properties):

            # get properties for all the streams we are tracking
            stream_properties= [dict(self.impq.get_properties(stream_id)) for stream_id in streams_to_track]

            # filter out streams we want to track based on matching subsets of properties
            streams_to_track= map(lambda sp: sp.get("id"), filter(lambda sp: set(self.properties.items()).issubset(sp.items()), stream_properties))

         for stream_id in streams_to_track:
            print "tracking stream", stream_id
            streams_tracking.update([(stream_id, (self.impq.get_queue(stream_id), self.impq.get_store(stream_id), self.properties))])

         # stop tracking streams which are no longer active
         for stream_id in streams_tracking.keys():
            if stream_id not in streams.keys():
               print 'stopped tracking stream', stream_id
               streams_tracking.pop(stream_id)

         # stop tracking workers which are no longer active
         for (pid, worker) in self.workers.items():
            if not worker.is_alive():
               #print "worker dead", pid, worker.stream_id
               self.workers.pop(pid)

         # create workers for streams we are currently tracking
         for (stream_id, (queue, store, properties)) in streams_tracking.items():

            qsize= queue.qsize()
            stream_workers= filter(lambda w: w.stream_id == stream_id, self.workers.values())
            num_stream_workers= min(qsize, self.mpps - len(stream_workers))
            if num_stream_workers:
               print "creating %s workers for %s" % (num_stream_workers, stream_id)
            for i in range(1, num_stream_workers + 1):
               worker= Worker(stream_id, queue, store, properties)
               worker.start()
               self.workers.update([(worker.pid, worker)])
               print "created worker", i, worker.pid, stream_id

         status= Status(
            number_of_workers= len(self.workers),
            number_of_streams= len(streams_tracking),
            queue_size= sum([queue.qsize() for (stream_id, (queue, store, properties)) in streams_tracking.items()]),
            store_size= sum([len(store) for (stream_id, (queue, store, properties)) in streams_tracking.items()]),
            mpps= self.mpps
         )
         #print "Status:", status
         sleep(1)

      self.shutdown()

   def shutdown(self):

      # wait for workers to finish before shutting down
      #print "shutting down node..."
      for (pid, worker) in self.workers.items():
         print "waiting for worker:", pid, worker.stream_id
         worker.join()
 
      #print "node shutdown complete."

   def run(self):

      while self.alive:
         try:
            self.process()
         except Exception, e:
            print >> stderr, "node communication error", str(e)
            self.connect()
         sleep(1)


class DFS(Daemon):

   def __init__(self, address, authkey, queue, qauthkey, logdir= curdir, piddir= curdir):

      self.address= address
      self.authkey= authkey

      self.queue= queue
      self.qauthkey= qauthkey

      self.alive= True
      self.nodes= {}

      self.manager= SyncManager(address= self.address, authkey= self.authkey)
      self.manager.register("get_nodes", callable= lambda: self.nodes, proxytype= DictProxy)

      self.connect()

   def connect(self):

      # remove connection from cache:
      # BaseProxy class has thread local storage which caches the connection
      # which is reused for future connections causing "borken pipe" errors on
      # creating new manager.
      if self.queue in BaseProxy._address_to_local:
         del BaseProxy._address_to_local[self.queue][0].connection

      # register handlers
      SyncManager.register("get_streams")

      while self.alive:

         try:
            self.impq= SyncManager(address= self.queue, authkey= self.qauthkey)
            self.impq.connect()
            break
         except (EOFError, IOError, SocketError) as e:
            print "could not connect ...trying again", str(e)
            sleep(1)

   def run(self):

      server= self.manager.get_server()
      print "running"
      server.serve_forever()

