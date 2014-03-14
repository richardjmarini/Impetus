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

from string import Template
from boto.ec2 import EC2Connection
from copy import deepcopy
from os import path, makedirs, getcwd, fork, chdir, setsid, umask, getpid, dup2, remove, kill, makedirs, pardir, stat, rename, curdir, getppid, getenv
from sys import stdin, stdout, stderr, exc_info, exit
from multiprocessing import Process, Value
from multiprocessing.managers import SyncManager, DictProxy, BaseProxy
from uuid import uuid1
from datetime import datetime
from itertools import izip, product
from time import sleep
from Queue import PriorityQueue , Empty
from marshal import dumps as mdumps, loads as mloads
from json import dumps as jdumps, JSONEncoder
from types import FunctionType
from threading import Thread, Lock, currentThread, Event
from traceback import extract_tb
from socket import error as SocketError, getfqdn, gethostbyname, gethostname, inet_ntoa, socket, AF_INET, SOCK_DGRAM
from atexit import register
from codecs import open as open
from zlib import compress, decompress
from base64 import b64encode as encode, b64decode as decode
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from fcntl import ioctl
from struct import pack

def getipaddress():
   """
   Determines which network card interface is being used
   for communicating with the rest of the Impetus system
   and returns the ip address of that interface.
   """
   ipaddress= None
   ip= gethostbyname(gethostname())

   interface_types= ["eth", "wlan", "wifi", "ath", "ppp"]
   interface_range= map(str, range(0, 5))

   # if the ip 
   if ip.startswith("127."):
     
      for interface in map(''.join, product(interface_types, interface_range)):

         try:
            sock= socket(AF_INET, SOCK_DGRAM)
            ipaddress= inet_ntoa(ioctl(sock.fileno(), 0x8915, pack("256s", interface[:15]))[20:24])
            break
         except IOError:
            continue

   if not ipaddress:
      ipaddress= getfqdn()

   return ipaddress

class Autovivification(object):
   """
   Recursivley traverses a dictionary data-structure
   and creates and object with it's corresponding properties
   based on the key/value pairs of the data-structre.
   """

   def __init__(self, **properties):

      for pname, pvalue in properties.items():

         if type(pvalue) == dict:
            setattr(self, pname, Autovivification(**pvalue))

         elif type(pvalue) in (list, tuple):
            setattr(self, pname, [Autovivification(**element) if type(element) == dict else element for element in pvalue])

         else:
            setattr(self, pname, pvalue)

class Stream(Autovivification):
   """
   Creates a stream object which acts as a container 
   for a priority queue, data-store and set of properties
   associated with the stream.
   """
 
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
   """
   Creates a Job object which acts as a container
   for each item within the Streams data-store.
   """

   def __init__(self, **kwargs):

      kwargs["id"]= kwargs.get("id", str(uuid1()))
      kwargs["created"]= kwargs.get("created", datetime.utcnow())
      kwargs["status"]= kwargs.get("status", "waiting")
      kwargs["delay"]= kwargs.get("delay", 0)

      super(Job, self).__init__(**kwargs)

class JobEncoder(JSONEncoder):
   """Encodes the Job object into Json."""

   def default(self, obj):
      if isinstance(obj, datetime):
         return str(obj)
      return JSONEncoder.default(self, obj)

class Daemon(object):
   """
   Damonizes a base object by forking off 
   from the controlling terminal. Maintains a 
   PID file of the current process(es). Provides
   start/stop and restart methods for the process(es). 
   """

   def __init__(self, pidfile, stdin= "/dev/null", stdout= "/dev/null", stderr= "/dev/null"):
      """Initializes daemon I/O streams"""

      self.stdin= stdin
      self.stdout= stdout
      self.stderr= stderr
      self.pidfile= pidfile
      super(Daemon, self).__init__()

   def fork(self):
      """Forks off the the process into the background"""

      try:
         pid= fork()
         if pid > 0:
            exit(0)
      except OSError, e:
         exit(1)

   def daemonize(self):
      """
      Forks the process(es) from the controlling terminal
      and redirects I/O streams for logging.
      """

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

      register(self.del_pid)
      self.set_pid()

   def set_pid(self, pid= None):
      """Creates PID file for the current daemon."""

      pid= str(getpid()) if not pid else str(pid)
      fh= open(self.pidfile, 'a')
      fh.write(pid + "\n")
      fh.close()

   def del_pid(self):
      """Removes the PID file."""

      remove(self.pidfile)

   def get_pids(self):
      """Reads the PID(s) from the PID file."""

      try:
          pids= open(self.pidfile, 'r').readlines()
      except IOError, e:
          pids= []

      return map(int, pids)

   def start(self):
      """
      Checks to see if the Daemon is already running
      (ie, already has a PID in the PID file) and if not
      then daemonizes the process(es).
      """

      pids= self.get_pids()
      if pids:
         exit(1)

      self.daemonize()
      self.run()

   def stop(self):
      """Stops/Kills the daemon process(es)"""
      
      pids= self.get_pids()
      if not pids:
         return

      for pid in pids:
         try:
            kill(pid, SIGTERM)
            sleep(0.1)
         except OSError, e:
            if str(e).find('No such process') > 0:
               continue

      if path.exists(self.pidfile):
         self.del_pid()
         exit(0)

   def restart(self):
      """Restarts the daemon process(es)."""

      self.stop()
      self.start()

   def run(self):
      """Override this method in your base class."""
      pass

class Queue(Daemon):
   """
   Creates remote methods for creating/deleteing streams,
   and accessing the Streams priority queue, data-store and
   properties.
   """

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

   def create_stream(self, **properties):
      """
      Creates stream and returns a unique stream identifier 
      to the caller.  If an identifier was passed in by the caller
      that identifier will be used to create the stream.  If the 
      stream already exists then now new stream is created and the
      existing stream is reused. 
      """

      stream= Stream(**properties)
      try:
         stream= self.streams[stream.id]
      except KeyError:
         self.streams[stream.id]= stream

      print "created stream", stream.id, properties

   def delete_stream(self, id):
      """Deletes the stream of the given identifier."""

      del self.streams[id]
      print "deleting stream", id

   def run(self):
      """Starts up the Queue server and starts listing for requests."""

      server= self.manager.get_server()
      print "running"
      server.serve_forever()


_declaration_order= 0
class Impetus(object):
   """
   Multi-threaded library for interfacing with the Impetus system. 
   Hides threading considerations from the client.  Determines callback
   methods through introspection if callbacks are not explicitly stated. 
   Decorators are provided for the client to indicate methods which run on 
   the remote nodes and local process methods which consume the results. 
   Creates a single stream per instance.  The client can created additional 
   streams through the Queue's remote methods via the "impq" handler. 
   """

   statuses= ("forked", "processed")

   def __init__(self, address, authkey, taskdir= "tasks", id= None, **properties):
      """Creates a stream and retrieves the streams priority queue and data-store."""

      self.id= id if id else str(uuid1())
      self.ipaddress= getipaddress()

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
      self.impq.create_stream(id= self.id, ipaddress= self.ipaddress, **properties)
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
      """Deletes the stream that was created during initialization."""

      self.impq.delete_stream(self.id)

   @staticmethod
   def node(target):
      """
      All methods that are to run on remote nodes must be staticmethods
      as the context of which the methods was defined can not be serialized.
      """

      return target

   @staticmethod
   def startup(target):
      """
      Sets up the startup method for the object to run as a thread.
      """

      def _process(self):

         target(self)

      global _declaration_order
      _process.order= _declaration_order
      _declaration_order+= 1
      return _process

   @staticmethod
   def shutdown(target):
      """
      Sets up the shutdown method to be excuted 
      after all threads have been terminated.  The 
      ready and errors parameters will contain a dict 
      of file-handles pointing to the results files
      (ie, ../tasks/<task_id>/<method>.ok, .err>
      for each @process method.
      """
   
      def _shutdown(self):

         target(self, self.ready, self.errors, self._progress)

      global _declaration_order
      _shutdown.order= _declaration_order
      return _shutdown

   @staticmethod
   def process(target):
      """
      Sets up method to run as a thread. The method will be 
      called with a list of currently available jobs that are 
      either in a ready or error state. The thread will die
      when it has finished processing all the jobs the previous
      @process method forked and when the previous @process method
      has terminatted. Each thread will be regulated so that all 
      threads have an eventual chance of executing.  Order of execution
      is not guarenteed and thread scheudling is handled by the 
      operating system.
      """

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
         
      global _declaration_order
      _process.order= _declaration_order
      _declaration_order+= 1
       
      return _process

   def fork(self, target, args, callback= None, priority= None, job_id= None, **properties):
      """
      Turns the target method to be forked into byte-code and creates a Job.  The Job
      is initialized to the starting state and placed the the streams priorty queue 
      for execution. 
      """

      current_thread= currentThread()
      
      job= Job(
         client= {"id": self.id, "ipaddress": self.ipaddress},
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
      """
      Keeps track of how many jobs the current
      thread has forked/processed.
      """

      with self._lock:
 
         progress= self._progress.get(name, dict([(s, 0) for s in self.statuses]))
         progress.update([(status, progress.get(status, 0) + count)])
         self._progress.update([(name, progress)])

   def _show_progress(self, current_thread):
      """Displays the current threads progress to stdout."""
 
      msg= []
      with self._lock:
         for thread in self.threads:
            progress= self._progress.get(thread.name, dict([(s, 0) for s in self.statuses]))
            msg.append("%s %s/%s -> " % (thread.name, progress.get("forked"), progress.get("processed")))

      print "thread: %s via %s" % (''.join(msg)[:-4], current_thread.name)
         
   def _thread_regulator(self, current_thread, previous_thread):
      """
      Regulates the current thread so all threads have an eventual 
      chance to run.  Thread scheduling is handled by the operating-system. 
      If the operating-system repeatively schedules the same thread than
      that thread is immediately put to sleep so the operating-system
      can schedule a new thread.
      """

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
      """
      Creates thread for the @process method as well 
      as error/ready file handlers for which all jobs
      in an error/ready state are written to. All threads
      are maintained in an internal thread list.
      """

      thread= Thread(target= method, name= name, args= (self, ))
      self.errors[name]= open(path.join(self.taskdir, '.'.join((name, "err"))), 'ab+')
      self.ready[name]= open(path.join(self.taskdir, '.'.join((name, "ok"))), 'ab+')

      return thread
 
   def _link_threads(self, threads):
      """
      Creates previous/next properties for each thread based
      on the threads declaration order.
      """
 
      for i in range(len(threads)):
 
        setattr(threads[i], "previous_thread", threads[i-1] if i > 0 else None)
        setattr(threads[i], "next_thread", threads[i+1] if i < len(threads)-1 else None)
  
      return threads[0]

   def _start_threads(self, threads):
      """Starts all threads based on their delcaration order."""

      [thread.start() for thread in threads]
      [thread.join() for thread in threads]

   def run(self):

      self.threads= [self._create_thread(name, method) for (name, method) in sorted(filter(lambda (name, method): type(method) == FunctionType and method.__name__ == "_process", self.__class__.__dict__.items()), key= lambda (name, method): method.order)]
      self._current_thread= self._link_threads(self.threads)
      self._start_threads(self.threads)

      [method(self) for (name, method) in sorted(filter(lambda (name, method): type(method) == FunctionType and method.__name__ == "_shutdown", self.__class__.__dict__.items()), key= lambda (name, method): method.order)]



class Worker(Process):
   """
   Worker process(es) are responsible for reading and 
   executing jobs from the stream to which it was assigned 
   ot process by the Node daemon.
   """

   statuses= ("idle", "busy")

   def __init__(self, id, stream_id, queue, store, properties):

      super(Worker, self).__init__()

      self.id= id
      self.alive= True
      self.stream_id= stream_id
      self.queue= queue
      self.store= store
      self.properties= properties


   def process(self):
      """
      Waits for a job from the streams queue then reads the jobs context from the 
      data-store.  The jobs method will then be de-serialized and executed. 
      The results, including any execution errors, are then returned to the
      data-store.  If no jobs appear on the streams queue within the designated
      timeout then the Worker will die. 
      """

      sig= None
      try:

         #print "processing stream", self.pid, self.stream_id, getppid()
         job_id= None
         job= {}

         try:
            (priority, job_id)= self.queue.get(block= True, timeout= self.properties.get("timeout", 15)).pop()
         except Empty:
            #print "stream idle", self.pid, self.stream_id
            self.alive= False
            return

         sig= signal(SIGINT, SIG_IGN)

         job= self.store.get(job_id)
         job.update([("status", "processing"), ("node", self.id), ("worker", getpid())])
         self.store.update([(job.get("id"), job)])

         print "processing job: %s,  %s, %s, %s" % (self.pid, job.get("id"), job.get("name"), job.get("status"))

         sleep(float(job.get("delay", 0.0)))
         method= FunctionType(mloads(decompress(decode(job.get("code")))), globals(), job.get("name"))
         result= method(job.get("args"))
         job.update([("result", result), ("status", "ready")])
         self.store.update([(job.get("id"), job)])

         print "completed job: %s, %s, %s, %s" % (self.pid, job.get("id"), job.get("name"), job.get("status"))

      except KeyboardInterrupt, e:
         print "worker shutting down", self.pid
         self.alive= False
      except Exception, e:

         sig= signal(SIGINT, SIG_IGN)

         (filename, linenumber, functionname, statement)= extract_tb(exc_info()[2])[-1]
         result= {"error": str(e), "name": functionname, "linenumber": linenumber, "statement": statement}
  
         job= self.store.get(job_id)
         print >> stderr, "error processing job:", self.pid, job.get("id"), job.get("name"), job.get("status"), str(e), functionname, linenumber, statement

         job.update([("result", result), ("status", "error")])
         self.store.update([(job.get("id"), job)])
  
      if sig:
         sig= signal(SIGINT, sig)

   def run(self):
      """
      Starts the worker processing jobs from the stream to which it was assigned.
      If a communication error occurs the Worker will attempt to re-establish 
      communication with the stream.
      """

      while self.alive:

         try:
            self.process()
         except (UnboundLocalError, EOFError, IOError, SocketError) as e:
            print >> stderr, "worker communication error:", self.stream_id, str(e)
            self.alive= False

         sleep(float(self.properties.get("frequency", 0.01)))


class Status(dict):
   """Container class to track status of a given object."""

   def __init__(self, **kwargs):

      kwargs["timestamp"]= kwargs.get("timestamp", datetime.utcnow())
      super(Status, self).__init__(**kwargs)

class Node(Daemon):
   """
   Node is started up on the remote instance via the bootstrapping process for that instance.
   The node is responsible for tracking active streams and managing the workers that process
   the jobs from thosee streams.  If a stream goes idle (ie, there are no more jobs in the streams
   queue and all workers have died) then node will stop tracking the stream.  If jobs re-appear
   on the stream Node will spawn new workers to process those jobs.  If a new stream appears 
   Node will spawn new workers to processs the jobs on that stream.  Each worker is an independent
   concurrent process that inherits the stream to process from the Node.
   """

   def __init__(self, queue, qauthkey, mpps= 5, dfs= None, dauthkey= None, logdir= curdir, piddir= curdir, **properties):
      """Initialize the Node's I/O stream and connect to the Queue and/or DFS."""     

      self.id= getipaddress()
      self.queue= queue
      self.qauthkey= qauthkey
      self.mpps= mpps
      self.dfs= dfs
      self.dauthkey= dauthkey
      self.properties= properties

      self.workers= {}
      self.alive= True
      self.start_time= datetime.utcnow()

      self.connect()
  
      super(Node, self).__init__(
         pidfile= path.join(piddir, self.__class__.__name__ + ".pid"),
         stdout= path.join(logdir, self.__class__.__name__ + ".out"),
         stderr= path.join(logdir, self.__class__.__name__ + ".err"),
         stdin= path.join(logdir, self.__class__.__name__ + ".in")
      )

   def connect(self):
      """Connects to the Queue and/or DFS on the host/port for whic hthe Node was intialized for."""

      self.qconnect()
      if None not in self.dfs:
         self.dconnect()

   def qconnect(self):
      """
      Attempts to connect to the Queue on the host/port for which the Node was initialized for.
      If no connection can be made, Node will keep attempting to connect until a connection
      can be established.  One connection is established the remove methods requested will be
      registered.
      """

      # remove connection from cache:
      # BaseProxy class has thread local storage which caches the connection
      # which is reused for future connections causing "borken pipe" errors on 
      # creating new manager.  
      if self.queue in BaseProxy._address_to_local:
         if hasattr(BaseProxy._address_to_local[self.queue][0], 'connection'):
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
            print "connected to queue", self.queue
            break
         except (EOFError, IOError, SocketError) as e:
            print "could not connect ...trying again", str(e)
            sleep(1)

   def dconnect(self):
      """
      Attempts to connect to the DFS on the host/port for which the Node was initialized for.
      If no connection can be made, Node will keep attempting to connect until a connection
      can be established. Once a connection can be established the remove methods requested
      will be registered.
      """

      # remove connection from cache:
      # BaseProxy class has thread local storage which caches the connection
      # which is reused for future connections causing "borken pipe" errors on
      # creating new manager.
      if self.dfs in BaseProxy._address_to_local:
         if hasattr(BaseProxy._address_to_local[self.dfs][0], 'connection'):
            del BaseProxy._address_to_local[self.dfs][0].connection

      # register handlers
      SyncManager.register("get_nodes")

      print "connecting to dfs", self.dfs
      while self.alive:

         try:
            self.impd= SyncManager(address= self.dfs, authkey= self.dauthkey)
            self.impd.connect()
            print "connected to dfs", self.dfs
            break
         except (EOFError, IOError, SocketError) as e:
            print "could not connect ...trying again", str(e)
            sleep(1)

   def process(self):
      """
      Starts tracking streams. When a stream is found which matches the Node's criteria
      workers are assigned to the stream and spawned to start processing jobs from the
      streams queue. When the stream goes idle and all workers for that stream have died
      the Node will stop tracking the stream until new jobs appear on the stream. Node will
      limit the amount of workers it can spawn for a stream to the configred amount.  If Node was
      started with the --dfs option then status updates about how many streams, workers and jobs
      are being processed will continually be sent back to DFS via a configurable rate.
      """

      print "processing", self.mpps

      # get list of streams proxies
      streams= self.impq.get_streams()
      streams_tracking= {} 

      # if reporting to DFS 
      # track nodes via shared dict else maintain local dict
      if hasattr(self, 'impd'):
         nodes= self.impd.get_nodes()

      idle_time= datetime.utcnow()

      while self.alive:

         # get list of streams to track we are not currently tracking
         streams_to_track= filter(lambda stream_id: stream_id not in streams_tracking.keys(), streams.keys())

         # if we are only tracking streams with specific properties
         # TODO: need to think through this more
         """
         if len(self.properties):

            # get properties for all the streams we are tracking
            stream_properties= [dict(self.impq.get_properties(stream_id)) for stream_id in streams_to_track]

            # filter out streams we want to track based on matching subsets of properties
            if "id" in self.properties:
               streams_to_track= map(lambda sp: sp.get("id"), filter(lambda sp: set(sp.items()).issubset(self.properties.items()), stream_properties))
            else:
               streams_to_track= map(lambda sp: sp.get("id"), filter(lambda sp: set(filter(lambda (property_name, property_value): property_name != 'id', sp.items())).issubset(self.properties.items()), stream_properties))
         """

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
            else:
               idle_time= datetime.utcnow()

         # create workers for streams we are currently tracking
         for (stream_id, (queue, store, properties)) in streams_tracking.items():

            qsize= queue.qsize()
            stream_workers= filter(lambda w: w.stream_id == stream_id, self.workers.values())
            num_stream_workers= min(qsize, self.mpps - len(stream_workers))
            if num_stream_workers:
               print "creating %s workers for %s" % (num_stream_workers, stream_id)
            for i in range(1, num_stream_workers + 1):
               worker= Worker(self.id, stream_id, queue, store, properties)
               worker.start()
               self.workers.update([(worker.pid, worker)])
               idle_time= datetime.utcnow()
               print "created worker", i, worker.pid, stream_id

         status= Status(
            mpps= self.mpps,
            streams= len(streams_tracking),
            workers= len(self.workers),
            starttime= self.start_time,
            uptime= datetime.utcnow() - self.start_time,
            lastactivity= idle_time,
            idletime= datetime.utcnow() - idle_time,
            properties= self.properties,
            pid= getpid()
         )

         if hasattr(self, 'impd'):
            nodes.update([(self.id, status)])

         sleep(1)

      self.stop()

   def stop(self):
      """
      Starts the shutdown process for the Node.  Waits for all
      workers to finish their activity. If Node was started with 
      the --dfs option then it will de-register itself with DFS.
      """

      # wait for workers to finish before shutting down
      print "shutting down node..."
      self.alive= False
      for (pid, worker) in self.workers.items():
         print "waiting for worker:", pid, worker.stream_id
         worker.join()

      # if reporting to DFS 
      # track nodes via shared dict else maintain local dict
      if hasattr(self, 'impd'):
         print "de-registering nodes with dfs"
         nodes= self.impd.get_nodes()
         try:
            del nodes[self.id]
         except KeyError:
            print >> stderr, "node not registered with dfs", self.id
 
      print "node shutdown complete."
      super(Node, self).stop()

   def run(self):
      """
      Starts processing the streams which match the given properties of the Node.
      If a connection error between Node and Queue/DFs occurs Node will continually
      try to re-establish connection.
      """

      while self.alive:
         try:
            self.process()
         except (KeyboardInterrupt, Exception) as e:
            if type(e) == KeyboardInterrupt:
               self.stop()
            else:
               print >> stderr, "queue/dfs communication error", str(e)
               self.connect()
         sleep(1)


class DFS(Daemon):
   """
   The Dynamic Frequency Scaler is responsible for 
   starting up instances and bootstrapping them with Node 
   to start processing jobs.  DFS will gather update statistics 
   from all the Nodes that are registered with it and spin up/down
   instances as needed. A configurable "billing period" can be set
   so DFS will make the most effective use of the instance for the 
   duration of the billing cycle.  Idle Nodes will be terminated
   when the end of the billing cycle is reached.  
   """

   billing_period= 3000
   idle_time= 300
   seconds_per_day= 86400

   def __init__(self, address, authkey, queue, qauthkey, mnon, mpps, ec2= None, bootstrap= None, deploy_key= None, logdir= curdir, piddir= curdir):
      """
      Initializes the available remote methods 
      and I/O streams to be used for the method.  
      Establishes connection to the Queue. 
      """
      super(DFS, self).__init__(
         pidfile= path.join(piddir, self.__class__.__name__ + ".pid"),
         stdout= path.join(logdir, self.__class__.__name__ + ".out"),
         stderr= path.join(logdir, self.__class__.__name__ + ".err"),
         stdin= path.join(logdir, self.__class__.__name__ + ".in")
      )

      self.id= getipaddress()

      self.address= address
      self.authkey= authkey

      self.queue= queue
      self.qauthkey= qauthkey

      self.mnon= mnon
      self.mpps= mpps
      self.bootstrap= bootstrap
      self.deploy_key= deploy_key

      self.ec2= ec2
      if self.ec2 != None:
         self.ec2= EC2Connection(*self.ec2.split(','))
         print "Connected to EC2", self.ec2

      self.nodes= dict()
      self.alive= True

      self.manager= SyncManager(address= self.address, authkey= self.authkey)
      self.manager.register("get_nodes", callable= lambda: self.nodes, proxytype= DictProxy)

      self.connect()

   def connect(self):
      """
      Attempts to connect to the Queue on the host/port for which the DFS was initialized for.
      If no connection can be made, DFS will keep attempting to connect until a connection
      can be established.  One connection is established the remove methods requested will be
      registered.
      """

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

   def get_bootstrap(self):
      
      fh= open(self.bootstrap, "r")
      template= Template(fh.read())
      fh.close()

      fh= open(self.deploy_key, "r")
      deploy_key= fh.read()
      fh.close()

      bootstrap= template.safe_substitute(
         deploy_key= deploy_key,
         queue= self.queue[0],
         qport= self.queue[1],
         dfs= self.address[0],
         dport= self.address[1],
         mpps= self.mpps
      )

      print bootstrap
         
      return bootstrap


   def startup_node(self, nodes, startup_count= 1):

      print "starting up nodes", startup_count

      bootstrap= self.get_bootstrap()
  
      try:
         image= self.ec2.get_image("")
         reservation= image.run(min_count= startup_count, max_count= startup_count, security_groups= [], key_name= "", instance_type= "", user_data= bootstrap, instance_initiated_shutdown_behavior= "terminate")
      except Exception, e:
         print >> stderr, "could not start instance(s): %s" % str(e)
         return

      # TODO: figure out better way todo this
      for instance in reservation.instances:
         while instance.state != "running":
            try:
               instance.update()
            except Exception, e:
               print >> stderr, "could not get update for instance"
               pass
            print "waiting for instance to startup"
            sleep(1)
         print "instance running", instance.private_ip_address

         while instance.private_ip_address not in nodes.keys():
            print "waiting for node to bootstrap", instance.private_ip_address
            sleep(1)
         print "node bootstrapped", instance.private_ip_address

   def shutdown_node(self, nodes, instance):

      print "shutting down node", instance.private_ip_address

      try:
         instance.stop()
      except Exception, e:
         print >> stderr, "could not stop instance", str(e)
         return

      while instance.state != "stopped":
         try:
            instance.update()
         except Exception, e:
            print >> stderr, "could not get update for instance"
            pass
         print "waiting for instance to stop"
      print "instance stopped"
      instance.terminate()

      nodes.pop(instance.private_ip_address)

   def monitor(self):
      """
      Starts collection and monitoring status messages received by all registered
      nodes as well as the Queue.  Will startup new instances and bootstrap them with Node
      as required.  When registered Nodes go idle DFS continue to make efficent use of the Node
      and will only terminate it at the end of the billing cycle.
      """

      nodes= self.manager.get_nodes()
      streams= self.impq.get_streams()
      streams_tracking= {}

      while self.alive: 

         streams_to_track= filter(lambda stream_id: stream_id not in streams_tracking.keys(), streams.keys())

         for stream_id in streams_to_track:
            if stream_id not in streams_tracking:
               streams_tracking.update([(stream_id, (self.impq.get_queue(stream_id), self.impq.get_store(stream_id), self.impq.get_properties(stream_id)))])

         # calculate our current status
         status= Status(
            number_of_workers= sum([node.get("workers") for node in nodes.values()]),
            max_number_of_workers= sum([node.get("mpps", 0) for node in nodes.values()]),
            number_of_jobs= sum([queue.qsize() for (queue, store, properties) in streams_tracking.values()]),
            number_of_store_items= sum([len(store) for (queue, store, properties) in streams_tracking.values()]),
            number_of_streams= len(streams_tracking.keys()),
            number_of_nodes= len(nodes)
         )

         print "Status:", status

         # TODO: figure out how to scale based on stream/node properties

         # if the we have more jobs than workers and we have less nodes running then we are allowed to start -- then scale up
         if status.get("number_of_jobs") > status.get("max_number_of_workers") and status.get("number_of_nodes") < self.mnon:

            # figure out how many nodes we should start
            number_of_nodes_to_start= 1
            if status.get("number_of_nodes") > 0:
               number_of_nodes_to_start= min(self.mnon - status.get("number_of_nodes"), (status.get("number_of_jobs") / self.mpps) + 1) 

            if self.ec2:
               self.startup_node(nodes, number_of_nodes_to_start)

         # cycle through existing nodes and check for shutdown stitution
         for node_id, node in nodes.items():

            # get the instance/node launchtime 
            instance= None
            starttime= node.get("starttime")
            if self.ec2:
               pass
               reservations= self.ec2.get_all_instances(filters= {"private_ip_address": node_id})
               (instance, )= reservations.instances 
               starttime= datetime.strptime(instance.launch_time, '%Y-%m-%dT%H:%M:%S.000Z')
               
            # calculate our time stamps and flags
            uptime= datetime.utcnow() - starttime
            idletime= datetime.utcnow() - node.get("lastactivity")
            end_of_billing_period= (uptime.days * self.seconds_per_day) + uptime.seconds >= self.billing_period
            idle= (idletime.days * self.seconds_per_day) + idletime.seconds >= self.idle_time

            print "Node:",  node, idle, end_of_billing_period

            # if we are at the end of the billing period and the node is idel -- then scale down
            if end_of_billing_period and idle:
               if self.ec2:
                  self.shutdown_node(nodes, instance)

         # stop tracking streams which are no longer active
         for stream_id in streams_tracking.keys():
            if stream_id not in streams.keys():
               streams_tracking.pop(stream_id)

         sleep(1)

   def startup(self):
      """
      Start the DFS processing running and 
      start listening for connections from Node.
      """

      pid= getpid()

      print "mananger process running", pid
      self.set_pid(pid= pid)

   def stop(self):
      """Close any connections to DFS and terminate the daemon."""

      print "shutting down ...please wait this may take up to 20 seconds"
      if hasattr(self.manager, "shutdown"):
         self.manager.shutdown()

      print "dfs has shutdown"
      super(DFS, self).stop()

   def run(self):
      """
      Start the DFS daemon running and start the monitoring process.  
      If a communication error accurs between DFS and the Queue 
      DFS will continue to try and re-establish connection.
      """

      print "dfs running", getpid()
      self.manager.start(self.startup)
      while self.alive:
         try:
            self.monitor()
            sleep(1)
         except KeyboardInterrupt:
            self.alive= False
         except Exception, e:
            print >> stderr, "queue communication error", str(e)
            self.connect()

      self.stop()

