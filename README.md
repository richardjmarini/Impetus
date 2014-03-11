#Impetus

##Auto-Scaling Asynchronous Distributed Processing Framework
impetus is an auto-scaling asynchronous distributed processing framework originally designed for the purpose of building a distributed crawler with machine learning analytics.  The Impetus Framework is the auto-scaling asynchronous distributed processing sub-system of that distributed crawler.  This is the second version of the sub-system.  The original version still exists here: https://github.com/richardjmarini/Impetus1.git

###Queue

impetus.Queue is a centralized context manager for the Impetus Framework.  Client applications connect to the queue via the API which consists of a series of remote methods that allow the client to create processing streams. An application can create one or more streams.  Each stream has a unique identifier.  If an identifier is not provided by the client a unique identifier is created. The same stream can be used by one or more applications.  Each stream can be assigned various meta-data properties that describe the stream.  These meta-data properties are used by DFS (the dynamic frequency scaler) to assist in intelligent auto-scaling. A stream contains both a Priority Queue and a Key-Val data store. 

```
Usage: impetusqueue.py start|stop|restart|foreground

Options:
  -h, --help            show this help message and exit
  -q QUEUE, --queue=QUEUE
                        address to bind queue instance to
  -p PORT, --port=PORT  port to bind queue instance to
  -a AUTHKEY, --authkey=AUTHKEY
                        authorization key for queue instance
  -i PIDDIR, --piddir=PIDDIR
                        pid file directory
  -l LOGDIR, --logdir=LOGDIR
                        log file directory
```

The Queue Daemon provides the following exposed methods:
```
class Queue(Daemon)
 |  Method resolution order:
 |      Queue
 |      Daemon
 |      __builtin__.object
 |  
 |  Methods defined here:
 |  
 |  __init__(self, address, authkey, logdir='.', piddir='.')
 |  
 |  create_stream(self, **properties)
 |  
 |  delete_stream(self, id)
 |  
 |  get_streams(self)
 |
 |  get_store(self, id)
 |
 |  get_properties(self, id) 
 |
```

###Node

impetus.Node is a multi-processing management daemon that tracks active Streams and spawns Worker processes that are responsible for executing Jobs within the Stream they are assigned to process.  The frequency at which the management daemon spawns new Worker processes is configurable property that can be set within the meta-data properties of the stream by the client when creating the Stream. The number of Worker processes that can be spawned per Stream is a configurable property of the Node called "mpps" (maximum processes per stream) and can be set by command line options (or by DFS) when the Node starts up. When a Stream goes idle, a configurable timeout property of the Stream via the streams meta-data properties, Node will stop tracking the Stream.  Nodes can be started up manually via the command line (for by DFS) to track streams with certain meta-data properties.

```
Usage: impetusnode.py start|stop|restart|foreground

Options:
  -h, --help            show this help message and exit
  -q QUEUE, --queue=QUEUE
                        host of queue instance
  -p QPORT, --qport=QPORT
                        port of queue instance
  -a QAUTHKEY, --qauthkey=QAUTHKEY
                        authorization key for queue instance
  -d DFS, --dfs=DFS     host of dfs instance
  -o DPORT, --dport=DPORT
                        port of queue instance
  -u DAUTHKEY, --dauthkey=DAUTHKEY
                        authorization key for queue instance
  -i PIDDIR, --piddir=PIDDIR
                        pid file directory
  -l LOGDIR, --logdir=LOGDIR
                        log file directory
  -m MPPS, --mpps=MPPS  max number of processes per stream
  
  -s PROPERTIES, --properties=PROPERTIES
                        key/value pairs of stream properties, eg
                        id:<stream_id>,frequency:<stream_frequency>, etc..
```

###DFS
impetus.DFS is a Dynamic Frequency Scaling daemon (Auto-Scale) which monitors active streams and is responsible for starting up some number of instances (such as AWS EC2 instances) determined by analytic methods (eg, taking into consideration the number of active streams, number of waiting jobs in those streams, how many existing Nodes are currently active, and the meta-data properties of the streams and existing nodes, etc..). DFS is responsible for bootstrapping these instances. Bootstrapping is a configurable process which consists of at a minimum starting up impetusnode but can also include installing necessary security/deploy keys, pulling the latest version of the Impetus system and/or installing any required packages. 

*Note: the DFS component is still being developed. For a functional example, which does not support scaling via meta-data properties, can be seen in an earlier version of the framework in a separate github project here: https://github.com/richardjmarini/Impetus1/blob/master/src/dfs.py  I've also used this version to bootstrap nodes with Selenium and Virtual Frame Buffer (Xvfb) to run headless selenium so crawling via selenium can be achieved for sites which hide links under asynchronous javascript calls, etc..* 

```
Usage: impetusdfs.py start|stop|restart|foreground

Options:
  -h, --help            show this help message and exit
  -d DFS, --dfs=DFS     address to bind dfs instance to
  -o PORT, --port=PORT  port to bind dfs instance to
  -u AUTHKEY, --authkey=AUTHKEY
                        authorization key for dfs
  -q QUEUE, --queue=QUEUE
                        host of queue instance
  -p QPORT, --qport=QPORT
                        port of queue instance
  -a QAUTHKEY, --qauthkey=QAUTHKEY
                        authorization key for queue instance
  -i PIDDIR, --piddir=PIDDIR
                        pid file directory
  -l LOGDIR, --logdir=LOGDIR
                        log file directory

```

###Impetus (client)
impetus.Impetus is a multi-threaded interface to the impetus system that allows for easy creation and management of a processing Streams and it's Jobs.  It allows the developer to define local methods which can then be forked with an associated callback method.  If no callback method is provided then the next defined method will be considered the callback method. Jobs are created from the forked methods which are marshalled into python bytecode and assoicated with various properties (eg, arguments for the method, state information, Job identifiers, timestamps, etc..) and feed into their assoicated Stream to await processing by a Node. The client can set various properties (eg, frequency rate of the Stream, Job delay, Job priority, etc..).  Each instance of impetus.Impetus has it's own processing stream. 

```
class Impetus(__builtin__.object)
 |  Methods defined here:
 |  
 |  __del__(self)
 |  
 |  __init__(self, address, authkey, taskdir='tasks', id=None, **properties)
 |  
 |  fork(self, target, args, callback=None, priority=None, job_id=None, **properties)
 |  
 |  run(self)
 |  
 |  ----------------------------------------------------------------------
 |  Static methods defined here:
 |  
 |  node(target)
 |  
 |  process(target)
 |  
 |  shutdown(target)
 |  
 |  startup(target)

```

Here is a "helloworld" example of using the Client API using inheritance:
```
class Helloworld(Client):

   def __init__(self, address, authkey, taskdir= curdir, id= None):

      self.address= address
      super(Helloworld, self).__init__(self.address, authkey, taskdir, id)

   @Impetus.node
   def pow(i):
      return i * i

   @Impetus.startup
   def start(self):

      for i in range(0, 100):
         self.fork(self.pow, args= i)
         sleep(0.025)

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
```

How to run a local version (ie, without DFS/auto-scaling) of the Helloworld example: 

```
$ ./impetusqueue.py start
$ ./impetusnode.py start
$ ./helloword.py
```

Example helloworld output:
```
My Id: b8b81d16-a47c-11e3-8b68-3ca9f46675e0
thread: start 0/0 -> stage1 0/0 via stage1
Total: 328350
Errors: 0
thread: start 100/0 -> stage1 0/100 via stage1
2014-03-05 15:42:19.380801 stage1 completed
shutting down
```

All Job and context information is saved to the tasks directory.  A file is created for each "process" method within the client application and stores each Job processed by the corresponding method. Each file contains one Job per line.  Each Job is a b64encoded zlib compressed json structuers.
```
$ ls -l ../tasks/b8b81d16-a47c-11e3-8b68-3ca9f46675e0/
total 72
-rw-r--r-- 1 rmarini rmarini     0 Mar  5 10:42 stage1.err
-rw-r--r-- 1 rmarini rmarini 38052 Mar  5 10:42 stage1.ok
-rw-r--r-- 1 rmarini rmarini     0 Mar  5 10:42 start.err
-rw-r--r-- 1 rmarini rmarini     0 Mar  5 10:42 start.ok
 
$ head -n 1 ../tasks/b8b81d16-a47c-11e3-8b68-3ca9f46675e0/stage1.ok  | base64 --decode | zlib-flate -uncompress

{"status": "ready", "name": "pow", "created": "2014-03-05 15:42:14.633071", "args": 31, "delay": 0, "callback": "stage1", "client": "b8b81d16-a47c-11e3-8b68-3ca9f46675e0", "result": 961, "id": "b9802310-a47c-11e3-9a1b-3ca9f46675e0", "transport": null, "code": "eJxLZmRgYABhJiB2BuJiDiBRA0YiwRogGT8NIMEAZpaAiEwIH0QU8wMJPf2M1Jyc/PL8opwUvYLKEmagWEF+uTpIHmQqAxMACdINGA=="}

If an error occours "result" will be the following error structure:

{"error": str(e), "name": functionname, "linenumber": linenumber, "statement": statement}

The forked method is also stored within the structure as a base64encoded ziplib compresed python bye code:
eJxLZmRgYABhJiB2BuJiDiBRA0YiwRogGT8NIMEAZpaAiEwIH0QU8wMJPf2M1Jyc/PL8opwUvYLKEmagWEF+uTpIHmQqAxMACdINGA==

>>> from base64 import b64decode
>>> from zlib import uncompress
>>> from marshal import loads
>>> loads(decompress(b64decode("eJxLZmRgYABhJiB2BuJiDiBRA0YiwRogGT8NIMEAZpaAiEwIH0QU8wMJPf2M1Jyc/PL8opwUvYLKEmagWEF+uTpIHmQqAxMACdINGA==")))
<code object pow at 0x7f683b86c230, file "./helloworld.py", line 39>
```
*Note:  Currently, the "transport" of the context information between client, Queue and Node happens via an in memory data store within the Queue. The original version had a concept of "transports" which allowed the transport of context information to occur either via the in memory data store, filesystem or an s3bucket. The original version also allowed the option of using Memcache as the in memory data store.  I have not yet currently implemented these concepts in this newer version.  Mainly, because I never used them and didn't see a need for them.  The original version can be found here: https://github.com/richardjmarini/Impetus1.git*


Example Node output:

*Node logs all activity in Daemon mode:*
```
$ ls -ltr ../log/Node*
-rw-rw-rw- 1 rmarini rmarini     0 Mar  4 13:35 ../log/Node.in
-rw-rw-rw- 1 rmarini rmarini    27 Mar  4 13:37 ../log/Node.err
-rw-rw-rw- 1 rmarini rmarini 22623 Mar  4 13:37 ../log/Node.out
```

*In foreground mode Node prints to stdout/stderr:*
```
connecting to queue ('localhost', 50000)
starting node in foreground mode
max processes per stream 5
tracking stream b8b81d16-a47c-11e3-8b68-3ca9f46675e0
creating 5 workers for b8b81d16-a47c-11e3-8b68-3ca9f46675e0
created worker 1 5632 b8b81d16-a47c-11e3-8b68-3ca9f46675e0
created worker 2 5633 b8b81d16-a47c-11e3-8b68-3ca9f46675e0
created worker 3 5634 b8b81d16-a47c-11e3-8b68-3ca9f46675e0
created worker 4 5636 b8b81d16-a47c-11e3-8b68-3ca9f46675e0
created worker 5 5637 b8b81d16-a47c-11e3-8b68-3ca9f46675e0
processing job: 5637,  b8fbef78-a47c-11e3-9a1b-3ca9f46675e0, pow, processing
completed job: 5637, b8fbef78-a47c-11e3-9a1b-3ca9f46675e0, pow, ready
etc...
stopped tracking stream b8b81d16-a47c-11e3-8b68-3ca9f46675e0
```

###To do:
1. Finish DFS.
2. Create a logging daemon:  Currently all print statements are sent to stdout/stderr when the components are run in foreground mode.  When the components are run in background mode print statements are sent to their corresponding log files instead of stdout/stderr. I would like to have the Daemon (izer) class re-route those output streams to a centralized logger vs retro-fitting each classes to use a logging method.

###Other Notes:
I've attempted to build a similar framework using Celery.  I was able to dynamically "fork" methods in the same manner by setting up the tasks.py file like this:

```
from marshal import loads
from celery import Celery
from types import FunctionType

app= Celery('tasks', broker= 'amqp://guest@localhost//', backend= 'amqp')

@app.task
def task(name, code, args):

   handler= FunctionType(loads(code), globals(), name)
   results= handler(args)

   return results
```

The context information could be written to an external key-store such as redis or memcache.  However, there are other features such as, multiple streams (which would be akin to having multiple Celery() instances), Client controlled execution rates and auto-scaling based on various properties which frustrated me when trying to use Celery and various other packages -- but that just may be my own issue ;-)
