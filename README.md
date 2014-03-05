#Impetus

##Auto-Scaling Asynchronous Distributed Processing Framework
impetus is an auto-scaling asynchronous distributed processing framework orginally designed for the purpose of building a distributed cralwer with machine learning analytics.  The Impetus Framework is the auto-scaling asynchronous distributed processing sub-system of that distributed crawler.  This is the second version of the sub-system.  The orginal version still exists here: https://github.com/richardjmarini/Impetus1.git

###Queue

impetus.Queue is a centralized context manager for the Impetus Framework.  Client applications connect to the queue via exposed methods that allow them to create processing streams.  A stream consists of a distributed Priority Queue, Data Store and associated Properties.

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

###Node
impetus.Node is a multi-processing management daemon that tracks active Streams and spawns Worker processes that are responsible for executing Jobs within the Stream they are assigned to process.  The frequency at which the management daemon spawns new Worker processes is configurable property of the Stream which can be set by the "Client" when creating the "Stream". The number of Worker processes that can be spwaned per Stream is a configurable property of the Node called "mpps" (maximum processes per stream) and can be set during startup via DFS (the Dynamic Frequency Scaler). When a Stream goes idle, a configurable timeout propertiy of the Stream, Node will stop tracking the Stream.

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
```

###DFS
impetus.DFS is a Dynamic Frequency Scaling daemon (Auto-Scale) which monitors active streams and is responsible for starting up some number of instances (such as AWS EC2 instances) determined by analytic methods (eg, taking into consideration the number of active streams, number of waiting jobs in those streams, how many existing Nodes are currently active, etc..). DFS is responsible for bootstraping these instances. Bootstrapping is a configurable process which consists of at a minimum starting up impetusnode but can also include instally necessary security/deploy keys pulling the lastest version of the Impetus system and/or installing any required packages. 

*Note: the DFS component is still being developed. For a functional exmaple see an earlier version of the framework in a seperate github project here: https://github.com/richardjmarini/Impetus1/blob/master/src/dfs.py  I've also used this version to bootstrap nodes with Selenium and Vritual Frame Buffer (Xvfb) to run headless selenium so crawling via selenium can be achived for sites which hide links under asynchronous javascript calls, etc..* 

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

###Client
impetus.Client is a multi-threaded Client API to the impetus system that allows for easy creation and management of Streams and Jobs within the impetus system.  It allows the developer to define local methods which can then be forked with an associated callback method.  If no callback method is provided then the next defined method will be considered the callback method.  Jobs are created from the forked methods which are marshalled into python bytecode and associated with various properties (eg, arguments for the method, state information, Job identifiers, timestamps, etc..) and feed into their assoicated Stream to await processing by a Node. The API allows the client to set various properties (eg, frequency rate of the Stream, Job delay, Job priority, etc..)

Here is a "helloworld" example of using the Client API:
```
class Helloworld(Client):

   def __init__(self, address, authkey, taskdir= curdir, id= None):

      self.address= address
      super(Helloworld, self).__init__(self.address, authkey, taskdir, id)

   @Client.node
   def pow(i):
      return i * i

   @Client.startup
   def start(self):

      for i in range(0, 100):
         self.fork(self.pow, args= i)
         sleep(0.025)

   @Client.process
   def stage1(self, ready, errors):

      total= 0
      for job in ready:
         total+= job.get('result')
         sleep(0.025)

      print "Total:", total
      print "Errors:", len(errors)

   @Client.shutdown
   def stop(self, ready, errors, progress):

      print "shutting down"
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

The Client API also stored all Job and context information within the tasks directory.  A file is created for each "process" method within the Client application and stores each Job processed by the corresponding method. Each file contains one Job per line.  Each Job is a b64encoded zlib compressed json structions.
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
*Note:  Currently, the "transport" of the context information between Client, Queue and Node happens via an in memory data store within the Queue. The orignal version had a concept of "transports" which allowed the transport of context information to occur either via the in memory data store, filesystem or an s3bucket. The orginal version also allowed the option of using Memcache as the in memory data store.  I have not yet currently impliemented these concepts in this newer version.  Mainly, because I never used them and didn't see a need for them.  The original version can be found here: https://github.com/richardjmarini/Impetus1.git*


Example Node output in foreground mode:
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

Node also logs all activity in Daemon mode: 
```
$ ls -ltr ../log/Node*
-rw-rw-rw- 1 rmarini rmarini     0 Mar  4 13:35 ../log/Node.in
-rw-rw-rw- 1 rmarini rmarini    27 Mar  4 13:37 ../log/Node.err
-rw-rw-rw- 1 rmarini rmarini 22623 Mar  4 13:37 ../log/Node.out
```

###Other Notes:
I've attemped to build a simular framework using Celerey.  I was able to dynamically "fork" methods in the same manner by setting up the tasks.py file like this:

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

The context information could the be written to an external data-store such as redis or memcache.  However, there are other features such as, multiple streams which would be akin to having multiple Celery() instances, Client controlled worker spawn frequency, and methods for implementing auto-scaling which frustrated me -- but that just may be my own issue ;-)
