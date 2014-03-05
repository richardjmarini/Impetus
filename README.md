#Impetus

##Auto-Scaling Asynchronous Distributed Processing Framework
impetus is an auto-scaling asynchronous distributed processing framework orginally designed for the purpose of building a distributed cralwer with machine learning analytics.  The Impetus Framework is the auto-scaling asynchronous distributed processing sub-system of that distributed crawler. 

###Queue

impetusqueue is a centralized context manager for the Impetus Framework.  Client applications connect to the queue via exposed methods that allow them to create processing streams.  A stream consists of a distributed Priority Queue, Data Store and associated Properties.

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
impetusnode is a multi-processing management daemon that tracks active Streams and spawns Worker processes that are responsible for executing Jobs within the Stream they are assigned to process.  The frequency at which the management daemon spawns new Worker processes is configurable property of the Stream which can be set by the "Client" when creating the "Stream". The number of Worker processes that can be spwaned per Stream is a configurtable property of the Node called "mpps" (maximum processes per stream) and can be set during startup via DFS (the Dynamic Frequency Scaler). 

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
impetusdfs is a Dynamic Frequency Scaling daemon (Auto-Scale) which monitors active streams and is responsible for starting up some number of instances (such as AWS EC2 instances) determined by analytic methods (eg, taking into consideration the number of active streams, number of waiting jobs in those streams, how many existing Nodes are currently active, etc..). DFS is responsible for bootstraping these instances. Bootstrapping is a configurable process which consists of at a minimum starting up impetusnode but can also include instally necessary security/deploy keys pulling the lastest version of the Impetus system and/or installing any required packages.

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




