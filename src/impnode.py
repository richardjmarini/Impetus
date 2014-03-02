#!/usr/bin/env python

from Queue import Empty
from sys import stdout
from impq import Job
from multiprocessing.managers import SyncManager
from time import sleep
from impq import Node

node= Node("localhost", 50000, "test", max_processes= 25)
node.run()

