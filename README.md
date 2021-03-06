LogSplit with Akka Cluster
============

This is an example of using Akka Clustering to perform log distribution
between distributed servers.

I was hitching to test the clustering mode in Akka and wanted to find
a "real world" example that wasn't too big either. That's when I
stumbled over this _technical assignment_ and I thought it would be
a fun week end project.

Checkout the [annotated code](http://pierreandrews.net/LogSplit).

*While the code works, I am still working on the literal description
of what's going on.*

Problem Statement
-----------

You have a number of web servers (let's assume three) that receive
load balanced traffic for your web site. On each server, you log
each connection, including a cookie containing a `userid`. The logs you
collect are ordered by time.

Write a program that re-groups all the connection logs for one user on
the same server. You can place the grouped output on any one server or
on many servers, as long as all the log entries of one user are
together on the same physical server.

Example of a log file:

```
177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a" 
177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=f85f124a-05cd-11e3-8a11-a8206608c529" 
177.126.180.83 - - [15/Aug/2013:13:57:48 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"
```

Parsing wasn't the point of this exercise, the distribution of the file
is the interesting part. While this could be accomplished with hadoop,
I wanted to play with a lighter setup.

Possible Solution
--------------

The idea behind my approach is to use Akka+scala clustering to
distribute the logs between the servers. The code is extensively
documented, but please let me know if you have questions.

Once you are done reading this, you might want to start exploring from
the [main class: `LogSplitApp`](./LogSplitApp.html).

Design and Data Flow
------

Akka deals with the communication between servers and once all nodes
are registered on the cluster, the actors talk to each other
transparently, without having to know if an actor is on a remote
server or in the same JVM.

The data flow is divided in two main steps:

- data distribution between server
- local data sorting
 
The distribution of work assumes the following:

- logs are already ordered in descending date order on each server
- each server has a set of buckets of logs, numbered from 0 (with the
  newest date at the top) to N (with the oldest date at the bottom)
 
This is a common pattern in log systems and the assumptions help in
distributing work to separate workers (by just giving different files
to the workers). If we were dealing with a big giant file, we could
always chunk it between actors by use of nio channels.

(note that the descending/ascending order of logs in each file is
not very important, as long as they are sorted in one way or the other)
 
Data Distribution
--------
 
The data distribution step relies on two main type of actors:
 
- the *Reader* actor is responsible for loading a bucket from the
  local disk system, assigning a partition (server where all this
  user' logs will eventually live) to each log line and distributing
  these lines to the right *Writer*. One server will use a number of
  reader worker to serve lines from bucket files in parallel.
- the *Writer* actor is responsible from pulling log lines from
  *Readers* and writing them to disk in a file part assigned to a
  particular user. To help running the process in parallel, we run a
  set of *WriterWorker* on each server that serve a set of *Readers*.

The *reader* and *writer* do not need to know on which node the actor
they are talking to are located. They could be on the local node or
the remote nodes.

Each reader is assigned _one_ writer per node (including the local
node) and a set of local part files to read from.

Each writer can deal with multiple readers and usually manages a fixed
set of parts/bucket.

The reader is mostly passive, it will fill up a buffer and then wait
for the writers to pull log lines. Writers are either blocked writing
to the local disk or pulling work from the assigned readers.

Sorting
----

Because all part files are sorted in the original log files, when we
send them over to a single server, we keep them in separate sorted
part files for one user. At the end of the data distribution step, the
output directory will contain something like:

```
fcf011b9-b28d-4eab-b0d7-faa7695dbd74.0.3.part
fcf011b9-b28d-4eab-b0d7-faa7695dbd74.0.4.part
fd0a3813-7430-4b1e-a03d-0eb5acbee1c4.0.0.part
fd0a3813-7430-4b1e-a03d-0eb5acbee1c4.0.1.part
fd0a3813-7430-4b1e-a03d-0eb5acbee1c4.0.2.part
fd0a3813-7430-4b1e-a03d-0eb5acbee1c4.0.3.part
fd0a3813-7430-4b1e-a03d-0eb5acbee1c4.0.4.part
fd60aaf3-5dda-40bd-9474-7926b85e4197.0.0.part
fd60aaf3-5dda-40bd-9474-7926b85e4197.0.1.part
fd60aaf3-5dda-40bd-9474-7926b85e4197.0.2.part
fd60aaf3-5dda-40bd-9474-7926b85e4197.0.3.part
fd60aaf3-5dda-40bd-9474-7926b85e4197.0.4.part
fe967415-c8ec-46c0-8112-7f15136bf9b1.0.0.part
fe967415-c8ec-46c0-8112-7f15136bf9b1.0.1.part
fe967415-c8ec-46c0-8112-7f15136bf9b1.0.2.part
fe967415-c8ec-46c0-8112-7f15136bf9b1.0.3.part
fe967415-c8ec-46c0-8112-7f15136bf9b1.0.4.part
ff911373-c19e-4535-a50e-42195c17644f.0.0.part
ff911373-c19e-4535-a50e-42195c17644f.0.1.part
ff911373-c19e-4535-a50e-42195c17644f.0.2.part
ff911373-c19e-4535-a50e-42195c17644f.0.3.part
ff911373-c19e-4535-a50e-42195c17644f.0.4.part
```

where we have for each userid a set of part files per server (first
id) and per bucket from that server.

The sort step is responsible to merge the partial sorted files in one
single file for each user. The sorter actor doesn't need to know it's
running in a cluster and is only responsible for distributing parallel
sorting tasks to local actors.

Requirements
------

This is an sbt/scala project, so you will need java, scala and sbt
installed.

Building
------

you can build a package jar with

`sbt clean assembly`

you can also run the following commands directly with `sbt 'run-main...'`
without running the assembly.


Generating a Sample
-------

You can generate a sample of log files with the utils.LogGenerator.

`> java -cp LogSplit-assembly-0.1-SNAPSHOT.jar net.pierreandrews.utils.LogGenerator --output /tmp/logsplittest/servers --linePerFile 10000 --numUsers 1000 --numFiles 50`

This will generate three folders in /tmp/logsplittest/servers with 50
files each containing 10000 lines. The userids will be selected from a
random pool of 1000 users.

You can then either transfer each serverN file to a separate machine,
or just run three separate JVMs pointing to these separate folders.

Running the Code
------

The app is currently configured to run with three separate JVMs on the
same machine (localhost) on the ports 2550, 2551 and 2552. You can
start the three JVMs with:

`>  java -cp LogSplit-assembly-0.1-SNAPSHOT.jar  net.pierreandrews.LogSplitApp --port 2550 --output /tmp/logsplittest/outputs/server0 --input /tmp/logsplittest/servers/server0 --serverID 0`

`>  java -cp LogSplit-assembly-0.1-SNAPSHOT.jar  net.pierreandrews.LogSplitApp --port 2551 --output /tmp/logsplittest/outputs/server1 --input /tmp/logsplittest/servers/server1 --serverID 1`

`>  java -cp LogSplit-assembly-0.1-SNAPSHOT.jar net.pierreandrews.LogSplitApp --port 2552 --output /tmp/logsplittest/outputs/server2 --input /tmp/logsplittest/servers/server2 --serverID 2`

If you are going to run each JVM on separate machines, you need to
change the seeds, either with the `--seeds` arguments, e.g. for one
server:

`>  java -cp LogSplit-assembly-0.1-SNAPSHOT.jar net.pierreandrews.LogSplitApp --port 2550 --output /tmp/logsplittest/outputs/server0 --input /tmp/logsplittest/servers/server0 --serverID 0 --seeds 192.168.1.21:2550,192.168.1.23:2551,192.168.1.1:2550`

or by updating the `application.conf` settings. Given the limited time
and resources I had, I couldn't test this extensively on distributed
servers but it should work transparently.

Tweaking/Tuning
------

You can tune the number of workers, cache/buffer sizes, etc. by
changing the command line arguments. Please see LogsplitAppArgs for
documentation. The current settings work "OK" on my laptop, but are
not optimal.


Assumption and alternative solution
--------

The major assumption is that user-ids are evenly distributed between
each server, that is, there no one server where a id would appear a
lot more than another server. We assume some fair load-balancing
between servers etc. that would create such balanced logs.

This assumption is important as the architecture of the system evenly
distributes each users logs to each server. That means that with three
servers, around 2/3 of the logs of a node will be transferred over to
the other nodes in the cluster. If users are evenly distributed, this
is as good a solution as any; however, if logs are unbalanced to start
with the current data flow will create more data transfers than
required.

If we were to deal with unbalanced logs, it would be better to split
logs per user on each server first, then negotiate between each node
which one contains the most logs for each user and send the data over
from the smallest nodes to the bigger node.

Running this on an hadoop or spark cluster would definitely require a
lot less code scaffolding, even if it would require a larger server
architecture. Apache Samza (LinkedIn) also seemed to be a good
solution but required to setup kafka, zookeeper and YARN, which was
quite some overhead.
