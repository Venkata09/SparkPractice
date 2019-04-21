
spark.sql.codegen.wholeStage=false

+------------------+
|           sum(id)|
+------------------+
|499999999500000000|
+------------------+

Sum Values in 9.203336968s
spark.sql.codegen.wholeStage=true

+------------------+
|           sum(id)|
+------------------+
|499999999500000000|
+------------------+

Sum Values in 0.418880625s
spark.sql.codegen.wholeStage=false
== Parsed Logical Plan ==
'Join UsingJoin(Inner,List(id))
:- Range (0, 1000000000, step=1, splits=Some(4))
+- Range (0, 1000, step=1, splits=Some(4))

== Analyzed Logical Plan ==
id: bigint
Project [id#24L]
+- Join Inner, (id#24L = id#27L)
   :- Range (0, 1000000000, step=1, splits=Some(4))
   +- Range (0, 1000, step=1, splits=Some(4))

== Optimized Logical Plan ==
Project [id#24L]
+- Join Inner, (id#24L = id#27L)
   :- Range (0, 1000000000, step=1, splits=Some(4))
   +- Range (0, 1000, step=1, splits=Some(4))

== Physical Plan ==
Project [id#24L]
+- BroadcastHashJoin [id#24L], [id#27L], Inner, BuildRight
   :- Range (0, 1000000000, step=1, splits=4)
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]))
      +- Range (0, 1000, step=1, splits=4)

18/06/25 15:16:39 INFO CodeGenerator: Code generated in 4.130728 ms
Joined count: 1000
joinValues in 16.194260922s
spark.sql.codegen.wholeStage=true
18/06/25 15:16:39 INFO Executor: Finished task 0.0 in stage 6.0 (TID 18). 2522 bytes result sent to driver
18/06/25 15:16:39 INFO TaskSetManager: Finished task 0.0 in stage 6.0 (TID 18) in 21 ms on localhost (executor driver) (1/1)
18/06/25 15:16:39 INFO TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool 
18/06/25 15:16:39 INFO DAGScheduler: ResultStage 6 (count at WholeStagen_Example.scala:38) finished in 0.022 s
18/06/25 15:16:39 INFO DAGScheduler: Job 3 finished: count at WholeStagen_Example.scala:38, took 15.896159 s
== Parsed Logical Plan ==
'Join UsingJoin(Inner,List(id))
:- Range (0, 1000000000, step=1, splits=Some(4))
+- Range (0, 1000, step=1, splits=Some(4))

== Analyzed Logical Plan ==
id: bigint
Project [id#41L]
+- Join Inner, (id#41L = id#44L)
   :- Range (0, 1000000000, step=1, splits=Some(4))
   +- Range (0, 1000, step=1, splits=Some(4))

== Optimized Logical Plan ==
Project [id#41L]
+- Join Inner, (id#41L = id#44L)
   :- Range (0, 1000000000, step=1, splits=Some(4))
   +- Range (0, 1000, step=1, splits=Some(4))

== Physical Plan ==
*Project [id#41L]
+- *BroadcastHashJoin [id#41L], [id#44L], Inner, BuildRight
   :- *Range (0, 1000000000, step=1, splits=4)
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]))
      +- *Range (0, 1000, step=1, splits=4)

18/06/25 15:16:39 INFO ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
Joined count: 1000
18/06/25 15:16:39 INFO Executor: Finished task 0.0 in stage 9.0 (TID 27). 1495 bytes result sent to driver
joinValues in 0.660073416s

Process finished with exit code 0
