# Cache & persistance secrets.





### This is what spark documentation says w.r.t CACHE & PERSISTANCE. 


### If I'm trying to cache a huge DataFrame (ex: 100GB table) and when I perform query on the cached DataFrame will it perform full table scan? How the spark will index data. Spark documentation says:


```scala

Spark SQL can cache tables using an in-memory columnar format by calling 
spark.catalog.cacheTable("tableName") or dataFrame.cache(). 
Then Spark SQL will scan only required columns and will automatically tune compression to minimize memory usage and GC pressure. 
You can call spark.catalog.uncacheTable("tableName") to remove the table from memory.



```

While some minor optimizations are possible, Spark doesn't index data at all. So in general case you should assume that Spark will perform a full data scan.

It can however apply early projections. So if the query uses only a subset of columns, Spark can access only these, which are required.

Columnar stores are good candidates for compression and Spark supports a number of compression schemes (RunLengthEncoding, DictEncoding, BooleanBitSet, IntDelta, LongDelta). Depending on the type of the column and the computed statistic Spark can automatically choose appropriate compression format or skip compression whatsoever.

In general compressions schemes used with columnar stores allow queries on compressed data and some (like RLE) can be used for efficient selections. At the same time you can increase amount data which can be stored in memory and accessed without fetching data from disk.



----------------------------------------------------------------------


Another istance about the cache 



```scala

We are using Spark 2.2.0. We have a 1.5 TB of data in a hive table. We have 80 node cluster where each node has about 512 GB RAM and 40 cores.

I am accessing this data using Spark SQL. With plain Spark SQL (without caching) simple command like getting distinct count of a particular column value takes about 13 sec. But when I run the same command after caching the table it takes more than 10 min. Not sure what is the issue?

export SPARK_MAJOR_VERSION=2
spark-shell --master yarn --num-executors 40 --driver-memory 5g --executor-memory 100g --executor-cores 5
spark.conf.set("spark.sql.shuffle.partitions", 10)
val df = spark.sql("select * from analyticalprofiles.customer_v2")
df.createOrReplaceTempView("tmp")
spark.time(spark.sql("select count(distinct(household_number)) from tmp").show())
>> Time taken: 13927 ms



import  org.apache.spark.storage.StorageLevel
val df2 = df.persist(StorageLevel.MEMORY_ONLY)
df2.createOrReplaceTempView("tmp2")
spark.time(spark.sql("select count(distinct(household_number)) from tmp2").show())
>> 1037482 ms ==> FIRST TIME - okay if this is more
spark.time(spark.sql("select count(distinct(household_number)) from tmp2").show())
>> 834740 ms  ==> SECOND TIME - Was expecting much faster execution ???




```



You can increase the number of executors and decrease the executor memory with the below formula

  SPARK_EXECUTOR_CORES (--executor-cores) : 5 

  Number of Executors (--num-executors) : (number of nodes) *  (number of cores) /(executor cores) -1 (for Application Master) = (80*40)/5 ~ 640-1 = 639

  SPARK_EXECUTOR_MEMORY (--executor-memory): Memory/(Number of Executors/Number of Nodes):  512/(639/80) ~ 64 GB
If you want to persist the dataframe use StorageLevel.MEMORY_AND_DISK_SER . If the Memory (RAM) is full , it will save in Disk.



