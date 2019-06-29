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



