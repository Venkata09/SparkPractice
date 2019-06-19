# What is scalability 


1) CPU's full utilization capability.
2) Cluster full utilization capability.
3) Memory Efficient
4) Minimize n/w consumption. 

# GroupBy or GroupByKey

This requires loading all the key values into memory at once to perform groupby. 

# control number of partition while reading data from Cassandra?

--------------------------------------------------------------------------

**spark.cassandra.input.split.size Default = 64** :- Approximate number of rows in a single Spark partition. The higher the value, the fewer Spark tasks are created. Increasing the value too much may limit the parallelism level.

**Experience :** I am trying to read 70 million rows and it is taking 15 minutes for it and the number of partitions it created was 21000 - which is too large. Then I executed the compaction on the cassandra table and then was able to control using this parameter. 


--------------------------------------------------------------------------

# How did I use the bucket-by in order to avoid the Spark Shuffle. Query Tuning & Performance. 

# Dynamic Resource Allocation - How we implemented in PROD environment. 
