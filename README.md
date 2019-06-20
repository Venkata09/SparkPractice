# SparkPractice


```scala

The number of tasks for a job is:

( no of your stages * no of your partitions )

```


```scala

Regarding the small files problem. 


While HDFS is processing, Each file ==> each map task. 
In HADOOP the files block-size = 64 MB and when the map operation reads the data, 
it expects the file size to be more than 64 MB.


If the files in HDFS are less than the 64 MB and there are numerous files, 
it can be considered as a SMALLL FILES PROBLEM .


```

