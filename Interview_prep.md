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

# How the CACHE & PERSIST will be used or applied in the SPARK. 


# SQL Programming Question

How to get top N rows with some conditions


```sql 

SELECT product_id,
         site,
         category_id,
         session_time,
         sum(cast(coalesce("#clicks", 0) AS bigint)) AS clicks
FROM df
WHERE site IN ('com', 'co')
        AND session_time = DATE('2020-02-27')
GROUP BY  product_id, site, session_time, category_id
ORDER BY clicks desc
LIMIT 10


I want to see the top 10 product_id for each site and category_id based on the clicks.

product_id, site, category_id, session_time, clicks




SELECT *
FROM (
    SELECT 
        product_id,
        site,
        category_id,
        session_time,
        SUM("#clicks") clicks,
        RANK() OVER(PARTITION BY site, category_id ORDER BY sum("#clicks") DESC) rn
    FROM df
    WHERE 
        site IN ('com', 'co')
        AND session_time = DATE('2020-02-27')
    GROUP BY  product_id, site, session_time, category_id
) t
WHERE rn <= 10
ORDER BY site, category, clicks desc

```