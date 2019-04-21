package concepts

/**
  * Created by vdokku on 2/16/2018.
  */
object Hive_SortMerge_BucketJoin {

  def main(args: Array[String]): Unit = {

    /*


    In SMB join in Hive,
    each mapper reads a bucket from the first table and the corresponding bucket from the second table


    and then a merge sort join is performed.


    Sort Merge Bucket (SMB) join in hive is mainly used as there is no limit on file or partition or table join.
    SMB join can best be used when the tables are large. In SMB join the columns are bucketed and sorted using the join columns.

    All tables should have the same number of buckets in SMB join.


     */



    /*

    MOVING Average :

    select t2.date, round(count(t1.users)) as users_inmarket
from (select distinct to_date(datehour) date
      from store.exposure
      where datehour >= '2014-05-01 00:00:00' ) t2
inner join (select to_date(datehour) date, count(distinct userpid) users
             from store.exposure
             where datehour between '2014-05-01 00:00:00' and '2014-05-31 23:59:59'
             group by to_date(datehour) ) t1
where t2.date is not null
  and datediff(t2.date, t1.date) between 0 and 21
group by t2.date


To run a moving average you can do something like this:



select t2.date, round(sum(t1.users)/3) as avg_unique_users
from (select distinct to_date(datehour) date
      from store.exposure
      where datehour >= '2014-05-25 00:00:00' ) t2
inner join (select to_date(datehour) date, count(distinct userpid) users
            from store.exposure
            where datehour between '2014-05-25 00:00:00' and '2014-05-31 23:59:59'
            group by to_date(datehour) ) t1
where t2.date is not null
  and datediff(t2.date, t1.date) between 0 and 2
group by t2.date


     */


  }


}
