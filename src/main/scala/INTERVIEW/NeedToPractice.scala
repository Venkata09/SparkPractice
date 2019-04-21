package INTERVIEW

/*



You have two files in hdfs one having date range with two columns start date and end date and another having two column with date and visitors field. You have to write a spark code which gives date range having maximum no. of visitors using that two files.


Answer:

Let me do it using mapreduce instead of spark, I don't know much of spark. I would assume the concept is similar though.

file1: <startDate endDate>
1 5
8 20
file2: <date visitor>
2 5
3 8
10 120

So answer should be 8-20 since in that date range we have max visitor.

Assumption : since file1 just has ranges it should be a small file and can be loaded into the distributed cache of hadoop.

Now execute map code for file2, the code should do following :
1) read file2 and for each date , see which range it belongs to from the distributed cache and increment the counter for that time range.
eg
for 2 increment counter for 1_5
for 3 increment counter for 1_5
for 10 increment counter for 8_20
3) output <range , counter> as map output
4) In reduce add all the counters for every range.

Also - we need to add total order sorting so that overall output of all reducers are sorted.




 */
object NeedToPractice {

  def main(args: Array[String]): Unit = {


  }
}
