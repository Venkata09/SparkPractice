package api

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * Created by vdokku on 1/29/2018.
  */
object PairRDDFunctionsAndExamples {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< PairRDDFunctionsAndExamples>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //Aggregate PairRDD


    def meanByKey(rdd: RDD[(Int, Int)]): RDD[(Int, Double)] = {
      rdd
        .mapValues(v => (v, 1))
        .reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))
        .mapValues {
          case (sum, count) => sum / count.toDouble
        }
    }

    def combineByKey(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {
      // Calculate every key's sum & count of value
      rdd1.combineByKey(
        i => (i, 1), // initial value
        (t: (Int, Int), i) => (t._1 + i, t._2 + 1), // apply on every element in partition
        (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._2, t1._2 + t2._2) // apply between partitions
      )
    }


    //    ActionOnPairRDD


    def countByKey(rdd: RDD[(Int, Int)]): Map[Int, Long] = {

      /*

      if an RDD is applied the CountByKey operation ==> MAP[Key, Value]
      if an RDD is applied the countByValue operation ==> MAP[(KEY1, KEY2), Value]

      */


      rdd.countByKey()
    }

    def collectAsMap(rdd: RDD[(Int, Int)]): Map[Int, Int] = {
      rdd.collectAsMap()
    }

    def lookup(rdd: RDD[(Int, Int)], key: Int): List[Int] = {
      rdd.lookup(key).toList // This looks similar to the MAP.GET function.
    }


    //    CreatePairRDD

    def create(rdd: RDD[String]): RDD[(String, String)] = {
      rdd.map(s => (s.split(" ")(0), s))
    }

    //GroupPairRDD


    def groupByKey(rdd: RDD[(Int, Int)]): RDD[(Int, List[(Int, Int)])] = {
      rdd.groupBy(_._1).mapValues(_.toList)
    }

    def cogroup(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Iterable[Int], Iterable[Int]))] = {
      // like first rdd1.groupBy(_._1).join(rdd2.groupBy(_._1))
      rdd1.cogroup(rdd2)
    }


    //    JoinPairRDD

    def innerJoin(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Int, Int))] = {
      // only join keys both exists in 2 rdds, and return key -> (value_in_rdd1, value_in_rdd2)
      rdd1.join(rdd2)
    }

    def rightJoin(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Option[Int], Int))] = {
      rdd1.rightOuterJoin(rdd2)
    }

    def leftJoin(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, (Int, Option[Int]))] = {
      rdd1.leftOuterJoin(rdd2)
    }


    //    PartitionPairRDD


    // ex: partition = new HashPartitioner(100)
    def partition[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], partitioner: Partitioner): RDD[(K, V)] = {
      // The reason to do partition on pair rdd is to avoid data shuffle
      // Pair rdd is partition by its' keys
      // Once partition finished and cached, we can do some job on keys without shuffling data, such as join.
      rdd.partitionBy(partitioner).persist()
    }

    def join[K: ClassTag, V: ClassTag](partitioned: RDD[(K, V)], toJoin: RDD[(K, V)]): RDD[(K, (V, V))] = {
      // Only the pair rdd "toJoin" will be shuffled cause the pair "partitioned" is partitioned
      // Attention: if "partitioned" hasn't been persisted, each time using this pair will cause shuffle4
      partitioned.join(toJoin)
    }

    def mapValue[K: ClassTag, V: ClassTag](partitioned: RDD[(K, V)]): RDD[(K, String)] = {
      // mapValue will not destroy the partition info on a partitioned pair rdd, but map will
      // same thing on flatMapValue & flatMap
      partitioned.mapValues(v => v.toString)
    }

    def pageRank(partitioned: RDD[(String, Seq[String])]): RDD[(String, Double)] = {
      // We use mapValues so initRanks restore its parent's("partitioned") partition info.
      val initRanks = partitioned.mapValues(v => 1.0)
      (0 until 10).foldLeft(initRanks) { case (ranks, _) =>
        // Action join won't consume too much cause pair rdd is partitioned
        val contributions = partitioned
          .join(ranks)
          .flatMap {
            case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
          }

        // reduceByKey will create partition info and mapValues won't destroy it,
        // so when the result of mapValues to join "partitioned" won't cause shuffle
        contributions.reduceByKey((c1, c2) => c1 + c2).mapValues(v => 0.15 + 0.85 * v)
      }
    }


    //    TransformPairRDD

    // group rdd with same key and apply function on each element
    def reduceByKey(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
      rdd.reduceByKey { case (i1, i2) => i1 + i2 }
    }


    /* def mapValue(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
       rdd.mapValues(_ + 1)
     }*/

    def flatMapValues(rdd: RDD[(Int, List[Int])]): RDD[(Int, Int)] = {
      rdd.flatMapValues(l => l)
    }

    def substractByKey(rdd1: RDD[(Int, Int)], rdd2: RDD[(Int, Int)]): RDD[(Int, Int)] = {
      // remove all keys in rdd1 which exist in rdd2
      rdd1.subtract(rdd2)
    }

    def filterByValue(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
      rdd.filter(_._2 > 5)
    }


  }

}
