package examples

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * Created by vdokku on 6/18/2017.
  */
object Average_UDAF extends UserDefinedAggregateFunction {


  override def inputSchema: StructType = {
    new StructType().add("myinput", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("mycnt", LongType).add("mysum", DoubleType)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0l)
    buffer.update(1, 0d)
  }


  // Partitions & Combine
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getAs[Long](0) + 1)
    buffer.update(1, buffer.getAs[Double](1) + input.getAs[Double](0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Long](0) + buffer2.getAs[Long](0))
    buffer1.update(1, buffer1.getAs[Double](1) + buffer2.getAs[Double](1))
  }

  override def evaluate(buffer: Row): Any = {
    val avg = buffer.getAs[Double](1) / buffer.getAs[Long](0)
    f"$avg%.2f".toDouble
  }
}
