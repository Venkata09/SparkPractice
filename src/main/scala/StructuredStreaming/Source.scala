package StructuredStreaming

import org.apache.spark.sql.execution.streaming.{LongOffset, Offset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by vdokku on 1/3/2018.
  */
class Source(val schema: StructType, sqlContext: SQLContext) {

  var offset = LongOffset(0)

  def getOffset: Option[Offset] = {
    println(">>> getOffset")
    offset = offset + 1
    Some(offset)
  }

  def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    println(s">>> getBatch($start, $end)")
    val n = end.asInstanceOf[LongOffset].offset
    sqlContext.sparkSession.range(n).toDF
  }


  def stop(): Unit = {}
}

/*
class AmadeusDataSourceRegister
  extends StreamSourceProvider
    with DataSourceRegister {

  println("We got instantiated")

  override def shortName() = "amadeus"

  lazy val defaultSchema = new StructType().add("id", IntegerType)

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    (shortName, schema.getOrElse(defaultSchema))
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    new Source(schema.getOrElse(defaultSchema), sqlContext)
  }
}*/
