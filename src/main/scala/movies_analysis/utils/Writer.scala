package movies_analysis.utils

import org.apache.spark.sql.DataFrame

object Writer {
  def write(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.parquet(path)
  }
}
