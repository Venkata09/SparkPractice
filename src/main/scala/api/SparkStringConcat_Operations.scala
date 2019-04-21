package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/*



+-------+--------------------+--------------------+
| Dev_No|               model|              Tested|
+-------+--------------------+--------------------+
|BTA16C5|          Windows PC|                   N|
|BTA16C5|                 SRL|                   N|
|BTA16C5|     Hewlett Packard|                   N|
|CTA16C5|     Android Devices|                   Y|
|CTA16C5|     Hewlett Packard|                   N|
|4MY16A5|               Other|                   N|
|4MY16A5|               Other|                   N|
|4MY16A5|              Tablet|                   Y|
|4MY16A5|               Other|                   N|
|4MY16A5|           Cable STB|                   Y|
|4MY16A5|               Other|                   N|
|4MY16A5|          Windows PC|                   Y|
|4MY16A5|          Windows PC|                   Y|
|4MY16A5|         Smart Watch|                   Y|
+-------+--------------------+--------------------+

Now I need the output to be like below.

Which is similar to create data frame with a new column called Tested_devices and populate the column with
values for each Dev_No list all the models associated when the Tested FLAG is Y.

If the tested flag is not Y then the column will be EMPTY.



+-------+--------------------+--------------------+------------------------------------------------------+
| Dev_No|               model|              Tested|                                        Tested_devices|
+-------+--------------------+--------------------+------------------------------------------------------+
|BTA16C5|          Windows PC|                   N|                                                      |
|BTA16C5|                 SRL|                   N|                                                      |
|BTA16C5|     Hewlett Packard|                   N|                                                      |
|CTA16C5|     Android Devices|                   Y|                                       Android Devices|
|CTA16C5|     Hewlett Packard|                   N|                                                      |
|4MY16A5|               Other|                   N|                                                      |
|4MY16A5|               Other|                   N|                                                      |
|4MY16A5|              Tablet|                   Y| Tablet, Cable STB,Windows PC, Windows PC, Smart Watch|
|4MY16A5|               Other|                   N|                                                      |
|4MY16A5|           Cable STB|                   Y| Tablet, Cable STB,Windows PC, Windows PC, Smart Watch|
|4MY16A5|               Other|                   N|                                                      |
|4MY16A5|          Windows PC|                   Y| Tablet, Cable STB,Windows PC, Windows PC, Smart Watch|
|4MY16A5|          Windows PC|                   Y| Tablet, Cable STB,Windows PC, Windows PC, Smart Watch|
|4MY16A5|         Smart Watch|                   Y| Tablet, Cable STB,Windows PC, Windows PC, Smart Watch|
+-------+--------------------+--------------------+------------------------------------------------------+






Created by vdokku on 1/29/2018.
  */
object SparkStringConcat_Operations {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder().master("local[4]")
      .appName("<<<<<<<<<<<< Spark String CONCAT Operation >>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    val stringConcatData = spark.read.option("header", "true").
      csv("src/main/resources/spark_data/spark_string_concatenation.csv")


    stringConcatData.show()


    /* Approach 1 */

    /* One way to do this without using a **udf** or any **Window functions** is to create a second temporary
    DataFrame with the collected values and then join this back to the original DataFrame. */


    /*First group by both Dev_No and Tested and aggregate using concat_ws and collect_list. After aggregation, filter the DataFrame for tested devices only.*/
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val aggregatedDF_Completed = stringConcatData.groupBy("Dev_No", "Tested")
      .agg(concat_ws(", ", collect_list("model")).alias("Tested_devices"))

    /*

+-------+------+------------------------------------------------------+
|Dev_No |Tested|Tested_devices                                        |
+-------+------+------------------------------------------------------+
|4MY16A5|N     |Other, Other, Other, Other                            |
|CTA16C5|N     |Hewlett Packard                                       |
|BTA16C5|N     |Windows PC, SRL, Hewlett Packard                      |
|CTA16C5|Y     |Android Devices                                       |
|4MY16A5|Y     |Tablet, Cable STB, Windows PC, Windows PC, Smart Watch|
+-------+------+------------------------------------------------------+

     */



    aggregatedDF_Completed.show(false)


    val aggDF_WithOnlyTested_Y = stringConcatData.groupBy("Dev_No", "Tested")
      .agg(concat_ws(", ", collect_list("model")).alias("Tested_devices"))
      .where(col("Tested") === "Y")

    aggDF_WithOnlyTested_Y.show(false)
    /*

    +-------+------+------------------------------------------------------+
    |Dev_No |Tested|Tested_devices                                        |
    +-------+------+------------------------------------------------------+
    |CTA16C5|Y     |Android Devices                                       |
    |4MY16A5|Y     |Tablet, Cable STB, Windows PC, Windows PC, Smart Watch|
    +-------+------+------------------------------------------------------+

     */

    /* Now do a join of original DF and the aggregated DF */


    def sanitize(input: String): String = s"`$input`"


    /* Line: 136 is the join */
    val joinedDF = stringConcatData.join(aggDF_WithOnlyTested_Y,
      /* The below is the JOIN condition among the columns & values from the Data Frame */
      stringConcatData("Dev_No") === aggDF_WithOnlyTested_Y("Dev_No") && stringConcatData("Tested") === aggDF_WithOnlyTested_Y("Tested"),
      /* This is the JOIN type */
      "left")
    /* This statement is selecting the COLUMNS */

    joinedDF.show(false)
    /* The above join should work like LEFT join, but it's not working as expected ... Its going as a FULL Outer Join. */
    joinedDF.select(stringConcatData("Dev_No"), stringConcatData("model"), stringConcatData("Tested"), joinedDF("Tested_devices")).show(false)



    /*


+-------+---------------+------+------------------------------------------------------+
|Dev_No |model          |Tested|Tested_devices                                        |
+-------+---------------+------+------------------------------------------------------+
|BTA16C5|Windows PC     |N     |null                                                  |
|BTA16C5|SRL            |N     |null                                                  |
|BTA16C5|Hewlett Packard|N     |null                                                  |
|CTA16C5|Android Devices|Y     |Android Devices                                       |
|CTA16C5|Hewlett Packard|N     |null                                                  |
|4MY16A5|Other          |N     |null                                                  |
|4MY16A5|Other          |N     |null                                                  |
|4MY16A5|Tablet         |Y     |Tablet, Cable STB, Windows PC, Windows PC, Smart Watch|
|4MY16A5|Other          |N     |null                                                  |
|4MY16A5|Cable STB      |Y     |Tablet, Cable STB, Windows PC, Windows PC, Smart Watch|
|4MY16A5|Other          |N     |null                                                  |
|4MY16A5|Windows PC     |Y     |Tablet, Cable STB, Windows PC, Windows PC, Smart Watch|
|4MY16A5|Windows PC     |Y     |Tablet, Cable STB, Windows PC, Windows PC, Smart Watch|
|4MY16A5|Smart Watch    |Y     |Tablet, Cable STB, Windows PC, Windows PC, Smart Watch|
+-------+---------------+------+------------------------------------------------------+

     */


    /* Approache - 2  is to use the Window Functions */

    import org.apache.spark.sql.functions._

    stringConcatData.createTempView("Initial_Table")


    println("*************************************************************************************")
    spark.sql(" select *, CASE " +
      " WHEN " +
      " Tested = 'Y' " +
      " THEN " +
      "COLLECT_LIST ( CASE WHEN Tested = 'Y' THEN model END ) OVER (PARTITION BY Dev_No) END AS Tested_Devices  " +
      "from Initial_Table ").show(false)
    println("*************************************************************************************")


    /*


    +-------+---------------+------+--------------------------------------------------------+
|Dev_No |model          |Tested|Tested_Devices                                          |
+-------+---------------+------+--------------------------------------------------------+
|BTA16C5|Windows PC     |N     |null                                                    |
|BTA16C5|SRL            |N     |null                                                    |
|BTA16C5|Hewlett Packard|N     |null                                                    |
|4MY16A5|Other          |N     |null                                                    |
|4MY16A5|Other          |N     |null                                                    |
|4MY16A5|Tablet         |Y     |[Tablet, Cable STB, Windows PC, Windows PC, Smart Watch]|
|4MY16A5|Other          |N     |null                                                    |
|4MY16A5|Cable STB      |Y     |[Tablet, Cable STB, Windows PC, Windows PC, Smart Watch]|
|4MY16A5|Other          |N     |null                                                    |
|4MY16A5|Windows PC     |Y     |[Tablet, Cable STB, Windows PC, Windows PC, Smart Watch]|
|4MY16A5|Windows PC     |Y     |[Tablet, Cable STB, Windows PC, Windows PC, Smart Watch]|
|4MY16A5|Smart Watch    |Y     |[Tablet, Cable STB, Windows PC, Windows PC, Smart Watch]|
|CTA16C5|Android Devices|Y     |[Android Devices]                                       |
|CTA16C5|Hewlett Packard|N     |null                                                    |
+-------+---------------+------+--------------------------------------------------------+

*************************************************************************************



     */



    /*stringConcatData.select("*", when(col("Tested") === "Y",
        collect_list(
          when(
            col("Tested") === "Y",
            col("model"
            )
          )
        ).over(Window.partitionBy("Dev_No"))
      ).alias("Tested_devices")
    ).show(false)*/

  }

}
