package cleanse

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame

object DataCleanse {
  def removeDuplicates(df: DataFrame, primaryKeyCols : Seq[String], orderByCol: String) : DataFrame = {

    //function to remove duplicates
    val dfRemoveDuplicates = df.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col(_)):_*).orderBy(desc(orderByCol))))
      .filter(col("rn") === 1).drop("rn")
    dfRemoveDuplicates.show(500)
    dfRemoveDuplicates

  }
}
