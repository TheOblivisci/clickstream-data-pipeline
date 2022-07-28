package filereader

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReader {

  def fileReader(spark: SparkSession, path: String, fileType : String) : DataFrame ={

    val df: DataFrame = spark.read.format(fileType).option("header", "true").load(path)
    df
  }
}

