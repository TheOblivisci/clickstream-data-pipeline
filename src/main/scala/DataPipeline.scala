import org.apache.spark.sql.SparkSession
import constants.AppConstants
import cleanse.DataCleanse
import exceptions.AppExceptions.{checkDuplicate, checkPrimaryKeys}
import filereader.FileReader
import org.apache.log4j.Logger

object DataPipeline {
  val log = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit= {
    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    //read the clickstream dataset
    val clickstream_df = FileReader.fileReader(spark, AppConstants.clickstream_log_path, "csv")

    //remove duplicates from the clickstream dataset
    DataCleanse.removeDuplicates(clickstream_df, AppConstants.clickstream_primary_keys, "event_timestamp")

    //read the item dataset
    val item_df = FileReader.fileReader(spark, AppConstants.item_data_path, "csv")

    //remove duplicates from the item dataset
    DataCleanse.removeDuplicates(item_df, AppConstants.item_primary_keys, "item_id")

    //calling the exceptions
    checkPrimaryKeys(clickstream_df, AppConstants.clickstream_primary_keys)
    checkPrimaryKeys(item_df, AppConstants.item_primary_keys)
    checkDuplicate(clickstream_df, "id")

  }

}
