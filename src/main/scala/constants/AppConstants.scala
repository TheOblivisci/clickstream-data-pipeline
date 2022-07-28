package constants

object AppConstants {

  //Input dataset paths
  val clickstream_log_path = "data/clickstream/clickstream_log.csv"
  val item_data_path = "data/item/item_data.csv"

  //primary key columns
  val clickstream_primary_keys : Seq[String] = Seq("visitor_id", "item_id")
  val item_primary_keys : Seq[String] = Seq("item_id")

}
