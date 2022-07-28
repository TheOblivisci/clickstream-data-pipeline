package exceptions

import org.apache.spark.sql.DataFrame

object AppExceptions {

  //check if primary key columns have null values
  def checkPrimaryKeys(df: DataFrame, primaryKeyCols : Seq[String]){
    if(df.na.drop("any", primaryKeyCols).count()!= df.count()){
      throw new customException("Null value found in Primary Key Columns")
    }
  }

  //check for duplicate event id
  def checkDuplicate( df: DataFrame, checkCol : String) = {
    if(df.dropDuplicates(checkCol).count()!= df.count()){
        throw new customException("Duplicate Event ID, Not Valid")
    }
  df
  }

}