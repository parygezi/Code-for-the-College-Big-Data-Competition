package renwuB.zirenwuer
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object one {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("任务B子任务二第一题")
      .config("spark.serializer", "org.apache.spark.serializer.kryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .getOrCreate()

    val ods_ds_hudi_user_infoDF = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/ods_ds_hudi.db/user_info").createOrReplaceTempView("hudi_df")
    val etl_datedf = spark.sql("select * from hudi_df WHERE etl_date = '20241208'")
    val dwd_ds_hudi_dim_user_infoDF = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/dim_user_info").createOrReplaceTempView("dwd_df")
    val dwdDF = spark.sql("SELECT * FROM dwd_df WHERE etl_date = (SELECT MAX(etl_date) FROM dwd_df)")
    val window = Window.partitionBy("id").orderBy(desc("operate_time"))
    val hebingDF = etl_datedf
      .unionByName(dwdDF, allowMissingColumns = true)
      .withColumn("row",row_number().over(window))
      .filter(col("rwo") === 1)
      .drop("row")
    val etl_date = "20241208"
    val localTime = current_timestamp()
    val addrDF = hebingDF
      .withColumn("etl_date",lit(etl_date))
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_insert_time",when(col("dwd_insert_time").isNull,lit(localTime)).otherwise(col("dwd_insert_time")))
      .withColumn("dwd_modify_time",lit(localTime))
    val hudiMap = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "operate_time",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "etl_date",
      DataSourceWriteOptions.OPERATION.key() -> "upsert",
      DataSourceWriteOptions.TABLE_NAME.key() -> "dim_user_info",
      "hoodie.table.name" -> "dim_user_info"
    )
    addrDF.write
      .format("hudi")
      .options(hudiMap)
      .mode(SaveMode.Append)
      .save("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")

  }
}
//create table 表名称 (??? ??? .... etl_date string) using hudi options (type = "cow" ,path = "目录",primaryKey = "id",preCombineField = "operate_time")partitioned by (etl_date)
//alter table 表名 add partition (etl_date="???") location = "路径"
// msck repair table 表名称