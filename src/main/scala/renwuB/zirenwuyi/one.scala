package renwuB.zirenwuyi
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
object one {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("任务B子任务一第一题")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .enableHiveSupport()
      .getOrCreate()
    val user_infoDF = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/ods_ds_hudi.db/user_info")
      .createOrReplaceTempView("hudi_df")
    val max_time = spark.sql("SELECT MAX(COALESCE(operate_time, create_time)) AS max_time FROM hudi_df")
      .collect()(0).getAs[String]("max_time")
    val MysqlSql = s"(SELECT * FROM shtd_store.user_info WHERE COALESCE(operate_time, create_time) > '$max_time') as tmp"
    val MysqlDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://192.168.64.161:3306/shtd_store")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .option("dbtable",MysqlSql)
      .load()
    val localDate = LocalDate.now().minusDays(1)
    val etl_date = localDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    val etl_dateDF = MysqlDF
      .withColumn("operate_time",when(col("operate_time").isNull,col("create_time").otherwise("operate_time")))
      .withColumn("etl_date",lit(etl_date))

    val hudiMap = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "operate_time",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "etl_date",
      DataSourceWriteOptions.TABLE_NAME.key() -> "user_info",
      "hoodie.table.name" -> "user_info",
      DataSourceWriteOptions.OPERATION.key() -> "upsert"
      //"hoodie.source.write.hive_style_partitioning" -> "true"
    )
    etl_dateDF.write
      .format("hudi")
      .options(hudiMap)
      .mode(SaveMode.Append)
      .save("hdfs:///user/hive/warehouse/ods_ds_hudi.db/user_info")
  }
}
//create table 表名 () using hudi options (type = "cow",path = "hdfs:///user/hive/warehouse/ods_ds_hudi.db/user_info",primaryKey = "id",preCombineField = "operate_time")partitioned by (etl_date)
// alter table 表名称 add partition (elt_date="???") location = "hdfs:///user/hive/warehouse/ods_ds_hudi.db/user_info"
//msck repair table 表名称
//spark.sql("show partitions 表名称").show(false)