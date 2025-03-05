package renwuB.zirenwuyi
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
object two {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("任务B子任务一第二题")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions","org.apache.spark.sql.hudi.SparkSessionExtension")
      .getOrCreate()
    val Hudilocation = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/ods_ds_hudi.db/sku_info").createOrReplaceTempView("hudi_df")
    val hudiDF = spark.sql("SELECT MAX(create_time) AS max_time FROM hudi_df")
      .collect()(0).getAs[String]("max_time")

    val MysqlSql =
      s"""
         |SELECT * FROM shtd_store.sku_info WHERE create_time > '$hudiDF'
         |""".stripMargin

    val mysqlDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://192.168.64.161:3306/shtd_store")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .option("dbtable",MysqlSql)
      .load()

    val localDate = LocalDate.now().minusDays(1)
    val etl_date = localDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    val etl_dateDF = mysqlDF
      .withColumn("etl_date",lit(etl_date))
    val hudiMap = Map(
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "operate_time",
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "etl_date",
      DataSourceWriteOptions.OPERATION.key() -> "upsert",
      DataSourceWriteOptions.TABLE_NAME.key() -> "sku_info",
      "hoodie.table.name" -> "sku_info"
      //"hoodie.source.write.hive_style_partitioning" -> "true"
    )
    etl_dateDF.write
      .format("hudi")
      .options(hudiMap)
      .mode(SaveMode.Append)
      .save("hdfs:///user/hive/warehouse/ods_ds_hudi.db/sku_info")
  }
}
//create table 表名 (adasad int etl_date string.....) using hudi options(type = "cow",path = "目录",primaryKey = "id",preCombineField = "operate_time")partitioned by (etl_date)
//alter table 表名 add partition (etl_date="???") location = "路径"
//msck repair table 表名
//