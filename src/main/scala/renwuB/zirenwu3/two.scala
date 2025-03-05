package renwuB.zirenwu3
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.expressions.Window
object two {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
    .appName("任务B子任务三第二题")
    .config("spark.serializer","org.apache.serializer.KryoSerializer")
    .config("spark.sql.extensions","org.apache.spark.sql.SparkSessionExtension")
    .getOrCreate()

    val fact_order_detailDF = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
    val dim_provinceDF = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/dim_region")
    val dim_region = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/dim_province")

    val JIEGUOdf = fact_order_detailDF
      .join(dim_provinceDF,fact_order_detailDF("province_id") === dim_provinceDF("id"))
      .join(dim_region,fact_order_detailDF("region_id") === dim_region("id"))
      .withColumn("year",year(fact_order_detailDF("order_date")))
      .withColumn("month",month(fact_order_detailDF("order_date")))
      .orderBy("year","month","province_id","province_name","region_id","region_name")
      .agg(
        sum("final_total_amount").alias("total_amount"),
        count("order_id").as("total_count")
      )
      .withColumn("sequence",row_number().over(Window.partitionBy("year","month").orderBy(col("total_amount").desc)))

    import org.apache.spark.sql.functions.udf
    val uuidudf = udf(() => java.util.UUID.randomUUID().toString)
    val resultWithUuidDF = JIEGUOdf.withColumn("uuid",uuidudf())

    val hudiMap = Map(
      "table.name" -> "province_consumption_day_aggr",
      "primaryKey" -> "uuid",
      "preCombineField" -> "total_count"
    )
    resultWithUuidDF.write.format("hudi")
      .mode(SaveMode.Append)
      .save("hdfs:///user/hive/warehouse/dws_ds_hudi.db/province_consumption_day_aggr")
  }
}
