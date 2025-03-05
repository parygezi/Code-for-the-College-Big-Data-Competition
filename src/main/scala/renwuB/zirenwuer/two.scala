package renwuB.zirenwuer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.expressions.Window
object two {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("任务B子任务二第二题")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions","org.apache.spark.sql.hudi.SparkSessionExtension")
      .getOrCreate()

    val zuorianEtl = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/ods_ds_hudi.db/sku_info")
      .createOrReplaceTempView("sku_infoDF")
      val sk_infoDF = spark.sql("SELECT * FROM sku_infoDF WHERE etl_date = '20241208' ")
    val dim_sku_info = spark.read.format("hudi").load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")
      .createOrReplaceTempView("dim_sku_infoDF")
    val dim_sku_infoDF = spark.sql("SELECT * FROM dim_sku_infoDF WHERE etl_date = (SELECT MAX(etl_date) FROM dim_sku_infoDF)")
val window = Window.partitionBy("id").orderBy(desc("create_time"))
    val fil = sk_infoDF
      .unionByName(dim_sku_infoDF,allowMissingColumns = true)
      .withColumn("row",row_number().over(window))
      .filter(col("row") === 1)
      .drop("row")

    val etl_date = "20241208"
    val DangQianTime = current_timestamp()
    val addrDF = fil
      .withColumn("etl_date",lit(etl_date))
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_insert_time",when(col("dwd_insert_time").isNull,DangQianTime).otherwise(col("dwd_insert_time")))
      .withColumn("dwd_modify_time",when(col("dwd_modify_time").isNull,DangQianTime).otherwise(col("dwd_modify_time")))
    val hudiMap = Map(
      DataSourceWriteOptions.OPERATION.key() -> "upsert",
      DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "dwd_modify_time",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "etl_date",
      DataSourceWriteOptions.TABLE_NAME.key() -> "dim_sku_info",
      "hoodie.table.name" -> "dim_sku_info"
      //"hoodie.source.write.hive_style_partitioning" -> "true"
    )
    addrDF.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(hudiMap)
      .save("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")

 spark.sql(
   """
     |SELECT id,sku_desc,dwd_insert_user,dwd_modify_time,etl_date FROM dim_sku_info
     |WHERE id > 15 AND id <= 20 order by id ASC
     |""".stripMargin)


  }
}
//SELECT COUNT(*) AS record_count
//  FROM dwd_ds_hudi.dim_province
//  WHERE etl_date = '20241208'
//  """

//SELECT COUNT(*) FROM dwd_ds_hudi.dim_region WHERE etl_date = (SELECT MAX(etl_date) FROM dwd_ds_hudi.dim_region)

//