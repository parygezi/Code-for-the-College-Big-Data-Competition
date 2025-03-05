package renwuB.zirenwu3
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.expressions.Window
object twolianxi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("daasddsa")
      .getOrCreate()

    val fact_order_detailDF = spark.read.format("hudi").load("addsa")
    val dim_provinceDF = spark.read.format("hudi").load("awdasdasd")
    val dim_regionDF = spark.read.format("hudi").load("adadsasd")
   val window = Window.partitionBy("yarn","month").orderBy(col("total_amount").desc)
    val jieguoDF = fact_order_detailDF
      .join(dim_provinceDF,fact_order_detailDF("province_id") === dim_provinceDF("id"))
      .join(dim_regionDF,fact_order_detailDF("region_id") === dim_regionDF("id"))
      .withColumn("yarn",year(fact_order_detailDF("order_date")))
      .withColumn("month",month(fact_order_detailDF("order_date")))
      .groupBy("yarn","month","province_id","province_name","region_id","region_name")
      .agg(
        sum("final_total_amount").as("total_amount"),
        count("order_id").as("total_count")
      )
      .withColumn("sequence",row_number().over(window))
    import org.apache.spark.sql.functions.udf
    val uuidDF = udf(()=> java.util.UUID.randomUUID())
    val addruuid = jieguoDF.withColumn("uuid",uuidDF())

    val hudiMap = Map(
      "hoodie.table.name" -> "adsads",
      "hoodie.source.write.primaryKey.field" -> "uudi",
      "hoodie.source.write.preCombine.field" -> "total_count",
      "hoodie.source.write.operation" -> "upsert"
    )
    addruuid.write
      .format("hudi")
      .mode(SaveMode.Overwrite)
      .options(hudiMap)
      .save("dasasasd")

    spark.sql(
      """
        |SELECT id,
        |province_name,
        |region_name,
        | CAST(total_amount AS BIGINT) AS total_amount,
        |total_count,
        |sequence,
        |year,
        |month
        |FROM SADAD
        |ORDER BY total_count DESC ,total_amount DESC , province_id DESC
        |LIMIT 5;
        |""".stripMargin)
  }
}
