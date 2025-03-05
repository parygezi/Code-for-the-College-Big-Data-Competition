package renwuB.zirenwu3
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object four {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Order Amount Median Calculation")
      .getOrCreate()
    // 读取dwd层表数据
    val orderDetailDF = spark.read.format("hudi")
      .load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
    // 过滤2020年数据
    val filteredDF = orderDetailDF
      .filter(year(col("order_date")) === 2020)

    // 计算每个省份的订单⾦额中位数
    val provinceMedianDF = filteredDF
      .join(
        spark.read.format("hudi").load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db / dim_province"),
          filteredDF ("province_id") === col("dim_province.id")
      )
      .groupBy("province_id", "province_name")
      .agg(
        expr("percentile(final_total_amount, 0.5)").as("provincemedian")
      )
    // 计算每个地区的订单⾦额中位数
    val regionMedianDF = filteredDF
      .join(
        spark.read.format("hudi").load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db / dim_region"),
          filteredDF ("region_id") === col("dim_region.id")
      )
      .groupBy("region_id", "region_name")
      .agg(
        expr("percentile(final_total_amount, 0.5)").as("regionmedian")
      )
    // 计算每个省份所在地区的订单⾦额中位数
    val resultDF = provinceMedianDF
      .join(
        regionMedianDF,
        provinceMedianDF("province_id") === regionMedianDF("region_id"),
        "left"
      )
      .select(
        col("province_id"),
        col("province_name"),
        col("region_id"),
        col("region_name"),
        col("provincemedian"),
        col("regionmedian")
      )
    // 将结果写⼊ ClickHouse
    resultDF.write
      .format("jdbc")
      .option("url", "jdbc:clickhouse://<clickhouse_host>:<clickhouse_port>/shtd_result")
      .option("dbtable", "nationmedian")
      .option("user", "<username>")
      .option("password", "<password>")
      .mode("overwrite")
      .save()

    resultDF.show()

//    USE shtd_result;
    //SELECT *
    //FROM nationmedian
    //ORDER BY regionid ASC, provinceid ASC
    //LIMIT 5;
  }
}
