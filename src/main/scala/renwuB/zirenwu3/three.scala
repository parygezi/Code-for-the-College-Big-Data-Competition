package renwuB.zirenwu3

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object three {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("Top 10 Products Calculation")
      .getOrCreate()

    // 2. 读取 dwd 层 Hudi 表数据
    val orderDetailDF = spark.read.format("hudi")
      .load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")

    // 3. 过滤出 2020 年的数据
    val filteredDF = orderDetailDF.filter(year(col("order_date")) === 2020)

    // 4. 计算销售量前 10 的商品
    val quantityGroupedDF = filteredDF.groupBy("sku_id", "sku_name")
      .agg(count("order_id").as("total_quantity")) // 计算总销售量

    // 为销售量前 10 的商品增加排名
    val quantityRankedDF = quantityGroupedDF.withColumn(
      "sequence",
      row_number().over(Window.orderBy(col("total_quantity").desc))
    )

    // 筛选出排名前 10 的商品
    val topQuantityDF = quantityRankedDF.filter(col("sequence") <= 10)
      .select(
        col("sku_id").as("topquantityid"),
        col("sku_name").as("topquantityname"),
        col("total_quantity").as("topquantity"),
        lit(null).as("toppriceid"),
        lit(null).as("toppricename"),
        lit(null).as("topprice"),
        col("sequence")
      )

    // 5. 计算销售额前 10 的商品
    val priceGroupedDF = filteredDF.groupBy("sku_id", "sku_name")
      .agg(sum("final_total_amount").as("total_price")) // 计算总销售额

    // 为销售额前 10 的商品增加排名
    val priceRankedDF = priceGroupedDF.withColumn(
      "sequence",
      row_number().over(Window.orderBy(col("total_price").desc))
    )

    // 筛选出排名前 10 的商品
    val topPriceDF = priceRankedDF.filter(col("sequence") <= 10)
      .select(
        lit(null).as("topquantityid"),
        lit(null).as("topquantityname"),
        lit(null).as("topquantity"),
        col("sku_id").as("toppriceid"),
        col("sku_name").as("toppricename"),
        col("total_price").as("topprice"),
        col("sequence")
      )

    // 6. 合并销售量和销售额的前 10 数据
    val resultDF = topQuantityDF.union(topPriceDF)

    // 7. 写入结果到 ClickHouse
    resultDF.write
      .format("jdbc")
      .option("url", "jdbc:clickhouse://<clickhouse_host>:<clickhouse_port>/shtd_result")
      .option("dbtable", "topten")
      .option("user", "<username>")
      .option("password", "<password>")
      .mode("overwrite")
      .save()

    // 8. 打印结果
    resultDF.show()
  }
}
