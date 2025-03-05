package renwuC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object one {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Feature Engineering")
      .getOrCreate()

    // 读取Hudi或MySQL中的数据
    // 这⾥以Hudi为例，MySQL数据读取请根据实际配置调整
    val orderDetailDF = spark.read.format("hudi")
      .load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/fact_order_detail")
    val orderInfoDF = spark.read.format("hudi")
      .load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
    val skuInfoDF = spark.read.format("hudi")
      .load("hdfs:///user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")

    // 提取需要的字段并进⾏去重
    val userSkuDF = orderDetailDF
      .join(orderInfoDF, "order_id")
      .join(skuInfoDF, "sku_id")
      .select("user_id", "sku_id")
      .distinct()


    // ⽣成⽤户ID和商品ID的映射
    val userIds = userSkuDF.select("user_id").distinct().rdd.map(r => r.getInt(0)).collect().zipWithIndex.toMap
    val skuIds = userSkuDF.select("sku_id").distinct().rdd.map(r => r.getInt(0)).collect().zipWithIndex.toMap

    // ⼴播映射
    val userIdMap = spark.sparkContext.broadcast(userIds)
    val skuIdMap = spark.sparkContext.broadcast(skuIds)

    // 转换为映射格式
    val mappedDF = userSkuDF.rdd.map { row =>
      val userMapping = userIdMap.value(row.getAs[Int]("user_id"))
      val skuMapping = skuIdMap.value(row.getAs[Int]("sku_id"))
      (userMapping, skuMapping)
    }.take(5)


    // 打印前5条数据
    mappedDF.foreach { case (userMapping, skuMapping) => println(s"$userMapping:$skuMapping")
    }
spark.stop()
  }

}
