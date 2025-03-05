package renwuB.zirenwu3
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.expressions.Window
object threelianxi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dad0")
      .getOrCreate()

    //获取hudi数据
    val fact_order_detail = spark.read.format("hudi").load("dasdas")
    //过滤出来商品2020年的数据
    val DF2020 = fact_order_detail
      .filter(year(col("order_date")) === 2020)
    //计算销售额前十的商品
    val

  }
}
