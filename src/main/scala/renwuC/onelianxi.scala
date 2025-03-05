package renwuC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
object onelianxi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dsada")
      .getOrCreate()

    val fact_order_detail = spark.read.format("hudi").load("das")
    val fact_order_info = spark.read.format("hudi").load("das")
    val dim_user_info = spark.read.format("hudi").load("dasd")

val qvcho = fact_order_detail
  .join(fact_order_info,"user_id")
  .join(dim_user_info,"sku_id")
  .select("user_id","sku_id")
  .distinct()

    val user_idMap = qvcho.select("user_id").distinct().rdd.map(r => r.getInt(0)).collect().zipWithIndex
    val sku_idMap = qvcho.select("sku_id").distinct().rdd.map(r => r.getInt(0)).collect().zipWithIndex

    val user_idMapbr = spark.sparkContext.broadcast(user_idMap)
    val sku_idMapbr = spark.sparkContext.broadcast(sku_idMap)

val maping = qvcho.rdd.map{row =>
  val user_idmapbring = user_idMapbr.value(row.getAs[Int]("user_id"))
  val sku_idMapbring = sku_idMapbr.value(row.getAs[Int]("sku_id"))
  (user_idmapbring,sku_idMapbring)
}
     maping.foreach{case
      (user_idmapbring,sku_idMapbring) =>
       println("结果如下")
      println(s"$user_idmapbring:$sku_idMapbring")
    }

    maping.saveAsTextFile("hdfs:///tmp")
    spark.stop()
  }
}
