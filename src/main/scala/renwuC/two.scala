package renwuC

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object two {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("One-Hot Encoding")
      .getOrCreate()

    // 读取第1⼩题结果文件（假设是保存到HDFS）
    val userSkuDF = spark.read.format("csv")
      .option("header", "true")
      .load("hdfs:///tmp/user_sku_mapping.csv") // 修改为实际路径

    // 获取所有的SKU ID
    val skuIds = userSkuDF.select("sku_id").distinct()
      .rdd.map(_.getString(0)).collect() // 将SKU ID作为字符串处理

    // 创建⼀个DataFrame⽤于存储one-hot编码
    import spark.implicits._
    val skuIdDF = skuIds.toSeq.toDF("sku_id")

    // 使⽤StringIndexer为SKU ID创建索引
    val indexer = new StringIndexer()
      .setInputCol("sku_id")
      .setOutputCol("sku_index")
      .fit(skuIdDF)
    val indexedDF = indexer.transform(skuIdDF)

    // 使⽤OneHotEncoder对SKU ID进⾏one-hot编码
    val encoder = new OneHotEncoder()
      .setInputCols(Array("sku_index"))
      .setOutputCols(Array("sku_vec"))
    val encodedDF = encoder.fit(indexedDF).transform(indexedDF)

    // 将SKU的one-hot编码与⽤户数据进⾏合并
    val userSkuWithIndexDF = userSkuDF
      .join(encodedDF, "sku_id")
      .select("user_id", "sku_vec")

    // 创建矩阵
    val assembler = new VectorAssembler()
      .setInputCols(skuIds.map(skuId => s"sku_vec")) // 注意OneHotEncoder的输出是单列
      .setOutputCol("features")
    val assembledDF = assembler.transform(userSkuWithIndexDF)

    // 展示矩阵的第⼀⾏前5列数据
    val firstRow = assembledDF.orderBy("user_id").first()
    val firstRowFeatures = firstRow.getAs[org.apache.spark.ml.linalg.Vector]("features")
    val firstRowArray = firstRowFeatures.toArray.take(5)

    println("---------------第⼀⾏前5列结果展示为---------------")
    println(firstRowArray.mkString(","))

    // 关闭SparkSession
    spark.stop()
  }
}
