package renwuC

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.sql.functions.collect_list

object three {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("SVD Recommendation")
      .getOrCreate()

    import spark.implicits._

    // 读取子任务一的结果数据
    val userSkuDF = spark.read.format("csv")
      .option("header", "true")
      .load("path/to/user_sku_mapping.csv") // 替换为实际路径

    // 转换为用户-商品评分矩阵
    val userSkuMatrix = userSkuDF
      .groupBy("user_id")
      .agg(collect_list("sku_vec").as("sku_vectors"))

    // 将数据转换为RDD[Vector]
    val vectorsRDD = userSkuMatrix.rdd.map { row =>
      val userId = row.getAs[Int]("user_id")
      val skuVectors = row.getAs[Seq[Vector]]("sku_vectors")
      (userId, Vectors.dense(skuVectors.flatMap(_.toArray).toArray))
    }

    // 创建RowMatrix
    val rowMatrix = new RowMatrix(vectorsRDD.map(_._2))

    // 进行SVD分解，保留前5个奇异值
    val svd = rowMatrix.computeSVD(5, computeU = true)
    val U = svd.U
    val S = svd.s
    val V = svd.V

    // 对每个用户计算余弦相似度
    def cosineSimilarity(vec1: Vector, vec2: Vector): Double = {
      val dotProduct = vec1.asML.dot(vec2.asML)
      val norm1 = Vectors.norm(vec1, 2)
      val norm2 = Vectors.norm(vec2, 2)
      dotProduct / (norm1 * norm2)
    }

    // 计算每个未购买商品的平均相似度
    val recommendedProducts = userSkuMatrix.rdd.flatMap { case (userId, userVector) =>
        V.toBreeze.rows.iterator.map { case (productId, productVector) =>
          val similarity = cosineSimilarity(userVector, productVector)
          (productId, similarity)
        }
      }
      .reduceByKey(_ + _)
      .map { case (productId, similaritySum) =>
        (productId, similaritySum / userSkuMatrix.count())
      }
      .top(5)(Ordering.by(_._2))

    // 输出推荐结果
    println("------------------------推荐Top5结果如下------------------------")
    recommendedProducts.zipWithIndex.foreach { case ((productId, avgSimilarity), index) =>
      println(s"相似度Top${index + 1}(商品id：${productId}，平均相似度：${avgSimilarity})")
    }

    // 关闭SparkSession
    spark.stop()
  }
}
