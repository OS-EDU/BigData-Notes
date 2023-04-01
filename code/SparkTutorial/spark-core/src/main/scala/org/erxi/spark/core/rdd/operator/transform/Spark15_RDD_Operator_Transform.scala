package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduceByKey: 数据按照相同的 Key 对 Value 进行聚合，聚合过程是两两聚合。reduceByKey 中如果 key 的数据只有一个，是不会参与运算的。
 */
object Spark15_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("reduceByKey"))

    sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    )).reduceByKey((x: Int, y: Int) => {
      println(s"x = $x, y = $y")
      x + y
    }).collect().foreach(println)

    sc.stop()
  }
}
