package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap 测试：将处理的数据进行扁平化后在进行映射处理
 */
object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("flatMap"))

    // TODO 算子 - flatMap
    // 处理 Int 类型
    sc.makeRDD(List(
      List(1, 2), List(3, 4)
    )).flatMap(list => list).collect().foreach(println)
    println("=====================")

    // 处理 String 类型
    sc.makeRDD(List(
      "Hello Scala", "Hello Spark"
    )).flatMap(_.split(" ")).collect().foreach(println)

    println("=====================")
    // 处理混合类型
    sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
      .flatMap {
        case list: List[_] => list
        case dat => List(dat)
      }.collect().foreach(println)

    sc.stop()

  }
}
