package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * combineByKey: 可以对 RDD 中的每个 key 进行归约操作，将具有相同 key 值的 value 归为一组，并对每组数据进行聚合计算。
 * 在实际应用中，combineByKey 常用于计算平均值、求最大值/最小值等聚合操作。
 */
object Spark19_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("combineByKey"))

    val inputRDD = sc.parallelize(Seq(("apple", 3), ("banana", 1), ("orange", 2),
      ("banana", 2), ("orange", 4)))
    // 将相同 key 值的 value 归为一组，并对每组数据进行聚合计算
    val resultRDD = inputRDD.combineByKey(
      // 将第一个 value 转换成 tuple 类型并初始化累加器
      (value: Int) => (value, 1),
      // 将后续的 value 合并到累加器中
      (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1),
      // 合并所有分区的结果
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    // 计算每个键的平均值
    val averageRDD = resultRDD.map{ case (key, value) => (key, value._1 / value._2.toFloat)}

    println(averageRDD.collect().mkString("Array(", ", ", ")"))

    sc.stop()
  }

}
