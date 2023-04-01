package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * aggregateByKey: 将数据根据不同的规则进行分区内计算和分区间计算
 * 第一个参数列表,需要传递一个参数，表示为初始值，主要用于当碰见第一个 key 的时候，和 value 进行分区内计算
 * 第二个参数列表需要传递 2 个参数：分别表示分区内计算规则 (seqOp) 和分区间计算规则 (combOp)
 *
 */
object Spark17_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("aggregateByKey"))

    // TODO 取出每个分区内相同 key 的最大值然后分区间相加
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)), 2)

    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    println("====================")

    rdd.aggregateByKey(5)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    println("====================")

    // seqOp 函数是 _+_，表示对相同键值的 value 进行求和操作；combOp 函数也是 _+_，表示将所有分区的求和结果进行加总。
    rdd.aggregateByKey(0)(_ + _, _ + _).collect.foreach(println)

    println("====================")

    // 如果聚合计算时，分区内和分区间计算规则相同 则可使用 foldByKey
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    sc.stop()
  }
}
