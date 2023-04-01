package org.erxi.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ACC"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val sumAcc = sc.longAccumulator("sum")

    val mapRDD = rdd.map(
      num => {
        sumAcc.add(num)
        num
      }
    )

    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 一般情况下，累加器会放置在行动算子进行操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)

    sc.stop()
  }
}
