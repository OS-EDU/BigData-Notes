package org.erxi.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ACC"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器，spark 默认提供了简单数据聚合的累加器
    val sum = sc.longAccumulator("sum")

    rdd.foreach(
      num => {
        // 使用累加器
        sum.add(num)
      }
    )

    println(sum.value)

    sc.stop()


  }

}
