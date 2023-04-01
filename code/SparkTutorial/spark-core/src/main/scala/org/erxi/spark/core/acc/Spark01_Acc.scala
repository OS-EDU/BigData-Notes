package org.erxi.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ACC"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // reduce : 分区内计算，分区间计算
    println(rdd.reduce(_ + _))

    println("==============")

    var sum = 0;
    rdd.foreach(
      num => {
        sum += num
      }
    )

    println("sum = " + sum)

    sc.stop()
  }

}
