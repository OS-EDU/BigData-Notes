package org.erxi.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("foreach")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // foreach 其实是 Driver 端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("******************")
    // foreach 其实是 Executor 端内存数据打印
    rdd.foreach(println)

    // 算子 ： Operator（操作）
    //         RDD 的方法和 Scala 集合对象的方法不一样
    //         集合对象的方法都是在同一个节点的内存中完成的。
    //         RDD 的方法可以将计算逻辑发送到 Executor 端（分布式节点）执行
    //         为了区分不同的处理效果，所以将 RDD 的方法称之为算子。
    //        RDD 的方法外部的操作都是在 Driver 端执行的，而方法内部的逻辑代码是在 Executor 端执行。

    sc.stop()
  }

}
