package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * join: 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的
 * (K,(V,W))的 RDD
 */
object Spark21_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("join"))

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3)
    ))

    val rdd2 = sc.makeRDD(List(
      ("a", 5), ("c", 6), ("a", 4)
    ))

    // join : 两个不同数据源的数据，相同的 key 的 value 会连接在一起，形成元组
    //        如果两个数据源中 key 没有匹配上，那么数据不会出现在结果中
    //        如果两个数据源中 key 有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)

    sc.stop()
  }
}
