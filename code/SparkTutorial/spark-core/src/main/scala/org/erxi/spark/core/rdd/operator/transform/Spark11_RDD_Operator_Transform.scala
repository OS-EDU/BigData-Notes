package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * repartition: 该操作内部其实执行的是 coalesce 操作，参数 shuffle 的默认值为 true。无论是将分区数多的
 * RDD 转换为分区数少的 RDD，还是将分区数少的 RDD 转换为分区数多的 RDD，repartition
 * 操作都可以完成，因为无论如何都会经 shuffle 过程。
 */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("repartition"))
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // coalesce 算子可以扩大分区的，但是如果不进行 shuffle 操作，是没有意义，不起作用。
    // 所以如果想要实现扩大分区的效果，需要使用 shuffle 操作
    // 缩减分区：coalesce，如果想要数据均衡，可以采用 shuffle
    // 扩大分区：repartition, 底层代码调用的就是 coalesce，而且肯定采用 shuffle
    //val newRDD: RDD[Int] = rdd.coalesce(3, true)
    rdd.repartition(3).saveAsTextFile("output")

    sc.stop()
  }
}
