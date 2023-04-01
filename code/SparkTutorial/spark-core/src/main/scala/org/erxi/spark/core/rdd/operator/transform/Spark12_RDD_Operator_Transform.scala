package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy: 该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理
 * 的结果进行排序，默认为升序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一
 * 致。中间存在 shuffle 的过程
 */
object Spark12_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("repartition"))

    sc.makeRDD(List(6, 2, 4, 5, 3, 1), 2)
      .sortBy(num => num).collect().foreach(println)

    println("=======================")

    // sortBy 方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // sortBy 默认情况下，不会改变分区。但是中间存在 shuffle 操作
    sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
      .sortBy(t => t._1.toInt).collect().foreach(println)

    sc.stop()
  }
}
