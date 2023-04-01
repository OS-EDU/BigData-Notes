package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey : 将数据源中的数据，相同 key 的数据分在一个组中，形成一个对偶元组
 *              元组中的第一个元素就是 key，
 *              元组中的第二个元素就是相同 key 的 value 的集合
 */
object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("groupByBtKey"))

    sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    )).groupByKey().collect().foreach(println)

    sc.stop()
  }
}

/**
 * 从 shuffle 的角度：reduceByKey 和 groupByKey 都存在 shuffle 的操作，但是 reduceByKey
可以在 shuffle 前对分区内相同 key 的数据进行预聚合（combine）功能，这样会减少落盘的
数据量，而 groupByKey 只是进行分组，不存在数据量减少的问题，reduceByKey 性能比较
高。
从功能的角度：reduceByKey 其实包含分组和聚合的功能。GroupByKey 只能分组，不能聚
合，所以在分组聚合的场合下，推荐使用 reduceByKey，如果仅仅是分组而不需要聚合。那
么还是只能使用 groupByKey
 */