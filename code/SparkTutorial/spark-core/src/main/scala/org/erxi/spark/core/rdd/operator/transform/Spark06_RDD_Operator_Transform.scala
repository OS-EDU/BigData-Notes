package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy: 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，将这样的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中。
 * 一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
 */
object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("groupBy"))

    // TODO 算子 - groupBy
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    /**
     * groupBy 会将数据源中的每一个数据进行分组判断，根据返回的分组 key 进行分组， 相同的 key 值的数据会放置在一个组中
     */

    // 按照奇偶进行分组
    rdd.groupBy((_ % 2)).collect().foreach(println)

    // 按照首字母进行分组
    sc.makeRDD(List("Hadoop", "Spark", "Sqoop", "Hive"))
      .groupBy(_.charAt(0)).collect().foreach(println)


    sc.stop()
  }
}
