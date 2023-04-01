package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("aggregateByKey"))

    // TODO 获取相同 key 的数据的平均值 => (a, 3),(b, 4)
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)), 2)

    // 对 rdd 中每个键执行聚合操作，并且初始值为 (0, 0)
    val newRDD = rdd.aggregateByKey((0, 0))(
      // 对于每个键，将该键的值加入到已有的累加器中
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      // 合并所有分区的结果
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    // 计算每个键的平均值
    val resultRDD = newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }

    resultRDD.collect().foreach(println)

    sc.stop()
  }

}
