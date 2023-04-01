package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * coalesce: 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，收缩合并分区，减少分区的个数，减小任务调度成本
 */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("distinct"))

    /**
     * coalesce 方法默认情况下不会将分区的数据打乱重新组合
     * 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
     * 如果想要让数据均衡，可以进行 shuffle 处理
     */
    sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
      .coalesce(2, true).saveAsTextFile("output")

    sc.stop()
  }

}
