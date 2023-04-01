package org.erxi.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个
或多个 Spark 操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，
广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark 会为每个任务
分别发送。
 */
object Spark06_Bc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // 封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd.map {
      case (w, c) => {
        // 方法广播变量
        val l = bc.value.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
