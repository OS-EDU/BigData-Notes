package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

/**
 * 从服务器日志数据 apache.log 中获取每个时间段访问量
 */
object Spark06_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("groupBy"))

    val rdd = sc.textFile("input/apache.log")


    val timeRDD = rdd.map(
      line => {
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val datas = line.split(" ")
        val time = datas(3)
        val date = sdf.parse(time)
        val hour = new SimpleDateFormat("HH")
        val count = hour.format(date)
        (count, 1)
      }
    ).groupBy(_._1)

    timeRDD.map {
      case (hour, iter) =>
        (hour, iter.size)
    }.collect.foreach(println)

    sc.stop()
  }
}
