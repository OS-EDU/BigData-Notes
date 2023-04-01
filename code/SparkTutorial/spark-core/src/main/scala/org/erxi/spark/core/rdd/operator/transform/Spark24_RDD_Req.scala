package org.erxi.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AdClickCount")
    val sc = new SparkContext(sparkConf)

    // 1. 获取原始数据：时间戳，省份，城市，用户，广告
    val dataRDD = sc.textFile("input/agent.log")

    // 2. 将原始数据进行结构的转换。方便统计
    //    时间戳，省份，城市，用户，广告  =>  ( ( 省份，广告 ), 1 )
    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )

    // 3. 将转换结构后的数据，进行分组聚合
    //    ( ( 省份，广告 ), 1 ) => ( ( 省份，广告 ), sum )
    val reduceRDD = mapRDD.reduceByKey(_ + _)

    // 4. 将聚合的结果进行结构的转换
    //    ( ( 省份，广告 ), sum ) => ( 省份, ( 广告, sum ) )
    val newMapRDD = reduceRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }

    // 5. 将转换结构后的数据根据省份进行分组
    //    ( 省份, 【( 广告A, sumA )，( 广告B, sumB )】 )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    // 6. 将分组后的数据组内排序（降序），取前3名
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    // 7. 采集数据打印在控制台
    resultRDD.collect().foreach(println)

    sc.stop()
  }
}
