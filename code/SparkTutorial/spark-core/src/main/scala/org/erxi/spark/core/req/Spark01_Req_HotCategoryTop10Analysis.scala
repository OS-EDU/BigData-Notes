package org.erxi.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {

    // TODO: top10 热门商品
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD = sc.textFile("input/user_visit_action.txt")

    // 2. 统计品类的点击数量：（品类 ID，点击数量）
    val clickCountRDD = actionRDD.filter(
      action => {
        val strings = action.split("_")
        strings(6) != "-1"
      }
    ).map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 统计品类的下单数量：（品类 ID，下单数量）
    val orderCountRDD = actionRDD.filter(
      action => {
        val strings = action.split("_")
        strings(8) != "null"
      }
    ).flatMap(
      action => {
        val datas = action.split("_")
        val str = datas(8)
        val cid = str.split(",")
        cid.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4. 统计品类的支付数量：（品类 ID，支付数量）
    val payCountRDD = actionRDD.filter(
      action => {
        val strings = action.split("_")
        strings(10) != "null"
      }
    ).flatMap(
      action => {
        val datas = action.split("_")
        val str = datas(10)
        val cid = str.split(",")
        cid.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5. 将商品进行排序，并且取前 10 名
    /*
         点击数量排序，下单数量排序，支付数量排序
         元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
         ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )

         cogroup = connect + group
     */
    val analysisRDD = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
      .mapValues {
        case (clickIter, orderIter, payIter) => {
          var clickCnt = 0
          var iter1 = clickIter.iterator
          if (iter1.hasNext) {
            clickCnt = iter1.next()
          }
          var orderCnt = 0
          val iter2 = orderIter.iterator
          if (iter2.hasNext) {
            orderCnt = iter2.next()
          }
          var payCnt = 0
          val iter3 = payIter.iterator
          if (iter3.hasNext) {
            payCnt = iter3.next()
          }

          (clickCnt, orderCnt, payCnt)
        }
      }

    analysisRDD.sortBy(_._2, false).take(10).foreach(println)

    sc.stop()
  }
}
