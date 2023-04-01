package org.erxi.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req_HotCategoryTop10Analysis {

  def main(args: Array[String]): Unit = {
    // TODO: top10 热门商品
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    /**
     * TODO: actionRDD 重复使用 cogroup 性能可能较低
     */

    // 1. 读取原始日志数据
    val actionRDD = sc.textFile("input/user_visit_action.txt")
    actionRDD.cache()

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

    // (品类 ID, 点击数量) => (品类 ID, (点击数量, 0, 0))
    // (品类 ID, 下单数量) => (品类 ID, (0, 下单数量, 0))
    //                    => (品类 ID, (点击数量, 下单数量, 0))
    // (品类 ID, 支付数量) => (品类 ID, (0, 0, 支付数量))
    //                    => (品类 ID, (点击数量, 下单数量, 支付数量))
    // ( 品类 ID, ( 点击数量, 下单数量, 支付数量 ) )

    // 5. 将品类进行排序，并且取前 10 名
    //    点击数量排序，下单数量排序，支付数量排序
    //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    //    ( 品类 ID, ( 点击数量, 下单数量, 支付数量 ) )
    val rdd1 = clickCountRDD.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }

    val rdd2 = orderCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }

    val rdd3 = payCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }

    // 将三个数据源合并在一起，统一进行聚合计算
    val sourceRDD = rdd1.union(rdd2).union(rdd3)

    sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    ).sortBy(_._2, false).take(10).foreach(println)

    sc.stop()

  }
}
