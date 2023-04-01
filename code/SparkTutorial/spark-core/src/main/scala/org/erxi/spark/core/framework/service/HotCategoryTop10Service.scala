package org.erxi.spark.core.framework.service

import org.erxi.spark.core.framework.common.TService
import org.erxi.spark.core.framework.dao.HotCategoryTop10Dao

class HotCategoryTop10Service extends TService {

  private val dao = new HotCategoryTop10Dao()

  override def dataAnalysis(): Any = {
    val actionRDD = dao.readTextFile("input/user_visit_action.txt")

    val flatRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击的场合
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单的场合
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付的场合
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    //  ( 品类 ID，( 点击数量, 下单数量, 支付数量 ) )
    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    // 将统计结果根据数量进行降序处理，取前 10 名
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    resultRDD.foreach(println)
  }
}
