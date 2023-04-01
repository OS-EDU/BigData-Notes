package org.erxi.spark.core.framework.service

import org.erxi.spark.core.framework.bean.UserVisitAction
import org.erxi.spark.core.framework.common.TService
import org.erxi.spark.core.framework.dao.PageFlowDao

class PageFlowService extends TService {

  private val dao = new PageFlowDao()

  override def dataAnalysis(): Any = {
    val actionRDD = dao.readTextFile("input/user_visit_action.txt")
    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(datas(0), datas(1).toLong, datas(2), datas(3).toLong,
          datas(4), datas(5), datas(6).toLong, datas(7).toLong, datas(8),
          datas(9), datas(10), datas(11), datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    // TODO 对指定的页面连续跳转进行统计
    // 1-2,2-3,3-4,4-5,5-6,6-7
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    val okFlowIds = ids.zip(ids.tail)

    // TODO 计算分母
    val pageIdToCountMap = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // TODO 计算分子
    val sessionRDD = actionDataRDD.groupBy(_.session_id) // 根据 session 进行分组
    // 分组后，根据访问时间进行排序（升序）
    val mvRDD = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        val flowIds = sortList.map(_.page_id)
        val pageFlowIds = flowIds.zip(flowIds.tail)
        // 将不合法跳转页面进行过滤
        pageFlowIds.filter(
          t => {
            okFlowIds.contains(t)
          }
        ).map(
          t => (t, 1)
        )
      }
    )
    //  ((1, 2), 1) => ((1, 2), sum)
    val dataRDD = mvRDD.map(_._2).flatMap(list => list).reduceByKey(_ + _)

    // TODO 计算单跳转换率
    // 分子除以分母
    dataRDD.foreach {
      case ((pageId1, pageId2), sum) => {
        val lon = pageIdToCountMap.getOrElse(pageId1, 0L)
        println(s"页面 ${pageId1} 跳转到页面 ${pageId2} 单跳转换率为:" + (sum.toDouble / lon))
      }
    }
  }
}
