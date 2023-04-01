package org.erxi.spark.core.framework.controller

import org.erxi.spark.core.framework.common.TController
import org.erxi.spark.core.framework.service.WordCountService

// 控制层
class WordCountController extends  TController{

  private val service = new WordCountService()

  // 调度
  def dispatch(): Unit = {
    // TODO 执行作业操作
    val value = service.dataAnalysis()
    value.foreach(println)
  }
}
