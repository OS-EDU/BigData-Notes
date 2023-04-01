package org.erxi.spark.core.framework.controller

import org.erxi.spark.core.framework.common.TController
import org.erxi.spark.core.framework.service.HotCategoryTop10Service

class HotCategoryTop10Controller extends TController{
  override def dispatch(): Unit = {
    val service = new HotCategoryTop10Service()
    service.dataAnalysis()
  }
}
