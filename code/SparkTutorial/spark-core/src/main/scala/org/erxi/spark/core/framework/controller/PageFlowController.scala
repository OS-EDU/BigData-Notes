package org.erxi.spark.core.framework.controller

import org.erxi.spark.core.framework.common.TController
import org.erxi.spark.core.framework.service.PageFlowService

class PageFlowController extends TController {

  private val service = new PageFlowService()

  def dispatch(): Unit = {
    service.dataAnalysis()
  }
}
