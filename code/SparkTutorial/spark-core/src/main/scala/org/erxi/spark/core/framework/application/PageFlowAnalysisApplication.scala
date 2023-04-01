package org.erxi.spark.core.framework.application

import org.erxi.spark.core.framework.common.TApplication
import org.erxi.spark.core.framework.controller.PageFlowController

object PageFlowAnalysisApplication extends App with TApplication{

  start() {
    val controller = new PageFlowController()
    controller.dispatch()
  }

}
