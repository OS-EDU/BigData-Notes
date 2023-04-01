package org.erxi.spark.core.framework.application

import org.erxi.spark.core.framework.common.TApplication
import org.erxi.spark.core.framework.controller.HotCategoryTop10Controller

object HotCategoryTop10AnalysisApplication extends App with TApplication {

  start() {
    val controller = new HotCategoryTop10Controller()
    controller.dispatch()
  }

}
