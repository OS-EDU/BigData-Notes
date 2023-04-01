package org.erxi.spark.core.framework.application

import org.erxi.spark.core.framework.common.TApplication
import org.erxi.spark.core.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  start("local[*]", "WordCount") {
    val controller = new WordCountController()
    controller.dispatch()
  }
}
