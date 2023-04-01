package org.erxi.spark.core.framework.common

import org.apache.spark.{SparkConf, SparkContext}
import org.erxi.spark.core.framework.util.EnvUtil

trait TApplication {

  def start(master: String = "local[*]", appName: String = "Application")(op: => Unit): Unit = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }
}
