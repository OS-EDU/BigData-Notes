package org.erxi.spark.core.framework.common

import org.apache.spark.rdd.RDD
import org.erxi.spark.core.framework.util.EnvUtil

trait TDao {

  def readTextFile(path: String): RDD[String] = {
    EnvUtil.take().textFile(path)
  }

}
