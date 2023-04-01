package org.erxi.spark.core.framework.service

import org.erxi.spark.core.framework.common.TService
import org.erxi.spark.core.framework.dao.WordCountDao

class WordCountService extends TService {

  private val wordCountDao = new WordCountDao()

  def dataAnalysis(): Array[(String, Int)] = {
    val lines = wordCountDao.readTextFile("input/word.txt")
    val words = lines.flatMap(_.split(" "))
    val word = words.map(word => (word, 1))
    val reduce = word.reduceByKey(_ + _)
    val array = reduce.collect()
    array
  }
}
