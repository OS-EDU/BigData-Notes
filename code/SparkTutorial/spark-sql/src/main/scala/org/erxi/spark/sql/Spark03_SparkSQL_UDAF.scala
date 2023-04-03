package org.erxi.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

object Spark03_SparkSQL_UDAF {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UDAF")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df = spark.read.json("input/user.json")

    val ds = df.as[User]

    val udafCol = new MyAvgUDAF().toColumn
    ds.select(udafCol).show()

    spark.close()
  }

  /**
   * 自定义聚合函数类：计算年龄的平均值
   * 1. 继承 org.apache.spark.sql.expressions.Aggregator 定义泛型
   * IN：输入的数据类型 Long
   * BUF：缓冲区的数据类型 Buff
   * OUT：输出的数据类型 Long
   * 2. 重写方法
   */
  case class User(username: String, age: Long)

  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[User, Buff, Long] {

    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(b: Buff, in: User): Buff = {
      b.total = b.total + in.age
      b.count = b.count + 1
      b
    }

    // 合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
