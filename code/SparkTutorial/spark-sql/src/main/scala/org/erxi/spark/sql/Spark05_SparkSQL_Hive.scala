package org.erxi.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark05_SparkSQL_Hive {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "quakewang")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hive")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 使用 SparkSQL 连接外置的 Hive
    // 1. 拷贝 Hive-site.xml文件到 classpath 下
    // 2. 启用 Hive 的支持
    // 3. 增加对应的依赖关系（包含 MySQL 驱动）
    spark.sql("show tables").show()

    spark.close()

  }
}
