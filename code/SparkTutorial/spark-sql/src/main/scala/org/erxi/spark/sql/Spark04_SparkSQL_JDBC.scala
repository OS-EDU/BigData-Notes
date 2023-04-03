package org.erxi.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("jdbc")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/cloud_office")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "sys_user")
      .load()
    df.show()

    // 保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/cloud_office")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "sys_user00")
      .mode(SaveMode.Append)
      .save()

    spark.close()
  }
}
