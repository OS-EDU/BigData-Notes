package org.erxi.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("udf")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val frame = spark.read.json("input/user.json")
    frame.createOrReplaceTempView("user")

    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })

    spark.sql("select age, prefixName(username) from user").show()

    spark.stop()

  }

}
