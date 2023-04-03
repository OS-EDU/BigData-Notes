package org.erxi.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_SparkSQL_Basic {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("basic")
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    import session.implicits._

    val frame = session.read.json("input/user.json")

    // DataFrame ==> SQL
    frame.createOrReplaceTempView("user")
    session.sql("select * from user").show()
    session.sql("select username, age from user").show()
    session.sql("select avg(age) from user").show()

    // DataFrame ==> DSL 在使用 DataFrame 时，如果涉及到转换操作，需要引入转换规则
    frame.select("username", "age").show()
    frame.select($"age" + 1).show()
    frame.select('age + 2).show()

    // DataSet
    Seq(1, 2, 3, 4).toDS().show()

    // DataFrame <=> DataSet
    val rdd = session.sparkContext.makeRDD(List((1, "David", 35), (2, "Kris", 28)))
    val df = rdd.toDF("id", "name", "age")
    df.show()
    val rowRDD = df.rdd
    rowRDD.foreach(println)

    // RDD <=> DataSet

    val ds = rdd.map {
      case (id, name, age) =>
        User(id, name, age)
    }.toDS()
    ds.show()
    val userRDD = ds.rdd
    userRDD.foreach(println)

    session.stop()
  }

  case class User(id: Int, name: String, age: Int)
}
