package org.erxi.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark06_SparkSQL_Create {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "quakewang")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use sparkDemo")

    // 准备数据
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'input/sql/user_visit_action.txt' into table sparkDemo.user_visit_action
            """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'input/sql/product_info.txt' into table sparkDemo.product_info
            """.stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'input/sql/city_info.txt' into table sparkDemo.city_info
            """.stripMargin)

    spark.sql("""select * from city_info""").show


    spark.close()

  }
}
