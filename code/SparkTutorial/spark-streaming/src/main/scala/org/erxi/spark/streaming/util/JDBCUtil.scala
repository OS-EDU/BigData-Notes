package org.erxi.spark.streaming.util

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource

object JDBCUtil {

  // 初始化连接池
  var dataSource: DataSource = init()

  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hadoop102:3306/spark-streaming?useUnicode=true&characterEncoding=UTF-8")
    properties.setProperty("username", "root")
    properties.setProperty("password", "000000")
    properties.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(properties)
  }

  // 获取 MySQL 连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  // 执行 SQL，单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var preparedStatement: PreparedStatement = null

    try {
      connection.setAutoCommit(false)
      preparedStatement = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          preparedStatement.setObject(i + 1, params(i))
        }
      }
      rtn = preparedStatement.executeUpdate()
      connection.commit()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var preparedStatement: PreparedStatement = null

    try {
      preparedStatement = connection.prepareStatement(sql)
      for (i <- params.indices) {
        preparedStatement.setObject(i + 1, params(i))
      }
      flag = preparedStatement.executeQuery().next()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }


}
