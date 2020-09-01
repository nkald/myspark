package com.sparkstreamingproject.req

import java.io.InputStream
import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {

  var dataSource:DataSource = init()


  def init():DataSource = {
    val properties = new Properties()
    val stream: InputStream = JDBCUtil.getClass.getClassLoader.getResourceAsStream("./jdbc.Properties")
    properties.load(stream)
    val source: DataSource = DruidDataSourceFactory.createDataSource(properties)

    source
  }

}
