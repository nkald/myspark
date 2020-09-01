package com.sparkStreaming

import java.io.InputStream
import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object TestMysql {
  def main(args: Array[String]): Unit = {

    //通过反射获取配置文件输入流
   val resourceAsStream: InputStream  = TestMysql.getClass.getClassLoader().getResourceAsStream("./jdbc.Properties");

    //创建Properties 对象
    val properties: Properties  = new Properties();

    //加载配置文件
    properties.load(resourceAsStream);

    //通过工厂类获取DataSource（线程池）
    val source: DataSource = DruidDataSourceFactory.createDataSource(properties)

    //获取线程连接
    val connection: Connection = source.getConnection()

    //获取 PreparedStatement  操作数据库
//    val sql = "insert into account(id)  values(?)";
    val sql = "select * from spark";
    val statement: PreparedStatement = connection.prepareStatement(sql)
    //设置值
//    statement.setInt(1,2000);
    //执行语句并返回影响行数
//    val i: Int = statement.executeUpdate()
//    statement.
    //释放资
//    statement.close()
    //归还线程
//    connection.close()


  }

}
