package com.spark

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import org.apache.commons.dbutils.QueryRunner
import org.apache.htrace.fasterxml.jackson.databind.ser.std.JsonValueSerializer
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON


object Duxie {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("My App")
    val sc = new SparkContext(conf)
//  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("My App")

    val ds =new DruidDataSource();

    //2、设置参数
    //(1)设置基本参数
    ds.setDriverClassName("com.mysql.jdbc.Driver");
    ds.setUrl("jdbc:mysql://localhost:3306/mydb2");
    ds.setUsername("root");
    ds.setPassword("root");

    //(2)设置连接数等参数
    ds.setInitialSize(5);//一开始提前申请好5个连接，不够了，重写申请
    ds.setMaxActive(10);//最多不超过10个，如果10都用完了，还没还回来，就会出现等待
    ds.setMaxWait(1000);

    val connection: DruidPooledConnection = ds.getConnection()

    val str = "insert into account(username) values(?)"
    val queryRunner = new QueryRunner();
    queryRunner.update(connection,str,"zhags");


    connection.close();



  }

}
