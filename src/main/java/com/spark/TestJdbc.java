package com.spark;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.SQLException;

public class TestJdbc {

    public static void main(String[] args) throws SQLException {
        DruidDataSource ds =new DruidDataSource();


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


        DruidPooledConnection connection = ds.getConnection();


        String str = "insert into account(username) values(?)";
        QueryRunner queryRunner = new QueryRunner();
        queryRunner.update(connection,str,"zhags");


        connection.close();

    }
}
