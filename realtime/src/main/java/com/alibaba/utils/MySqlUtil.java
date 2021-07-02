package com.alibaba.utils;

import com.alibaba.bean.testObj;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/8 15:00
 */
public class MySqlUtil {


    /**
     *  传入查询语句，获取样例类c
     * @param sql ： 查询语句
     * @param clz : 返回的数据样例类
     * @param underScoreToCamel : 是否将下划线转换为驼峰命名法
     * @return
     */
    public static <T> List<T>  queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        //(String sql, Class<T> clz, boolean underScoreToCamel) {
        //定义对象
        Connection conn = null;


        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(
                    "jdbc:mysql://103.72.145.113:3306/mysql?characterEncoding=utf-8&useSSL=false",
                    "weyes_spider",
                    "Changeme_123");

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData(); //返回： number, types and properties
            //存放结果
            List<T> sqlList = new ArrayList<T>();
            //遍历集合
            while(resultSet.next()){
                //遍历每一列
                //创建一个对象，用于封装查询出来一条结果集中的数据
                T obj = clz.newInstance();

                for (int i = 1; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnClassName(i); //获取每一列的名字
                    String propertyName = columnName; //可变变量
                    //是否需要修改列名
                    if(underScoreToCamel){
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName); //转换为farmat格式
                    }
                    //列名装车 BeanUtilsBean.getInstance().setProperty(bean, name, value);
                    BeanUtils.setProperty(obj,propertyName,resultSet.getObject(i)); //获取resultSet.getObject(i)
                }

                //结果集放到list
                sqlList.add(obj);
            }

           return sqlList;

        } catch (Exception e) {
            //抛出运行时异常
            throw new RuntimeException("get mysql data exception ...");
        } finally{
            //如果不为空，关闭连接
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }

//
//    public static <T> List<T> queryList2(String sql, Class<T> clz, boolean underScoreToCamel) {
//        Connection conn = null;
//        PreparedStatement ps = null;
//        ResultSet rs = null;
//        try {
//            //注册驱动
//            Class.forName("com.mysql.jdbc.Driver");
//            //创建连接
//            conn = DriverManager.getConnection(
//                    "jdbc:mysql://103.72.145.113:3306/mysql?characterEncoding=utf-8&useSSL=false",
//                    "weyes_spider",
//                    "Changeme_123");
//            //创建数据库操作对象
//            ps = conn.prepareStatement(sql);
//            //执行SQL语句
//            // 100      zs      20
//            // 200		ls 		30
//            rs = ps.executeQuery();
//            //处理结果集
//            //查询结果的元数据信息
//            // id		student_name	age
//            ResultSetMetaData metaData = rs.getMetaData();
//            List<T> resultList = new ArrayList<T>();
//            //判断结果集中是否存在数据，如果有，那么进行一次循环
//            while (rs.next()) {
//                //创建一个对象，用于封装查询出来一条结果集中的数据
//                T obj = clz.newInstance();
//                //对查询的所有列进行遍历，获取每一列的名称
//                for (int i = 1; i <= metaData.getColumnCount(); i++) {
//                    String columnName = metaData.getColumnName(i);
//                    String propertyName = columnName;
//                    if(underScoreToCamel){
//                        //如果指定将下划线转换为驼峰命名法的值为 true，通过guava工具类，将表中的列转换为类属性的驼峰命名法的形式
//                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
//                    }
//                    //调用apache的commons-bean中工具类，给obj属性赋值
//                    BeanUtils.setProperty(obj,propertyName,rs.getObject(i));
//                }
//                //将当前结果中的一行数据封装的obj对象放到list集合中
//                resultList.add(obj);
//            }
//
//            return resultList;
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new RuntimeException("从MySQL查询数据失败");
//        } finally {
//            //释放资源
//            if (rs != null) {
//                try {
//                    rs.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//            if (ps != null) {
//                try {
//                    ps.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//            if (conn != null) {
//                try {
//                    conn.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

    /**
     * 从 mysql 读取数据
     * @param args
     */
    public static void main(String[] args) {
        List<testObj> sqlObject = queryList("select * from engine_cost", testObj.class, true);
        for (testObj sqlObj : sqlObject) {
            System.out.println(sqlObj.getCost_name());
            System.out.println(sqlObj.getEngine_name());
            System.out.println(sqlObj.getLast_update());
        }

    }

}
