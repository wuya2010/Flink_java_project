package com.alibaba.utils;

import com.alibaba.common.ConstantConf;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.reflect.internal.Constants;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/17 16:09
 */
public class ClickHouseUtil {

    /**
     * Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
     * @param sql
     * @param <T>
     */
     public static <T> SinkFunction getJdbcSink(String sql){

         /**
          * 四个参数：
          * sql
          * jdbcStatementBuilder
          * jdbcExeutionOptions
          * jdbcConnectionOptions
          */
         SinkFunction<T> sinkFunction = JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
             @Override
             public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {

             }
            },
              new JdbcExecutionOptions.Builder().withBatchSize(5).build(),

              new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(ConstantConf.CLICKHOUSE_URL).withDriverName("ru.yandex.clickhouse.ClickHouseDriver").build()
                 );

         return sinkFunction;


     }

}
