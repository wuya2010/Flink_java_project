package com.alibaba.layered.dws;

import com.alibaba.utils.ClickHouseUtil;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/17 15:51
 */
public class KeywordStats4Product {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String groupId = "kwyword_stats_app";
        String productStatsSourceTopic = "dws_product_stats";

        //创建动态表
        tableEnv.executeSql(
                "CREATE TABLE product_status (spu_name STRING), click_ct BIGINT, cart_ct BIGINT ," +
                        "order_ct BIGINT ,stt STRING,edt STRING ) WITH(" + KafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId) + ")"

        );

        //sql 查询语句
        Table keywordProductStatus = tableEnv.sqlQuery("");

        //动态流写入 指定泛型
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.<KeywordStats>toAppendStream(keywordProductStatus, KeywordStats.class);

        keywordStatsDataStream.print();

        //数据落盘到 hbase /es / 其他
        keywordStatsDataStream.addSink(
                ClickHouseUtil.getJdbcSink(""));

        env.execute();
    }

    class KeywordStats{
        private String keyword;
        private Long ct;
        private String source;
        private String stt;
        private String edt;
        private Long ts;
    }
}
