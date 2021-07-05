package com.alibaba.layered.dws;

import com.alibaba.common.Constant;
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
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //1.4 创建Table环境
        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        // 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordStats4Product.KeyWordUdf.class);

        // 3.创建动态表
        //3.1 声明主题以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";

        //3.2建表: 建表语句
        tableEnv.executeSql(
                "CREATE TABLE page_view (" +
                        " common MAP<STRING, STRING>," +
                        " page MAP<STRING, STRING>," +
                        " ts BIGINT," +
                        " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                        " WITH (" + KafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"  // 创建表 rowtime 、 watermark
        );
        // 4.从动态表中查询数据  fixme: sql语句
        Table fullwordTable = tableEnv.sqlQuery(
                "select page['item'] fullword,rowtime " +
                        " from page_view " +
                        " where page['page_id']='good_list' and page['item'] IS NOT NULL"
        );
        // 5.利用自定义函数  对搜索关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery(
                "SELECT keyword, rowtime " +
                        "FROM  " + fullwordTable + "," +
                        "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)"
        );
        //fixme:  6.分组、开窗、聚合(聚合计算)
        Table reduceTable = tableEnv.sqlQuery(
                "select keyword,count(*) ct,  '" + Constant.KEYWORD_SEARCH + "' source," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," +
                        "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                        " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );

        // 7.转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(reduceTable, KeywordStats.class);

        keywordStatsDS.print(">>>>");

        // 8.写入到ClickHouse
        keywordStatsDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into keyword_stats_0820(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );

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
