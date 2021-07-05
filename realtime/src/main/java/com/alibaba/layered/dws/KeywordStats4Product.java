package com.alibaba.layered.dws;

import com.alibaba.bean.KeywordStats;
import com.alibaba.layered.func.KeywordProductC2RUDTF;
import com.alibaba.layered.func.KeywordUtil;
import com.alibaba.utils.ClickHouseUtil;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.wltea.analyzer.core.IKSegmenter;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/17 16:44
 */
public class KeywordStats4Product {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //注册自定义函数
        tableEnv.createTemporarySystemFunction("ik",KeyWordUdf.class);
        tableEnv.createTemporarySystemFunction("keywordProductC2R",  KeywordProductC2RUDTF.class);

        // 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String productStatsSourceTopic ="dws_product_stats";

        tableEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, " +
                "click_ct BIGINT," +
                "cart_ct BIGINT," +
                "order_ct BIGINT ," +
                "stt STRING,edt STRING ) " +
                "  WITH ("+ KafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId)+")");

        // 6.聚合计数
        Table keywordStatsProduct = tableEnv.sqlQuery("select keyword,ct,source, " +
                "DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss')  stt," +
                "DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') as edt, " +
                "UNIX_TIMESTAMP()*1000 ts from product_stats  , " +
                "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
                "LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_ct)) as T2(ct,source)");
        
        // 7.转换为数据流
        DataStream<KeywordStats> keywordStatsProductDataStream =
                tableEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);

        
        keywordStatsProductDataStream.print();
        // 8.写入到ClickHouse
        keywordStatsProductDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats(keyword,ct,source,stt,edt,ts)  " +
                                "values(?,?,?,?,?,?)"));

        env.execute();
        

    }

    /**
     * 通过注册指定返回值类型，flink 1.11 版本开始支持， 返回值
     */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))

    class KeyWordUdf extends TableFunction<Row> {
        public void eval(String value){
            List<String> keywordList = KeywordUtil.analyze(value); //分词
            // 将关键词写到 row
            for (String s : keywordList) {
                //Create a new row instance.
                Row row = new Row(1);
                //在row的指定位置，设置值：  Sets the field's content at the specified position.
                row.setField(0,s);
                collect(row); // 返回值？
            }
        }
    }
}
