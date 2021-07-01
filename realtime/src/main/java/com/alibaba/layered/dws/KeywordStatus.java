package com.alibaba.layered.dws;

import com.alibaba.layered.func.KeywordUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
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
public class KeywordStatus {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //注册自定义函数
        tableEnv.createTemporarySystemFunction("ik",KeyWordUdf.class);


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
