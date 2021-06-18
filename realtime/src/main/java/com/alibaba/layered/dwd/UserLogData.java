package com.alibaba.layered.dwd;


import com.alibaba.bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.layered.func.TableProcessFunction;
import com.alibaba.utils.KafkaUtil;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * <p>Description: 用户行为数据</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/9 9:36
 */
public class UserLogData {

    //定义变量
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";


    public static void main(String[] args) throws Exception {

        /**
         * 1. kafka 中模拟生成数据
         * 2. 读取kafka 内容
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        String topic = "baselog";  // kafka topic 读取数据
        String group = "get_group";
        //通过kafak获取数据  streaming.api
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, group);
        //定义返回值类型
        DataStream<String> kafkaDs = env.addSource(kafkaSource);

        //将string类型转换为 jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDs.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String str) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(str);
                        return jsonObject;
                    }
                }
        );

        //根据json 中的mid 进行分组
        KeyedStream<JSONObject, String> midKeyDs = jsonObj.keyBy(
                data -> data.getJSONObject("common").getString("mid"));


        //对数据进行过滤，算子状态和监控状态
        SingleOutputStreamOperator<JSONObject> jsonDsWithFlag = midKeyDs.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        return null;
                    }
                }
        );



       //定义侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> display = new OutputTag<String>("display"){};


        //数据写入, 进行具体的数据分流
        SingleOutputStreamOperator<String> resultDs = jsonDsWithFlag.process(
                new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                    }
                }
        );

        //获取测输出流
        DataStream<String> startDs = resultDs.getSideOutput(startTag);
        DataStream<String> dispalyDs = resultDs.getSideOutput(display);

        FlinkKafkaProducer<String> startSink = KafkaUtil.getKafkaSink(TOPIC_START);
        startDs.addSink(startSink);

        FlinkKafkaProducer<String> displaySink = KafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        dispalyDs.addSink(displaySink);

        //页面输出
        FlinkKafkaProducer<String> pageSink = KafkaUtil.getKafkaSink(TOPIC_PAGE);
        resultDs.addSink(pageSink);

        env.execute();
    }
}
