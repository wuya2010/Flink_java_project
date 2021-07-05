package com.alibaba.layered.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/17 15:45
 */
public class UserJumpData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> dataStream = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.map(jsonStr -> JSON.parseObject(jsonStr));

        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        }
                ));

        //TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyByMidDS = jsonObjWithTSDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );


        // CEP 状态编程 ， 实现的是，一段时间内时间的先后顺序
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(  //字段类型
                new SimpleCondition<JSONObject>() {
                    //模式1:不是从其它页面跳转过来的页面，是一个首次访问页面
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("next").where(
                new SimpleCondition<JSONObject>() {

                    //判断对邮箱是否访问
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //获取当前页面的id
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        //判断当前访问的页面id是否为null
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                }
        )
                // 时间限制模式
                .within(Time.milliseconds(10000));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDS, pattern);

        //8.从筛选之后的流中，提取数据   将超时数据  放到侧输出流中
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {};

        //处理超时的逻辑
        SingleOutputStreamOperator<String> filterDs = patternStream.flatSelect(
                timeOutTag,
                //定义超时
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        for (JSONObject jsonObject : jsonObjectList) {
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                //处理的没有超时数据
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {
                        //没有超时的数据，不在我们的统计范围之内 ，所以这里不需要写什么代码
                    }
                }
        );


        //9.从侧输出流中获取超时数据
        DataStream<String> jumpDS = filterDs.getSideOutput(timeOutTag);

        //jumpDS.print(">>>>>");

        //10.将跳出数据写回到kafka的DWM层
        jumpDS.addSink(KafkaUtil.getKafkaSink(sinkTopic));

        env.execute();


    }
}
