package com.alibaba.layered.dwd;


import com.alibaba.bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.layered.func.TableProcessFunction;
import com.alibaba.utils.KafkaUtil;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

import java.text.SimpleDateFormat;
import java.util.Date;

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

        //将string类型转换为 jsonObject    ==> fixme：实现 .map(data -> JSON.parseObject(data));
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDs.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String str) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(str);
                        return jsonObject;
                    }
                }
        );

        //根据json 中的 mid 进行分组
        KeyedStream<JSONObject, String> midKeyDs = jsonObj.keyBy(
                data -> data.getJSONObject("common").getString("mid"));


        //对数据进行过滤，算子状态和键控状态  fixme: MapFunction vs RichMapFunction
        SingleOutputStreamOperator<JSONObject> jsonDsWithFlag = midKeyDs.map(
                /**
                 * 判断：
                 * 1. 从状态中获取日期和日志产生日志进行对比， 如果状态不为空，并且状态日期和当前日期不相等，说明是老访客， 修复is_new标记是1 （新客标识）
                 * 2. 否则就是新客户，重新状态
                 */
                new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> initValue;
                    private SimpleDateFormat sf; //日期格式

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //对状态以及日期格式进行初始化
                        initValue = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                        sf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNewFlag = jsonObject.getJSONObject("common").getString("is_new");
                        Long ts = jsonObject.getLong("ts");

                        //判断是否是新,并从状态值中找到对应的状态是否为空
                        if ("1".equals(isNewFlag)) {
                            String curDate = sf.format(new Date(ts));
                            String state = initValue.value();

                            //如果状态不为空，说明非法, 更改状态值
                            if (state != null && state.length() > 0) {
                                //判断是否为同一天
                                if (!state.equals(curDate)) {
                                    jsonObject.getJSONObject("commmon").put("is_new", "0");
                                }
                            } else {
                                initValue.update(curDate); //状态值更新
                            }
                        }
                        return jsonObject;
                    }
                }
        );


        //定义侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        // dispaly 有多个页面
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };


        //数据写入, 进行具体的数据分流
        SingleOutputStreamOperator<String> resultDs = jsonDsWithFlag.process(
                // 不独立写处理函数
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context ctx, Collector<String> out) throws Exception {
                        JSONObject startJson = jsonObject.getJSONObject("start");
                        String dataStr = jsonObj.toString();

                        //判断
                        if (startJson != null && startJson.size() > 0) {
                            //输出到启动测流
                            ctx.output(startTag, dataStr);

                        } else {
                            //如果不是启动日志  说明是页面日志 ，输出到主流
                            out.collect(dataStr);

                            //同时获取曝光日志
                            JSONArray display = jsonObject.getJSONArray("display");
                            if (display != null && display.size() > 0) {
                                //解析数组中内容
                                for (int i = 0; i < display.size(); i++) {
                                    //曝光日志
                                    JSONObject subJson = display.getJSONObject(i);
                                    //从元数据中获取page_id
                                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                    subJson.put("page_id", pageId);
                                    ctx.output(displayTag, subJson.toString());
                                }
                            }
                        }
                    }
                }
        );

        //获取测输出流
        DataStream<String> startDs = resultDs.getSideOutput(startTag);
        DataStream<String> dispalyDs = resultDs.getSideOutput(displayTag);

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
