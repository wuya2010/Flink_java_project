package com.alibaba.layered.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/17 15:44
 */
public class UniqueVisitData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))

        // 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        // 3.对读取到的数据进行结构的换换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

        // 4.按照设备id进行分组
        KeyedStream<JSONObject, String> keybyWithMidDS = jsonObjDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        //5. 过滤独立访客，有没有访问过
        SingleOutputStreamOperator<JSONObject> filterDs = keybyWithMidDS.filter(
                new RichFilterFunction<JSONObject>() {

                    ValueState<String> lastVisitDes = null;
                    SimpleDateFormat sf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化日期工具类
                        sf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态
                        ValueStateDescriptor<String> lastVisitDateStateDes = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //因为我们统计的是日活DAU，所以状态数据只在当天有效 ，过了一天就可以失效掉
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        lastVisitDateStateDes.enableTimeToLive(stateTtlConfig); //将状态值设置为时间
                        this.lastVisitDes = getRuntimeContext().getState(lastVisitDateStateDes);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPageID = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageID != null && lastPageID.length() > 0) {

                        }

                        //获取当前访问时间
                        Long ts = jsonObj.getLong("ts");
                        //将当前访问时间戳转换为日期字符串
                        String logDate = sf.format(new Date(ts));
                        //获取状态日期
                        String lastVisitDate = lastVisitDes.value();

                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                            return false;
                        } else {
                            lastVisitDes.update(logDate);  // 更新状态
                            return true;
                        }
                    }
                }
        );


        //6.1 json->string
        SingleOutputStreamOperator<String> kafkaDS = filterDs.map(jsonObj -> jsonObj.toJSONString());

        //6.2 写回到kafka的dwm层
        kafkaDS.addSink(KafkaUtil.getKafkaSink(sinkTopic));

        env.execute();

    }
}
