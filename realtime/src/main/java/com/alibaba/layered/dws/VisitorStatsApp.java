package com.alibaba.layered.dws;

import com.alibaba.bean.VisitorStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/7/5 14:37
 */
public class VisitorStatsApp {
    public static void main(String[] args) {

        //1.1 设置流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);

        // 2.从kafka主题中读取数据
        //2.1 声明读取的主题名以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";

        //2.2 从dwd_page_log主题中读取日志数据
        FlinkKafkaConsumer<String> pageViewSource = KafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pvJsonStrDS = env.addSource(pageViewSource);

        //2.3 从dwm_unique_visit主题中读取uv数据
        FlinkKafkaConsumer<String> uvSource = KafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        DataStreamSource<String> uvJsonStrDS = env.addSource(uvSource);

        //2.4 从dwm_user_jump_detail主题中读取跳出数据
        FlinkKafkaConsumer<String> userJumpSource = KafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);
        DataStreamSource<String> userJumpJsonStrDS = env.addSource(userJumpSource);

        //2.5 输出各流中的数据
        //pvJsonStrDS.print("pv>>>>>");
        //uvJsonStrDS.print("uv>>>>>");
        //userJumpJsonStrDS.print("userJump>>>>>");

        //  3.对各个流的数据进行结构的转换  jsonStr->VisitorStats
        // 3.1 转换pv流
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pvJsonStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                0L,
                                1L,
                                0L,
                                0L,
                                jsonObj.getJSONObject("page").getLong("during_time"),
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );
        // 3.2 转换uv流
        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uvJsonStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );
        //3.3 转换sv流（Session_count）  其实还是从dwd_page_log中获取数据
        SingleOutputStreamOperator<VisitorStats> svStatsDS = pvJsonStrDS.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取当前页面的lastPageId
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            VisitorStats visitorStats = new VisitorStats(
                                    "",
                                    "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L,
                                    0L,
                                    1L,
                                    0L,
                                    0L,
                                    jsonObj.getLong("ts")
                            );
                            out.collect(visitorStats);
                        }
                    }
                }
        );

        //3.4 转换跳出流
        SingleOutputStreamOperator<VisitorStats> userJumpStatsDS = userJumpJsonStrDS.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        //将json格式字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

        // 4. 将4条流合并到一起   注意：只能合并结构相同的流
        DataStream<VisitorStats> unionDS = pvStatsDS.union(uvStatsDS, svStatsDS, userJumpStatsDS);


        // *****************************************  4个流的join及分组 *************************************************************

    }
}
