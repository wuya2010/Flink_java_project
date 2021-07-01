package com.alibaba.layered.dwm;

import com.alibaba.bean.OrderDetail;
import com.alibaba.bean.OrderInfo;
import com.alibaba.bean.OrderWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.layered.func.DimAsyncFunction;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/10 11:33
 */
public class OrderWideData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //定义topic
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        FlinkKafkaConsumer<String> orderDetailSource = KafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderInfoSource = KafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);
        DataStreamSource<String> orderInfoDs = env.addSource(orderInfoSource); //the data stream constructed

        //转换数据结构，改变其中 Create_ts 字段 将 getCreate_time 转换 yyyy-MM-dd
        SingleOutputStreamOperator<OrderInfo> orderInfoParseData = orderInfoDs.map(
                //数据类型
                new RichMapFunction<String, OrderInfo>() {

                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    //对单个字段的处理
                    @Override
                    public OrderInfo map(String value) throws Exception {
                        OrderInfo jsonObject = JSON.parseObject(value, OrderInfo.class);
                        jsonObject.setCreate_ts(sdf.parse(jsonObject.getCreate_time()).getTime());
                        return jsonObject;
                    }
                }
        );


        //修改 orderDetail 字段类型
        SingleOutputStreamOperator<OrderDetail> orderDetailParseData = orderDetailDS.map(
                new RichMapFunction<String, OrderDetail>() {

                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderDetail map(String value) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );


        //指定时间字段 参数： WatermarkStrategy
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTs = orderInfoParseData.assignTimestampsAndWatermarks(
                //指定泛型类
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }
                )
        );


        // 窗口的关闭时间
        SingleOutputStreamOperator<OrderDetail> orderDetailWitsTs = orderDetailParseData.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }
                )
        );

        KeyedStream<OrderInfo, Long> orderInfoKeyDs = orderInfoWithTs.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyDs = orderDetailWitsTs.keyBy(OrderDetail::getOrder_id);

        //根据 id 进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDs = orderInfoKeyDs.intervalJoin(orderDetailKeyDs)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                                //自定义构造器，对数据进行合并
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

        //数据的关联：关联
        /*
            DataStream<IN> in,
            AsyncFunction<IN, OUT> func,
            long timeout,
            TimeUnit timeUnit,
            int capacity
         */
//        AsyncDataStream.unorderedWait(
//                orderWideDs,
//                //关联的表名
//                new DimAsyncFunction<OrderWide>("DIM_USER_INFO"){
//
//                    @Override
//                    public String getKey(OrderWide obj) {
////                        return obj.getUser_id()
//                        return "";
//                    }
//
//                    @Override
//                    public void join(OrderWide obj, JSONObject dimInfoJson) throws Exception {
////                        return null;
//                    }
//                }
//        )



    }
}
