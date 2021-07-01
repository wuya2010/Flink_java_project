package com.alibaba.layered.dwm;

import com.alibaba.bean.OrderWide;
import com.alibaba.bean.PaymentInfo;
import com.alibaba.fastjson.JSON;
import com.alibaba.utils.DateTimeUtil;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

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
public class PaymentWide {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //支付相关的topic
        String payment_info_topic = "dwd_payment_info";

        //宽表相关
        String order_wide_topic = "dwm_order_wide";
        String payment_Wide_topic = "dwd_payment_wide";
        String groupId = "order_wide";

        //加载数据
        FlinkKafkaConsumer<String> paymentInfoSource = KafkaUtil.getKafkaSource(payment_info_topic, groupId);
        DataStreamSource<String> paymentDs = env.addSource(paymentInfoSource);
        FlinkKafkaConsumer<String> orderWideSource = KafkaUtil.getKafkaSource(order_wide_topic, groupId);
        DataStreamSource<String> orderWideDs = env.addSource(orderWideSource);

        //转换结构, 改变 create_time 字段结构，比较复杂， 用map操作
        SingleOutputStreamOperator<PaymentInfo> paymentInfoStructDs = paymentDs.map(
                new RichMapFunction<String, PaymentInfo>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                    }

                    //返回一个对象
                    @Override
                    public PaymentInfo map(String s) throws Exception {
                        return null;
                    }
                }
        );


        SingleOutputStreamOperator<OrderWide> orderWideStructDs = orderWideDs.map(
                new RichMapFunction<String, OrderWide>() {


                    @Override
                    public void open(Configuration parameters) throws Exception {
                    }

                    //返回一个对象
                    @Override
                    public OrderWide map(String s) throws Exception {
                        return null;
                    }
                }
        );


        //订单指定字段的时间字段
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithTsDs = paymentInfoStructDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        //需要将字符串的时间转换为毫秒数
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );

        //订单明细指定时间字段
        SingleOutputStreamOperator<OrderWide> orderWideWithTsDs = orderWideStructDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                return DateTimeUtil.toTs(element.getCreate_date());
                            }
                        }
                )
        );


        //根据id进行分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithTsDs.keyBy(PaymentInfo::getId);
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithTsDs.keyBy(OrderWide::getOrder_id);


        //流的聚合
        SingleOutputStreamOperator<PaymentWide> paymentWideDs = paymentInfoKeyedDS.intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0)) //window Time
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {

                            }
                        }
                );

        paymentWideDs.map(
                row -> JSON.toJSONString(row)
        ).addSink(KafkaUtil.getKafkaSink(payment_Wide_topic));

        env.execute();

    }
}
