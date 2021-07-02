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
import java.util.Date;
import java.util.concurrent.TimeUnit;

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
        // 消费者组的使用
        String groupId = "order_wide_group";

        FlinkKafkaConsumer<String> orderDetailSource = KafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderInfoSource = KafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);
        DataStreamSource<String> orderInfoDs = env.addSource(orderInfoSource); //the data stream constructed

        //转换数据结构，改变其中 Create_ts 字段 将 getCreate_time 转换 yyyy-MM-dd
        SingleOutputStreamOperator<OrderInfo> orderInfoParseData = orderInfoDs.map(
                //数据类型
                new RichMapFunction<String, OrderInfo>() {
                    //定义变量
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


        //指定时间字段 参数： WatermarkStrategy ==> java 过于繁琐
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


        //订单明细指定事件时间字段
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

        //根据 id 进行关联 流数据的关联完成
        SingleOutputStreamOperator<OrderWide> orderWideDs = orderInfoKeyDs
                // fixme: join 的方式
                .intervalJoin(orderDetailKeyDs)
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

        //********************************************************** 完成流数据的关联 ***********************************************************************

        //数据的关联：关联
        /*
            DataStream<IN> in,
            AsyncFunction<IN, OUT> func,
            long timeout,
            TimeUnit timeUnit,
            int capacity
         */

        // Add an AsyncWaitOperator. The order of output stream records may be reordered
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDs,  //SingleOutputStreamOperator 是 DataStream 子类
                //关联的表名
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {  // 从对应的 hbase 中取值

                    // fixme: 内部实现方式
                    @Override
                    public String getKey(OrderWide orderWide) {
                        //long类型转换为 String
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderObject, JSONObject dimInfoJson) throws Exception {

                        String birthday = dimInfoJson.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date birthDate = sdf.parse(birthday);
                        long birthStamp = birthDate.getTime();
                        long curStamp = System.currentTimeMillis();

                        //获取年龄
                        long ageStamp = curStamp - birthStamp;
                        Long longAge = ageStamp / 1000L / 60L / 24L / 365L;
                        //long 转 int
                        int age = longAge.intValue();

                        // 添加需要的字段信息
                        orderObject.setUser_age(age);
                        orderObject.setUser_gender(dimInfoJson.getString("GENDER"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 根据上一步的结果，继续关联其他维度表，关联获取需要的字段： 关联SPU商品维度+ 关联SKU维度

        orderWideWithUserDS.print("get data");
        //样例类转json
        orderWideWithUserDS.map( obj -> JSONObject.toJSONString(obj)) //获取 String
                .addSink(KafkaUtil.getKafkaSink(orderWideSinkTopic)); //MyKafkaUtil.getKafkaSink(orderWideSinkTopic)

        env.execute();
    }
}
