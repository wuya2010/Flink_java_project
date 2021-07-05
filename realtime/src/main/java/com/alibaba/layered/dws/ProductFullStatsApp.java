package com.alibaba.layered.dws;

import com.alibaba.bean.ProductStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.utils.KafkaUtil;
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
 * @date 2021/7/5 14:44
 */
public class ProductFullStatsApp {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);

        //2.1 声明相关的主题名称以及消费者组
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";


        //2.2 从页面日志中获取点击和曝光数据
        FlinkKafkaConsumer<String> pageViewSource = KafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        //2.3 从dwd_favor_info中获取收藏数据
        FlinkKafkaConsumer<String> favorInfoSourceSouce = KafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);

        //2.4 从dwd_cart_info中获取购物车数据
        FlinkKafkaConsumer<String> cartInfoSource = KafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        //2.5 从dwm_order_wide中获取订单数据
        FlinkKafkaConsumer<String> orderWideSource = KafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        //2.6 从dwm_payment_wide中获取支付数据
        FlinkKafkaConsumer<String> paymentWideSource = KafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        //2.7 从dwd_order_refund_info中获取退款数据
        FlinkKafkaConsumer<String> refundInfoSource = KafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        //2.8 从dwd_order_refund_info中获取评价数据
        FlinkKafkaConsumer<String> commentInfoSource = KafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);



        //获取表： pageViewDStream，favorInfoDStream ，cartInfoDStream，orderWideDStream，paymentWideDStream，refundInfoDStream ， commentInfoDStream
        SingleOutputStreamOperator<ProductStats> productClickAndDispalyDS = pageViewDStream.process(

                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                        String page_id = pageJsonObj.getString("page_id");
                        Long ts = jsonObject.getLong("ts");

                        //如果 page_id 是 商品详情页，认为该商品被点击了一次
                        if ("good_detail".equals(page_id)) {
                            Long skuId = pageJsonObj.getLong("item");

                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(skuId)
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build();

                            out.collect(productStats);
                        }

                        JSONArray displays = jsonObject.getJSONArray("display");

                        //如果displays属性不为空，那么说明有曝光数据
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                //获取曝光数据
                                JSONObject displayJsonObj = displays.getJSONObject(i);
                                //判断是否曝光的某一个商品
                                if ("sku_id".equals(displayJsonObj.getString("item_type"))) {
                                    //获取商品id
                                    Long skuId = displayJsonObj.getLong("item");
                                    //封装曝光商品对象
                                    ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                    //向下游输出
                                    out.collect(productStats);
                                }
                            }
                        }

                    }
                }
        );

        //所有的流进行转换
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = null;

        // 4. 将转换后的流进行合并
        DataStream<ProductStats> unionDS = productClickAndDispalyDS.union(
                orderWideStatsDS
        );

        //********************************************************** 维度表的聚合 *****************************************************************

    }
}
