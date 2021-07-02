package com.alibaba.layered.dwd;

import com.alibaba.bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.layered.func.Dimsink;
import com.alibaba.layered.func.TableProcessFunction;
import com.alibaba.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * <p>Description: 业务数据</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/9 9:36
 */
public class BusinessBaseData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //获取topic
        String topic = "baselog";  // kafka topic 读取数据
        String group = "get_group";

        //通过kafak获取数据  streaming.api
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(topic, group);
        //定义返回值类型
        DataStream<String> media_data = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonData = media_data.map(data -> JSON.parseObject(data));

        //对数据进行初级过滤清洗
        SingleOutputStreamOperator<JSONObject> filter_data = jsonData.filter(data -> {
            boolean flag = data.getString("data") != null && data.getString("data").length() > 3;
            return flag;
        });

        //数据分流处理，事实表入 kafka - dwd 层， 维度表到 hbase
        //fixme： 定义测输出流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYEP_HBASE){};

        //初步清洗后的数据，写入kafka (collect), 自定义 TableProcessFunction
        SingleOutputStreamOperator<JSONObject> processDS = filter_data.process(new TableProcessFunction(hbaseTag));

        //获取测输出流
        DataStream<JSONObject> hbaseDs = processDS.getSideOutput(hbaseTag);

        //6.维度数据写入 phoenix
        hbaseDs.addSink(new Dimsink());

        //7. 事实表写入 kafka
        FlinkKafkaProducer<JSONObject> kafkaSink = KafkaUtil.getKafkaSinkBySchema(
                //重写方法
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("kafka序列化");
                    }

                  //重写方法
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                        String sinkTopic = element.getString("sink_table");
                        JSONObject dataJsonObj = element.getJSONObject("data");
                        //返回字段类型  String topic, V value
                        return  new ProducerRecord<>(sinkTopic,dataJsonObj.toString().getBytes());  // V 类型： byte<>
                    }
                }
        );

        processDS.addSink(kafkaSink);
        env.execute();

    }

}
