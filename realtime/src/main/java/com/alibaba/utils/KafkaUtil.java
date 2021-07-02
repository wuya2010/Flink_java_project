package com.alibaba.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/6/8 15:00
 */
public class KafkaUtil {

    private static String KAFKA_SERVER = "10.50.32.74:9092,10.50.219.19:9092,10.50.163.78:9092";
    private static String DEFAULT_TOPIC= "test_default";

    //定义flinkConsumer
    public static FlinkKafkaConsumer getKafkaSource(String topic, String groupId){
        Properties properties = new Properties();
        //获取ConsumerConfig属性
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        //返回值
//        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties)
        return new FlinkKafkaConsumer(topic,new SimpleStringSchema(),properties);

    }

    //flinkProducer
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());
    }


    /**
     * 写入kafka的方式
     * @param kafkaSerializationSchema
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        //设置生产数据的超时时间
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");
        //DEFAULT_TOPIC= "test_default"
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,kafkaSerializationSchema,props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


    //kafka关联属性 DDL
    public static String getKafkaDDL(String topic, String groupId){
        String ddl = "'connector'='kafka'," +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ KAFKA_SERVER +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "'format'='json'," +
                "'scan.startup.mode'='latest-offset'";
        return ddl;
    }

}
