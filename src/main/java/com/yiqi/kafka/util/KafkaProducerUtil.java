package com.yiqi.kafka.util;


import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-19
 */
public class KafkaProducerUtil{
    private KafkaProducer<String, String> producer;
    private Properties props = new Properties();
    private List<ProducerRecord<String, String>> kafkaData = null;
    private long localCacheSize = 100L;

    public KafkaProducerUtil(String topic,String kafkaIp){
        props.put("bootstrap.servers", kafkaIp);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 33554432);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 335544320);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //指定自定义分区
//        props.put("partitioner.class", "com.monk.kafka.CustomPartitioner");
        producer = new KafkaProducer<String, String>(props);
        kafkaData = Collections.synchronizedList(new ArrayList<ProducerRecord<String, String>>());
    }

    /**
     * set Local Cache Size.
     *
     * @param size
     *          Local Cache Size
     * @return this
     */
    public KafkaProducerUtil setLocalCacheSize(long size) {
        this.localCacheSize = size;
        return this;
    }

    /**
     * Send One Message to Local Cache.
     *
     * @param topic
     *          topic name
     * @param msg
     *          message contents
     */
    public void sendOneMessageUseLocalCache(String topic, String msg) {
        synchronized (kafkaData) {
            kafkaData.add(new ProducerRecord<String, String>(topic, msg));
            if (localCacheSize < kafkaData.size()) {
                sendLocalCache();
            }
        }
    }

    /**
     * Send a set number of Messages to Local Cache.
     *
     * @param topic
     *          topic name
     * @param msgs
     *          message contents
     */
    public void sendListMessageUseLocalCache(String topic, List<String> msgs) {
        msgs.forEach(msg -> {
            sendOneMessageUseLocalCache(topic, msg);
        });
    }

    /**
     * Send a set number of Local Cache to Kafka Broker.
     */
    public void sendLocalCache() {
        synchronized (kafkaData) {
            kafkaData.forEach(msg -> {
                producer.send(msg);
            });
            producer.flush();
            kafkaData.clear();
        }
    }

    /**
     * Send One Message to Kafka Broker.
     *
     * @param topic
     *          topic name
     * @param msg
     *          message contents
     */
    public void sendOneMessage(String topic, String msg) {
        producer.send(new ProducerRecord<String, String>(topic, msg));
        producer.flush();
    }

    /**
     * Send a set number of Messages to Kafka Broker.
     *
     * @param topic
     *          topic name
     * @param msgs
     *          message contents
     */
    public void sendListMessage(String topic, List<String> msgs) {
        msgs.forEach(msg -> {
            producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null){
                        System.out.println("发送失败!");
                    }else {
                        System.out.println("offset:" + recordMetadata.offset());
                        System.out.println("partition" + recordMetadata.partition());
                    }
                }
            });
        });
        producer.flush();
    }

    /**
     * Flush Kafka Producer.
     */
    public void flushKafkaProducer() {
        producer.flush();
    }

    /**
     * Close Kafka Producer.
     */
    public void closeKafkaProducer() {
        producer.flush();
        producer.close();
    }

}
