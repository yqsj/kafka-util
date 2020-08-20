package com.yiqi.kafka.util;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-19
 */
public class KafkaConsumerUtil{
    private KafkaConsumer<String, String> consumer = null;
    private Properties props = new Properties();
    private ArrayList<String> kafkaData = new ArrayList<String>();
    private List<String> topics = null;
    private CountDownLatch cdl = new CountDownLatch(1);
    private boolean status = true;
    private long cacheSize = 1000L;
    private Duration pollSize = Duration.ofMillis(1000);


    public KafkaConsumerUtil(String kafkaIp, String groupId, String zkHosts, List<String> topics, String offset) {
        this.topics = topics;
        // kafkaIp
        props.put("bootstrap.servers", kafkaIp);
        // 设置自动提交offset
        props.put("enable.auto.commit", "true");
        //消费者组ip
        props.put("group.id", groupId);
        if (null != zkHosts) {
            props.put("zookeeper.connect", zkHosts);
        }
        // 设置自动提交offset的延时(可能会造成重复消费的情况)
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        // key-value的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        if (null != offset) {
            props.put("auto.offset.reset", offset);
        }
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(topics);
    }
    /**
     * Reset Kafka Consumer.
     */
    public void resetConsumer() {
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(topics);
    }

    /**
     * Close Kafka Consumer.
     */
    public void closeKafkaConsumer() {
        try {
            status = false;
            cdl.await();
            consumer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Set cache size.
     *
     * @param cacheSize
     *          Cache Size
     * @return
     */
    public KafkaConsumerUtil setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    /**
     * Set Poll Size.
     *
     * @param pollSize
     *          Poll Size
     * @return
     */
    public KafkaConsumerUtil setPollSize(Duration pollSize) {
        this.pollSize = pollSize;
        return this;
    }

    /**
     * Initialization.
     */
    public void init() {
        new Thread(new Runnable() {
            public void run() {
                while (status) {
                    try {
                        if (getKafkaDataSize() > cacheSize) {
                            try {
                                Thread.sleep(500);
                                continue;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        ConsumerRecords<String, String> records = consumer.poll(pollSize);
                        for (ConsumerRecord<String, String> record : records) {
                            kafkaData.add(record.value());
                        }
                        System.out.println("kafka local data add " + records.count() + " times......");
                        consumer.commitSync();
                        records = null;
                    } catch (Exception e) {
                        try {
                            consumer.close();
                        } finally {
                            resetConsumer();
                        }
                    }
                }
                cdl.countDown();
                System.out.println("I'm KafkaConsumerUtils's init method,i'm over!");
            }
        }).start();
    }

    /**
     * Get Kafka Data Size.
     *
     * @return int
     */
    public int getKafkaDataSize() {
        return kafkaData.size();
    }

    /**
     * Clear Kafka Data.
     */
    public void clearKafkaData() {
        kafkaData.clear();
    }

    /**
     * Get one Kafka Data.
     *
     * @return String
     */
    public String getKafkaData() {
        synchronized (this) {
            if (getKafkaDataSize() > 0) {
                return kafkaData.remove(0);
            } else {
                return null;
            }
        }
    }

    /**
     * Get a set number of Kafka Data.
     *
     * @param size
     *          List Size
     *
     * @return List
     */
    public List<String> getKafkaData(int size) {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < size; i++) {
            String data = getKafkaData();
            if (null != data) {
                list.add(data);
            }
        }
        return list;
    }

    /**
     * Copy one Kafka Data.
     *
     * @return String
     */
    public String copyKafkaData() {
        if (0 < getKafkaDataSize()) {
            return kafkaData.get(0);
        } else {
            return null;
        }
    }

    /**
     * 消费指定分区
     * @param topicPartition
     */
    public void setAssign(TopicPartition topicPartition){
        consumer.assign(Collections.singleton(topicPartition));
    }

    /**
     * 指定偏移量
     * @param topicPartition
     * @param size
     */
    public void setSeek(TopicPartition topicPartition,long size){
        consumer.seek(topicPartition,size);
    }

    /**
     * Get Kafka Consumer.
     *
     * @return
     */
    public KafkaConsumer<String, String> getKafkaConsumer() {
        return this.consumer;
    }

}
