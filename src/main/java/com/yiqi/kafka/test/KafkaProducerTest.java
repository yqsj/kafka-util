package com.yiqi.kafka.test;

import com.yiqi.kafka.util.KafkaProducerUtil;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-19
 */
public class KafkaProducerTest{
    @Test
    public void test(){
        KafkaProducerUtil kafkaProducerUtil = new KafkaProducerUtil("hello","yqsj:9092");
        List<String> stringList = new ArrayList<>();
        for(int i =0;i <2000;i++){
            stringList.add(String.valueOf(i));
        }
        kafkaProducerUtil.sendListMessage("hello",stringList);
    }


//    public void run() {
//        int i = 1;
//        while (true){
//            ProducerRecord producerRecord = new ProducerRecord<Integer, String>(
//                    "dfsfs",
//                    "new_kafkaproducer ==> " + i);
//            producer.send(producerRecord, new Callback() {
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e != null){
//                        System.out.println("发送失败!");
//                    }else {
//                        System.out.println("offset:" + recordMetadata.offset());
//                        System.out.println("partition" + recordMetadata.partition());
//                    }
//                }
//            });
//            i++;
//        }
//    }

}
