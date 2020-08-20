package com.yiqi.kafka.test;

import com.yiqi.kafka.util.KafkaConsumerUtil;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: Wang Hongyu
 * @date: 2020-08-19
 */
public class KafkaConsumerTest {
    @Test
    public void test() throws InterruptedException {
        List<String> stringList = new ArrayList<>();
        stringList.add("hello");
        KafkaConsumerUtil kafkaConsumerUtil
                = new KafkaConsumerUtil("yqsj:9092","2",null,stringList,"latest");
        kafkaConsumerUtil.init();
        while(true){
            List<String> result = kafkaConsumerUtil.getKafkaData(1);
            result.parallelStream().forEach(i-> System.out.println(i));
            Thread.sleep(1000L);
        }
    }
}
