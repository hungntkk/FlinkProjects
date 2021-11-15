package com.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


public class ConsumerTest {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerTest.class.getName());
        String bootstrapServers="10.1.5.98:9092";
//        String bootstrapServers="10.1.6.110:9092";
        String grp_id="CDC_TRIGGER_COUCHDB26_*";
        String topic="cdc.data.couchdb";
//        String topic="mwgAppTest";
        //Creating consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //creating consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(properties);
        //Subscribing
        consumer.subscribe(Arrays.asList(topic));
        //polling
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
//                logger.info("Key: "+ record.key() + ", Value:" +record.value());
//                logger.info("Partition:" + record.partition()+",Offset:"+record.offset());
                System.out.println("Key: "+ record.key() + ", Value:" +record.value());
            }


        }
    }
}
