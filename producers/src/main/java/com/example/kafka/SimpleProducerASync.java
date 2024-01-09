package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerASync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class.getName());
    public static void main(String[] args) {

        String topicName = "simple-topic";
        //kafkaProducer 환경 설정
        Properties props = new Properties();
        //bootstrap.servers, key.serialzer.class, value.serializer.class 키와 밸류를 그대로 전송할 수 없어서 시리얼라이저해야한다.
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); 위와 같음

        //아래 설정과 Producer 인자 값이 같아야한다.
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //ProducerRecord 객체 생성. 이게 메시지
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,  "hello world 2");

        //kafkaProducer message send
//        kafkaProducer.send(producerRecord, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
//                if(exception == null){
//                    logger.info("\n #### record metadata received ##### \n" +
//                            "partition: " + recordMetadata.partition() + "\n" +
//                            "offset: " + recordMetadata.offset());
//                }else{
//                    logger.error("excetion error from broker " + exception.getMessage());
//                }
//            }
//        });
        //람다로 변경
        kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                logger.info("\n #### record metadata received ##### \n" +
                        "partition: " + recordMetadata.partition() + "\n" +
                        "offset: " + recordMetadata.offset());
            } else {
                logger.error("excetion error from broker " + exception.getMessage());
            }
        });
        //send가 한번보내고 종료되니 응답메시지를 확인하기 위해 기다림
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
