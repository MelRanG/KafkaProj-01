package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncCustomCB {
    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCB.class.getName());
    public static void main(String[] args) {

        String topicName = "multipart-topic";
        //kafkaProducer 환경 설정
        Properties props = new Properties();
        //bootstrap.servers, key.serialzer.class, value.serializer.class 키와 밸류를 그대로 전송할 수 없어서 시리얼라이저해야한다.
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092"); 위와 같음

        //아래 설정과 Producer 인자 값이 같아야한다.
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);
        for (int seq = 0; seq < 20; seq++) {

            //ProducerRecord 객체 생성. 이게 메시지
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName,seq ,"hello world " + seq);
            Callback callback = new CustomCallback(seq);

            kafkaProducer.send(producerRecord, callback);
        }
        //send가 한번보내고 종료되니 응답메시지를 확인하기 위해 기다림
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
