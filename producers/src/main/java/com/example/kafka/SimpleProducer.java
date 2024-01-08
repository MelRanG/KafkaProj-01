package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
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
        kafkaProducer.send(producerRecord);
        //버퍼에 있던걸 보냄
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
