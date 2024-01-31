package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeup {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerWakeup.class.getName());
    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01-static");
        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "2");

        //currentThread는 현재 쓰레드를 가리킨다. addShutdownHook에다 선언하면 거기있는 쓰레드를 의미함
        Thread mainThread = Thread.currentThread();
        //main 쓰레드 종료시 별도의 쓰레드로 kafka Consumer에 wakup 메소드를 호출하게 만듬

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        //메인쓰레드 종료시 별도의 쓰레드로 kafkaConsumer wakeup()메소드를 호출하게 함
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                logger.info("main program start to exit by calling wakeup");
                kafkaConsumer.wakeup();
                try{
                    //메인쓰레드가 죽기전까지 대기해야한다 -> 같이 죽어야 하니까.
                    mainThread.join();
                }catch(InterruptedException e){e.printStackTrace();}
            }

        });
        //for 문 작업을 하다가 Consumer가 종료(메인 쓰레드가 종료)될 때 wakeupException이 발동해야한다.
        //하지만 아래처럼만 적으면 예외가 잡히지 않아서 36번라인 addShutdownHook(죽기전에 유언을 남기는 느낌)에 wakeup코드를 넣는다.
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key:{}, partition:{} offset:{}, record value:{}",record.key(),record.partition(), record.offset(), record.value());
                }
            }
        }
        catch (WakeupException e){
            logger.error("wakeup exception has been called");
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
//        kafkaConsumer.close();
    }
}
