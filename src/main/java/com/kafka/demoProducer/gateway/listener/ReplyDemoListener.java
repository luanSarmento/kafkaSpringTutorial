package com.kafka.demoProducer.gateway.listener;

import com.google.gson.Gson;
import com.kafka.demoProducer.gateway.http.json.TopicResponse;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
/*

 - Listener de um tópico

 */
@Service
public class ReplyDemoListener {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    @KafkaListener(topics = "${spring.kafka.topic.reply-demo}")
    public void consume(String reply){

        TopicResponse topicResponse = new Gson().fromJson(reply, TopicResponse.class);
        logger.info(String.format("############################################################ -> Consumed message -> %s", topicResponse));

    }

}