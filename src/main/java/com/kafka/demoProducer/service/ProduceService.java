package com.kafka.demoProducer.service;

import com.google.gson.Gson;
import com.kafka.demoProducer.gateway.http.json.TopicRequest;
import com.kafka.demoProducer.gateway.http.json.TopicResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
/*

 - Envio de um tópico no primeiro método
 - Envio de um tópico aguardando uma resposta Síncrona no segundo método

 */
@Service
public class ProduceService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ReplyingKafkaTemplate<String, String, String> kafkaTemplateRequestReply;

    @Value("${spring.kafka.topic.request-demo}")
    private String requestDemoTopic;

    @Value("${spring.kafka.topic.request-demo-2}")
    private String requestDemo2Topic;

    @Value("${spring.kafka.topic.reply-demo-2}")
    private String replyDemo2Topic;

    public String sendMessage(String message) {

        TopicRequest topicRequest = TopicRequest.builder().name(message).build();
        this.kafkaTemplate.send(requestDemoTopic, new Gson().toJson(topicRequest));
        return message;
    }

    public TopicResponse sendMessageWaitReply(String message) throws Exception {

        try {

            TopicRequest topicRequest = TopicRequest.builder().name(message).build();

            ProducerRecord<String, String> record = new ProducerRecord<>(requestDemo2Topic, new Gson().toJson(topicRequest));
            record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, replyDemo2Topic.getBytes()));
            RequestReplyFuture<String, String, String> sendAndReceive = this.kafkaTemplateRequestReply.sendAndReceive(record);
            ConsumerRecord<String, String> consumerRecord = sendAndReceive.get();

            TopicResponse topicResponse = new Gson().fromJson(consumerRecord.value(), TopicResponse.class);;
            return topicResponse;

        } catch (Exception e) {

            throw new Exception();

        }

    }


}