package com.kafka.demoProducer.gateway.http;

import com.kafka.demoProducer.service.ProduceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
/*

 - Chamada dos controllers para os services

 */
@RestController
@RequestMapping(value = "/producer/v1/")
public class ProducerResource {

    @Autowired
    ProduceService produceService;

    @GetMapping(value = "/get/{name}")
    public ResponseEntity produceTopic(@NotNull @PathVariable("name") String name) {

        return new ResponseEntity<>(produceService.sendMessage(name), HttpStatus.OK);

    }

    @GetMapping(value = "/reply/{name}")
    public ResponseEntity produceTopicWithReply(@NotNull @PathVariable("name") String name) throws Exception {

        return new ResponseEntity<>(produceService.sendMessageWaitReply(name), HttpStatus.OK);

    }
}
