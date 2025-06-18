package com.example.realtime_stream.controller;

import com.example.realtime_stream.service.PubSubSubscriberService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class PubSubController {

    @Autowired
    private PubSubSubscriberService pubSubSubscriberService;

    @GetMapping("/listen")
    public List<String> listenToMessages(@RequestParam String subscriptionName) {
        pubSubSubscriberService.startListening(subscriptionName);
        return pubSubSubscriberService.getReceivedMessages(subscriptionName);
    }
}