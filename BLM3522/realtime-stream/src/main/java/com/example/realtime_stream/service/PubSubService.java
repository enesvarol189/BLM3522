package com.example.realtime_stream.service;

import org.springframework.beans.factory.annotation.Autowired;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Service;

@Service
public class PubSubService {

    @Autowired
    private PubSubTemplate pubSubTemplate;

    public void publishMessage(String topic, String message) {
        pubSubTemplate.publish(topic, message);
    }
}