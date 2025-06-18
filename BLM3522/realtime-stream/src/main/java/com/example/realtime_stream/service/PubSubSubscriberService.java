package com.example.realtime_stream.service;

import org.springframework.stereotype.Service;
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

@Service
public class PubSubSubscriberService {

    @Autowired
    private PubSubSubscriberTemplate subscriberTemplate;

    private final List<String> receivedMessages = new ArrayList<>();
    private boolean isListening = false;

    public synchronized void startListening(String subscriptionName) {
        if (isListening) {
            return;
        }
        isListening = true;

        subscriberTemplate.subscribe(subscriptionName, message -> {
            String receivedMessage = message.getPubsubMessage().getData().toStringUtf8();
            System.out.println("Received message from Pub/Sub: " + receivedMessage);
            synchronized (receivedMessages) {
                receivedMessages.add(receivedMessage);
            }
            message.ack();
        });
    }

    public List<String> getReceivedMessages(String subscriptionName) {
        List<String> messages = new ArrayList<>();
        subscriberTemplate.pull(subscriptionName, 10, true).forEach(message -> {
            String receivedMessage = message.getPubsubMessage().getData().toStringUtf8();
            System.out.println("Pulled message from Pub/Sub: " + receivedMessage);
            messages.add(receivedMessage);
            message.ack();
        });
        return messages;
    }
}