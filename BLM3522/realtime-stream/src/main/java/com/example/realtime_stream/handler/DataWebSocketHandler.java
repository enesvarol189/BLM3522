package com.example.realtime_stream.handler;
import com.example.realtime_stream.service.PubSubService;
import com.example.realtime_stream.service.RedisService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Component
public class DataWebSocketHandler extends TextWebSocketHandler {

    @Autowired
    private RedisService redisService;

    @Autowired
    private PubSubService pubSubService;

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String data = message.getPayload();
        System.out.println("Received data: " + data);

        redisService.saveData("latest-data", data);

        pubSubService.publishMessage("projects/upheld-chalice-459519-s3/topics/data-topic", data);
    }
}