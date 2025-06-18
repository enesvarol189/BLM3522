package com.example.realtime_stream.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import com.example.realtime_stream.handler.DataWebSocketHandler;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final DataWebSocketHandler dataWebSocketHandler;

    public WebSocketConfig(DataWebSocketHandler dataWebSocketHandler) {
        this.dataWebSocketHandler = dataWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(dataWebSocketHandler, "/data-stream").setAllowedOrigins("*");
    }
}