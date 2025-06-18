package com.example.realtime_stream.controller;

import com.example.realtime_stream.service.DataflowPipelineService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataflowController {

    @Autowired
    private DataflowPipelineService dataflowPipelineService;

    @PostMapping("/trigger-pipeline")
    public String triggerPipeline() {
        dataflowPipelineService.runPipeline();
        return "Pipeline triggered successfully!";
    }
}