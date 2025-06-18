package com.example.realtime_stream.service;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class DataflowPipelineService {

    public void runPipeline() {
        DataflowPipelineOptions options = PipelineOptionsFactory.create()
                .as(DataflowPipelineOptions.class);

        options.setRunner(DataflowRunner.class);
        options.setProject("upheld-chalice-459519-s3");
        options.setRegion("us-central1");
        options.setTempLocation("gs://my-dataflow-temp-bucket-0/temp");

        options.setStagingLocation("gs://my-dataflow-temp-bucket-0/staging");

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/upheld-chalice-459519-s3/subscriptions/dataflow-subscription"));

        PCollection<TableRow> rows = messages.apply("ParseMessages", ParDo.of(new DoFn<String, TableRow>() {
            @ProcessElement
            public void processElement(@Element String message, OutputReceiver<TableRow> out) {
                TableRow row = new TableRow();
                try {
                    org.json.JSONObject json = new org.json.JSONObject(message);
                    String id = json.optString("id", "default-id");
                    row.set("id", id);
                } catch (org.json.JSONException e) {
                    row.set("id", "invalid-id");
                }
                row.set("message", message);
                row.set("timestamp", System.currentTimeMillis() / 1000.0);
                out.output(row);
            }
        }));

        rows.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to("upheld-chalice-459519-s3:my_dataset.my_table")
                .withSchema(new TableSchema().setFields(Arrays.asList(
                        new TableFieldSchema().setName("id").setType("STRING"),
                        new TableFieldSchema().setName("message").setType("STRING"),
                        new TableFieldSchema().setName("timestamp").setType("TIMESTAMP")
                )))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        pipeline.run().waitUntilFinish();
    }
}