package com.amazonaws.kinesisanalytics.beam;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BasicBeamStreamingJob {
    public static final String BEAM_APPLICATION_PROPERTIES = "BeamApplicationProperties";

    private static Map<String, Object> getKafkaConsumerProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "AWS_MSK_IAM");
        properties.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        properties.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        return properties;
    }

    static class LogFn extends DoFn<String, String> {
        private static final Logger LOG = LoggerFactory.getLogger(LogFn.class);

        @ProcessElement
        public void processElement(@Element String value, OutputReceiver<String> out) {
            // Log the value
            LOG.info("Received value: {}", value);
            out.output(value);
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(KafkaIO.<String, String>read()
                        .withBootstrapServers("b-2.mskdatastage.3komus.c20.kafka.us-east-1.amazonaws.com:9098,b-1.mskdatastage.3komus.c20.kafka.us-east-1.amazonaws.com:9098")
                        .withTopic("discord")
                        .withMaxNumRecords(1000)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withConsumerConfigUpdates(getKafkaConsumerProperties()).withoutMetadata())
                .apply(Values.create())
                .apply("Log Values", ParDo.of(new LogFn()))
                .apply("Fixed Window", Window.<String>into(FixedWindows.of(Duration.standardSeconds(60)))
                        .triggering(AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(1)))
                        .withAllowedLateness(Duration.standardMinutes(30))
                        .discardingFiredPanes()
                )
                .apply(TextIO.write().to("/tmp/output/flick_").withSuffix(".txt"));

        pipeline.run().waitUntilFinish();
    }
}

