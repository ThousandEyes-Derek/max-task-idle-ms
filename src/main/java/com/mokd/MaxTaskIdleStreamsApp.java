package com.mokd;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MaxTaskIdleStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(MaxTaskIdleStreamsApp.class);

    public static void main(String[] args) throws InterruptedException {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "max-tasks-idle");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 5_000L);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        var builder = new StreamsBuilder();
        var table = builder.table("table",
                                  Materialized
                                          .<String, String, KeyValueStore<Bytes, byte[]>>as(
                                                  "table-store")
                                          .withStoreType(Materialized.StoreType.IN_MEMORY)
        );

        builder.<String, String>stream("input")
               .join(table, (inputValue, tableValue) -> {
                   logger.info("Processing input: {}, table: {}", inputValue, tableValue);
                   return inputValue + "," + tableValue;
               })
               .to("output");

        startStream(props, builder);

    }

    private static void startStream(Properties props, StreamsBuilder builder)
            throws InterruptedException
    {
        var topology = builder.build(props);
        var streams = new KafkaStreams(topology, props);
        var latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(
                latch::countDown,
                "streams-shutdown-hook"
        ));

        try (streams) {
            streams.start();
            latch.await();
        }
        System.exit(0);
    }
}
