package com.mokd;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;



public class MessageProducer {

    public static void main(String[] args) throws InterruptedException {
        try (var kafkaProducer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        )))
        {
            var now = Instant.now();
            var key = UUID.randomUUID().toString();
            kafkaProducer.send(new ProducerRecord<>(
                    "input",
                    null,
                    now.toEpochMilli(),
                    key,
                    "input-value"
            ));
            kafkaProducer.flush();

            Thread.sleep(3000L);

            kafkaProducer.send(new ProducerRecord<>(
                    "table",
                    null,
                    now.toEpochMilli(),
                    key,
                    "table-value"
            ));
            kafkaProducer.flush();
        }
    }
}
