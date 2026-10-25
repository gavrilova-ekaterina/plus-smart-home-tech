package ru.yandex.practicum.telemetry.analyzer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.HubEventAvroDeserializer;
import ru.yandex.practicum.kafka.telemetry.SensorsSnapshotAvroDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.snapshots.group-id}")
    private String snapshotsGroupId;

    @Value("${kafka.consumer.snapshots.auto-offset-reset:earliest}")
    private String snapshotsAutoOffsetReset;

    @Value("${kafka.consumer.snapshots.max-poll-records:50}")
    private int snapshotsMaxPollRecords;

    @Value("${kafka.consumer.hubs.group-id}")
    private String hubsGroupId;

    @Value("${kafka.consumer.hubs.auto-offset-reset:earliest}")
    private String hubsAutoOffsetReset;

    @Value("${kafka.consumer.hubs.max-poll-records:50}")
    private int hubsMaxPollRecords;

    @Bean
    public KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, snapshotsGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, snapshotsAutoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, snapshotsMaxPollRecords);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotAvroDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    @Bean
    public CountDownLatch hubEventsProcessedLatch() {
        return new CountDownLatch(1);
    }

    @Bean
    public KafkaConsumer<String, HubEventAvro> hubEventConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, hubsGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, hubsAutoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, hubsMaxPollRecords);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventAvroDeserializer.class);
        return new KafkaConsumer<>(props);
    }

}
