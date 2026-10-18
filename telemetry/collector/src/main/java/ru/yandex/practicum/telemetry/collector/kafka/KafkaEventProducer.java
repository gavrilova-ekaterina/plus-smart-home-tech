package ru.yandex.practicum.telemetry.collector.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.Properties;

@Slf4j
@Component
public class KafkaEventProducer {

    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final String sensorsTopic;
    private final String hubsTopic;

    public KafkaEventProducer(
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${kafka.topics.sensors}") String sensorsTopic,
            @Value("${kafka.topics.hubs}") String hubsTopic
    ) {
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);

        this.producer = new KafkaProducer<>(properties);
    }

    public void sendSensorEvent(SensorEventAvro event) {
        send(sensorsTopic, event.getId(), event);
    }

    public void sendHubEvent(HubEventAvro event) {
        send(hubsTopic, event.getHubId(), event);
    }

    private void send(String topic, String key, SpecificRecordBase value) {
        producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки сообщения в топик {}: {}", topic, exception.getMessage(), exception);
            } else {
                log.debug("Сообщение отправлено в топик {}, partition={}, offset={}",
                        topic, metadata.partition(), metadata.offset());
            }
        });
    }

    @PreDestroy
    public void close() {
        producer.flush();
        producer.close();
    }

}
