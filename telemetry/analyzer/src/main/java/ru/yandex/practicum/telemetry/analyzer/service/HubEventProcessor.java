package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final ScenarioService scenarioService;
    private final CountDownLatch hubEventsProcessedLatch;

    @Value("${kafka.topics.hubs}")
    private String hubsTopic;
    private static final Duration CONSUME_ATTEMPT_TIMEOUT = Duration.ofMillis(1000);
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    @Override
    public void run() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(List.of(hubsTopic));

            Set<TopicPartition> initialPartitions = null;
            Map<TopicPartition, Long> initialEndOffsets = null;

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(CONSUME_ATTEMPT_TIMEOUT);

                if (initialPartitions == null && !consumer.assignment().isEmpty()) {
                    initialPartitions = consumer.assignment();
                    initialEndOffsets = consumer.endOffsets(initialPartitions);
                }

                int count = 0;
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    try {
                        handleRecord(record);
                    } catch (Exception e) {
                        log.error("Ошибка при обработке хабового события offset={}: {}",
                                record.offset(), e.getMessage(), e);
                    }
                    count++;
                    manageOffsets(record, count, consumer);
                }
                consumer.commitAsync();

                if (initialPartitions != null) {
                    signalIfCaughtUp(initialPartitions, initialEndOffsets);
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хаба", e);
        } finally {
            hubEventsProcessedLatch.countDown();
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                log.info("Закрываем консьюмер событий хаба");
                consumer.close();
            }
        }
    }

    private void signalIfCaughtUp(Set<TopicPartition> partitions, Map<TopicPartition, Long> endOffsets) {
        if (hubEventsProcessedLatch.getCount() == 0) return;
        boolean caughtUp = partitions.stream()
                .allMatch(tp -> consumer.position(tp) >= endOffsets.getOrDefault(tp, 0L));
        if (caughtUp) {
            log.info("Все существующие события хаба обработаны, разрешаем запуск обработчика снапшотов");
            hubEventsProcessedLatch.countDown();
        }
    }

    private void manageOffsets(ConsumerRecord<String, HubEventAvro> record, int count,
                               KafkaConsumer<String, HubEventAvro> consumer) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );

        if (count % 10 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (exception != null) {
                    log.warn("Ошибка при фиксации оффсетов хабовых событий: {}", offsets, exception);
                }
            });
        }
    }

    private void handleRecord(ConsumerRecord<String, HubEventAvro> record) {
        HubEventAvro event = record.value();
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            scenarioService.addSensor(event.getHubId(), deviceAdded.getId());
            log.debug("Добавлено устройство: hubId={}, deviceId={}", event.getHubId(), deviceAdded.getId());
        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            scenarioService.removeSensor(event.getHubId(), deviceRemoved.getId());
            log.debug("Удалено устройство: hubId={}, deviceId={}", event.getHubId(), deviceRemoved.getId());
        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            scenarioService.addScenario(event.getHubId(), scenarioAdded);
        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            scenarioService.removeScenario(event.getHubId(), scenarioRemoved.getName());
        }
    }

}
