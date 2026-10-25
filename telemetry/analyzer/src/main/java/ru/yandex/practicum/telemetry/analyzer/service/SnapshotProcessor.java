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
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final ScenarioService scenarioService;
    private final HubRouterService hubRouterService;

    @Value("${kafka.topics.snapshots}")
    private String snapshotsTopic;

    private static final Duration CONSUME_ATTEMPT_TIMEOUT = Duration.ofMillis(1000);
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(List.of(snapshotsTopic));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(CONSUME_ATTEMPT_TIMEOUT);

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        handleRecord(record);
                    } catch (Exception e) {
                        log.error("Ошибка при обработке снапшота offset={}: {}", record.offset(), e.getMessage(), e);
                    }
                    trackOffset(record);
                }
                consumer.commitSync(currentOffsets);
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки снапшотов", e);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                log.info("Закрываем консьюмер снапшотов");
                consumer.close();
            }
        }
    }

    private void trackOffset(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );
    }

    private void handleRecord(ConsumerRecord<String, SensorsSnapshotAvro> record) {
        SensorsSnapshotAvro snapshot = record.value();
        List<ScenarioAddedEventAvro> matched = scenarioService.getScenariosForHub(snapshot.getHubId())
                .stream()
                .filter(scenario -> allConditionsMet(snapshot, scenario))
                .toList();
        matched.forEach(scenario -> dispatchActions(snapshot, scenario));
    }

    private boolean allConditionsMet(SensorsSnapshotAvro snapshot, ScenarioAddedEventAvro scenario) {
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
        return scenario.getConditions().stream()
                .allMatch(condition -> {
                    SensorStateAvro state = sensorsState.get(condition.getSensorId());
                    return state != null && conditionMet(state, condition);
                });
    }

    private boolean conditionMet(SensorStateAvro state, ScenarioConditionAvro condition) {
        Object conditionValue = condition.getValue();
        ConditionTypeAvro type = condition.getType();

        if (type == ConditionTypeAvro.MOTION || type == ConditionTypeAvro.SWITCH) {
            boolean expected;
            if (conditionValue instanceof Boolean b) {
                expected = b;
            } else if (conditionValue instanceof Integer i) {
                expected = i != 0;
            } else {
                log.warn("Нет значения для булевого условия: тип={}, значение={}", type, conditionValue);
                return false;
            }
            Boolean actual = getBooleanValue(state.getData(), type);
            if (actual == null) return false;
            int actualInt = actual ? 1 : 0;
            int expectedInt = expected ? 1 : 0;
            return switch (condition.getOperation()) {
                case EQUALS -> actualInt == expectedInt;
                case GREATER_THAN -> actualInt > expectedInt;
                case LOWER_THAN -> actualInt < expectedInt;
            };
        }

        if (!(conditionValue instanceof Integer intVal)) {
            log.warn("Нет значения для числового условия: тип={}, значение={}", type, conditionValue);
            return false;
        }
        Integer sensorVal = getIntegerValue(state.getData(), type);
        if (sensorVal == null) return false;
        return switch (condition.getOperation()) {
            case EQUALS -> sensorVal.equals(intVal);
            case GREATER_THAN -> sensorVal > intVal;
            case LOWER_THAN -> sensorVal < intVal;
        };
    }

    private Boolean getBooleanValue(Object data, ConditionTypeAvro type) {
        return switch (type) {
            case MOTION -> data instanceof MotionSensorAvro m ? m.getMotion() : null;
            case SWITCH -> data instanceof SwitchSensorAvro s ? s.getState() : null;
            default -> null;
        };
    }

    private Integer getIntegerValue(Object data, ConditionTypeAvro type) {
        return switch (type) {
            case LUMINOSITY -> data instanceof LightSensorAvro l ? l.getLuminosity() : null;
            case TEMPERATURE -> {
                if (data instanceof TemperatureSensorAvro t) yield t.getTemperatureC();
                if (data instanceof ClimateSensorAvro c) yield c.getTemperatureC();
                yield null;
            }
            case CO2LEVEL -> data instanceof ClimateSensorAvro c ? c.getCo2Level() : null;
            case HUMIDITY -> data instanceof ClimateSensorAvro c ? c.getHumidity() : null;
            default -> null;
        };
    }

    private void dispatchActions(SensorsSnapshotAvro snapshot, ScenarioAddedEventAvro scenario) {
        scenario.getActions().forEach(action -> {
            try {
                DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                        .setSensorId(action.getSensorId())
                        .setType(ActionTypeProto.valueOf(action.getType().name()));
                if (action.getValue() != null) {
                    builder.setValue(action.getValue());
                }
                hubRouterService.sendAction(
                        snapshot.getHubId(),
                        scenario.getName(),
                        builder.build(),
                        snapshot.getTimestamp()
                );
            } catch (Exception e) {
                log.error("Ошибка при отправке действия сценария '{}': {}", scenario.getName(), e.getMessage(), e);
            }
        });
    }
}
