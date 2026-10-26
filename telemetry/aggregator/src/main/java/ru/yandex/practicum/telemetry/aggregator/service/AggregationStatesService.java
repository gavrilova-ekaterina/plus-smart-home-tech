package ru.yandex.practicum.telemetry.aggregator.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class AggregationStatesService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(event.getHubId(),
                hubId -> new SensorsSnapshotAvro(hubId, event.getTimestamp(), new HashMap<>()));

        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());

        if (oldState != null) {
            if (oldState.getTimestamp().isAfter(event.getTimestamp())
                    || oldState.getData().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        snapshot.getSensorsState().put(event.getId(),
                new SensorStateAvro(event.getTimestamp(), event.getPayload()));
        snapshot.setTimestamp(event.getTimestamp());

        return Optional.of(snapshot);
    }

}
