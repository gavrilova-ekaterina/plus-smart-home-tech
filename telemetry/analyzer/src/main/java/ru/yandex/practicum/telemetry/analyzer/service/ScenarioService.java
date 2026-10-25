package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.ActionType;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioActionId;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioConditionId;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioService {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Transactional(readOnly = true)
    public List<ScenarioAddedEventAvro> getScenariosForHub(String hubId) {
        return scenarioRepository.findByHubId(hubId).stream()
                .map(this::toAvro)
                .toList();
    }

    @Transactional
    public void addScenario(String hubId, ScenarioAddedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(this::deleteScenarioWithDependencies);

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName());
        scenario = scenarioRepository.save(scenario);

        for (ScenarioConditionAvro condAvro : event.getConditions()) {
            Sensor sensor = getOrCreateSensor(condAvro.getSensorId(), hubId);

            Condition condition = new Condition();
            condition.setType(ConditionType.valueOf(condAvro.getType().name()));
            condition.setOperation(ConditionOperation.valueOf(condAvro.getOperation().name()));
            if (condAvro.getValue() instanceof Integer v) {
                condition.setValue(v);
            } else if (condAvro.getValue() instanceof Boolean b) {
                condition.setBoolValue(b);
                condition.setValue(b ? 1 : 0);
            }
            condition = conditionRepository.save(condition);

            ScenarioCondition sc = new ScenarioCondition();
            sc.setId(new ScenarioConditionId(scenario.getId(), sensor.getId(), condition.getId()));
            sc.setScenario(scenario);
            sc.setSensor(sensor);
            sc.setCondition(condition);
            scenario.getConditions().add(sc);
        }

        for (DeviceActionAvro actionAvro : event.getActions()) {
            Sensor sensor = getOrCreateSensor(actionAvro.getSensorId(), hubId);

            Action action = new Action();
            action.setType(ActionType.valueOf(actionAvro.getType().name()));
            if (actionAvro.getValue() instanceof Integer v) action.setValue(v);
            action = actionRepository.save(action);

            ScenarioAction sa = new ScenarioAction();
            sa.setId(new ScenarioActionId(scenario.getId(), sensor.getId(), action.getId()));
            sa.setScenario(scenario);
            sa.setSensor(sensor);
            sa.setAction(action);
            scenario.getActions().add(sa);
        }

        scenarioRepository.save(scenario);
        log.info("Сценарий '{}' сохранён для хаба {}", event.getName(), hubId);
    }

    @Transactional
    public void removeScenario(String hubId, String name) {
        scenarioRepository.findByHubIdAndName(hubId, name)
                .ifPresent(this::deleteScenarioWithDependencies);
        log.info("Сценарий '{}' удалён для хаба {}", name, hubId);
    }

    @Transactional
    public void addSensor(String hubId, String sensorId) {
        getOrCreateSensor(sensorId, hubId);
    }

    @Transactional
    public void removeSensor(String hubId, String sensorId) {
        sensorRepository.findByIdAndHubId(sensorId, hubId)
                .ifPresent(sensorRepository::delete);
    }

    private void deleteScenarioWithDependencies(Scenario scenario) {
        List<Long> conditionIds = scenario.getConditions().stream()
                .map(sc -> sc.getId().getConditionId()).toList();
        List<Long> actionIds = scenario.getActions().stream()
                .map(sa -> sa.getId().getActionId()).toList();

        scenarioRepository.delete(scenario);
        scenarioRepository.flush();

        conditionRepository.deleteAllById(conditionIds);
        actionRepository.deleteAllById(actionIds);
    }

    private Sensor getOrCreateSensor(String sensorId, String hubId) {
        return sensorRepository.findByIdAndHubId(sensorId, hubId)
                .orElseGet(() -> sensorRepository.save(new Sensor(sensorId, hubId)));
    }

    private static boolean isBooleanType(ConditionType type) {
        return type == ConditionType.MOTION || type == ConditionType.SWITCH;
    }

    private ScenarioAddedEventAvro toAvro(Scenario scenario) {
        List<ScenarioConditionAvro> conditions = new ArrayList<>();
        for (ScenarioCondition sc : scenario.getConditions()) {
            Boolean boolVal = sc.getCondition().getBoolValue();
            Integer intVal = sc.getCondition().getValue();
            Object conditionValue;
            if (boolVal != null) {
                conditionValue = boolVal;
            } else if (intVal != null && isBooleanType(sc.getCondition().getType())) {
                conditionValue = intVal != 0;
            } else {
                conditionValue = intVal;
            }
            conditions.add(new ScenarioConditionAvro(
                    sc.getSensor().getId(),
                    ConditionTypeAvro.valueOf(sc.getCondition().getType().name()),
                    ConditionOperationAvro.valueOf(sc.getCondition().getOperation().name()),
                    conditionValue
            ));
        }

        List<DeviceActionAvro> actions = new ArrayList<>();
        for (ScenarioAction sa : scenario.getActions()) {
            actions.add(new DeviceActionAvro(
                    sa.getSensor().getId(),
                    ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro.valueOf(sa.getAction().getType().name()),
                    sa.getAction().getValue()
            ));
        }

        return new ScenarioAddedEventAvro(scenario.getName(), conditions, actions);
    }
}
