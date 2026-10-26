package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class HubRouterService {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    @Value("${grpc.client.hub-router.deadline-seconds:5}")
    private long deadlineSeconds;

    public void sendAction(String hubId, String scenarioName, DeviceActionProto action, Instant timestamp) {
        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(action)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(timestamp.getEpochSecond())
                        .setNanos(timestamp.getNano())
                        .build())
                .build();

        log.debug("Отправка действия хабу: hubId={}, сценарий={}, deviceId={}",
                hubId, scenarioName, action.getSensorId());
        hubRouterClient.withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS).handleDeviceAction(request);
    }

}
