package ru.yandex.practicum.inventory.service;

import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;

import java.util.List;

public interface InventoryService {

    List<InventoryDto> getAllInventory();

    InventoryDto create(UpdateInventoryRequest request);

    InventoryDto update(UpdateInventoryRequest request);

    InventoryDto getByProductId(Long productId);

    ReserveResponse reserve(ReserveRequest request);
}
