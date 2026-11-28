package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.InventoryItem;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
public class InventoryServiceImpl implements InventoryService {

    private final InventoryRepository repository;

    @Override
    @Transactional(readOnly = true)
    public List<InventoryDto> getAllInventory() {
        return repository.findAll().stream()
                .map(this::toDto)
                .toList();
    }

    @Override
    @Transactional
    public InventoryDto create(UpdateInventoryRequest request) {
        if (repository.findByProductId(request.productId()).isPresent()) {
            throw new InsufficientStockException(
                    "Складская запись для товара id=" + request.productId() + " уже существует");
        }
        InventoryItem item = new InventoryItem();
        item.setProductId(request.productId());
        item.setQuantity(request.quantity());
        item.setReservedQuantity(0);
        return toDto(repository.save(item));
    }

    @Override
    @Transactional
    public InventoryDto update(UpdateInventoryRequest request) {
        InventoryItem item = findByProductId(request.productId());
        item.setQuantity(request.quantity());
        return toDto(repository.save(item));
    }

    @Override
    @Transactional(readOnly = true)
    public InventoryDto getByProductId(Long productId) {
        return toDto(findByProductId(productId));
    }

    @Override
    @Transactional
    public ReserveResponse reserve(ReserveRequest request) {
        InventoryItem item = findByProductId(request.productId());
        int available = item.getQuantity() - item.getReservedQuantity();
        if (request.quantity() > available) {
            throw new InsufficientStockException(
                    "Недостаточно товара: запрошено " + request.quantity() + ", доступно " + available);
        }
        item.setReservedQuantity(item.getReservedQuantity() + request.quantity());
        repository.save(item);
        return new ReserveResponse(true, item.getQuantity() - item.getReservedQuantity(), null);
    }

    private InventoryItem findByProductId(Long productId) {
        return repository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Складская запись для товара id=" + productId + " не найдена"));
    }

    private InventoryDto toDto(InventoryItem item) {
        return new InventoryDto(
                item.getId(),
                item.getProductId(),
                item.getQuantity(),
                item.getReservedQuantity(),
                item.getQuantity() - item.getReservedQuantity()
        );
    }
}
