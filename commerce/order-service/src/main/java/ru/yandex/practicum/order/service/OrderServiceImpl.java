package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemDto;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;

    @Override
    @Transactional(readOnly = true)
    public List<OrderDto> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(this::toDto)
                .toList();
    }

    @Override
    @Transactional
    public OrderDto createOrder(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerName(request.customerName());
        order.setCustomerEmail(request.customerEmail());
        order.setStatus("CREATED");
        order.setCreatedAt(LocalDateTime.now());

        List<OrderItem> items = request.items().stream()
                .map(itemReq -> {
                    OrderItem item = new OrderItem();
                    item.setProductId(itemReq.productId());
                    item.setProductName(itemReq.productName());
                    item.setQuantity(itemReq.quantity());
                    item.setPrice(itemReq.price());
                    item.setOrder(order);
                    return item;
                })
                .toList();
        order.setItems(items);

        BigDecimal total = items.stream()
                .filter(i -> i.getPrice() != null)
                .map(i -> i.getPrice().multiply(BigDecimal.valueOf(i.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        order.setTotalPrice(total);

        return toDto(orderRepository.save(order));
    }

    @Override
    @Transactional(readOnly = true)
    public OrderDto getOrderById(Long id) {
        return toDto(findOrder(id));
    }

    @Override
    @Transactional(readOnly = true)
    public List<OrderDto> getOrdersByEmail(String email) {
        return orderRepository.findByCustomerEmail(email).stream()
                .map(this::toDto)
                .toList();
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ с id=" + id + " не найден"));
    }

    private OrderDto toDto(Order order) {
        List<OrderItemDto> items = order.getItems().stream()
                .map(i -> new OrderItemDto(i.getId(), i.getProductId(), i.getProductName(), i.getQuantity(), i.getPrice()))
                .toList();
        return new OrderDto(
                order.getId(),
                order.getCustomerName(),
                order.getCustomerEmail(),
                order.getStatus(),
                order.getTotalPrice(),
                order.getStatusDetails(),
                order.getCreatedAt(),
                items
        );
    }
}
