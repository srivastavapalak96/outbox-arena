package io.outboxarena.order.api;

import io.outboxarena.order.domain.Order;
import io.outboxarena.order.domain.OrderRepository;
import io.outboxarena.order.intake.CreateOrderRequest;
import io.outboxarena.order.intake.CreateOrderResponse;
import io.outboxarena.order.intake.OrderIntakeService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

  private final OrderIntakeService intake;
  private final OrderRepository orderRepository;

  public OrderController(OrderIntakeService intake, OrderRepository orderRepository) {
    this.intake = intake;
    this.orderRepository = orderRepository;
  }

  @PostMapping
  public ResponseEntity<CreateOrderResponse> create(@Valid @RequestBody CreateOrderRequest req) {
    CreateOrderResponse response = intake.intake(req);
    return ResponseEntity.created(URI.create("/orders/" + response.orderUuid())).body(response);
  }

  @GetMapping("/{orderUuid}")
  public ResponseEntity<OrderView> get(@PathVariable UUID orderUuid) {
    return orderRepository
        .findByOrderUuid(orderUuid)
        .map(OrderView::from)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  public record OrderView(
      UUID orderUuid, String buyerId, String status, long totalAmountCents, String currency) {
    static OrderView from(Order order) {
      return new OrderView(
          order.getOrderUuid(),
          order.getBuyerId(),
          order.getStatus().name(),
          order.getTotalAmountCents(),
          order.getCurrency());
    }
  }
}
