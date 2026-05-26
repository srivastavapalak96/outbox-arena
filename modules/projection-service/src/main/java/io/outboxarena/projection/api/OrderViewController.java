package io.outboxarena.projection.api;

import io.outboxarena.projection.domain.OrderView;
import io.outboxarena.projection.domain.OrderViewRepository;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/views/orders")
public class OrderViewController {

  private final OrderViewRepository views;

  public OrderViewController(OrderViewRepository views) {
    this.views = views;
  }

  @GetMapping("/{orderUuid}")
  public ResponseEntity<OrderViewDto> get(@PathVariable UUID orderUuid) {
    return views
        .findById(orderUuid)
        .map(OrderViewDto::from)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  public record OrderViewDto(
      UUID orderUuid,
      String buyerId,
      String status,
      long totalAmountCents,
      String currency,
      OffsetDateTime lastUpdatedAt,
      String sourceLsn) {
    static OrderViewDto from(OrderView v) {
      return new OrderViewDto(
          v.getOrderUuid(),
          v.getBuyerId(),
          v.getStatus(),
          v.getTotalAmountCents(),
          v.getCurrency(),
          v.getLastUpdatedAt(),
          v.getSourceLsn());
    }
  }
}
