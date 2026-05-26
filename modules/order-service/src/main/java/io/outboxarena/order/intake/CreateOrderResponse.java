package io.outboxarena.order.intake;

import io.outboxarena.order.domain.OrderStatus;
import java.util.UUID;

public record CreateOrderResponse(UUID orderUuid, OrderStatus status, long totalAmountCents) {}
