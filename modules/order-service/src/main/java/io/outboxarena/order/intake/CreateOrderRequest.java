package io.outboxarena.order.intake;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.util.List;

public record CreateOrderRequest(
    @NotBlank @Size(max = 64) String buyerId,
    @NotBlank @Pattern(regexp = "^[A-Z]{3}$") String currency,
    @NotEmpty @Valid List<LineItem> items) {

  public record LineItem(
      @NotBlank @Size(max = 64) String sellerId,
      @NotBlank @Size(max = 64) String sku,
      @Positive int qty,
      @PositiveOrZero long unitPriceCents) {}
}
