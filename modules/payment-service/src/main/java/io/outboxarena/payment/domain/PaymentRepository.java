package io.outboxarena.payment.domain;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {

  Optional<Payment> findByOrderUuid(UUID orderUuid);

  Optional<Payment> findByIdempotencyKey(UUID idempotencyKey);
}
