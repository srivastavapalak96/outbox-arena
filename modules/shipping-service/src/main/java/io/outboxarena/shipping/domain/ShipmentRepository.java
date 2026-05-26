package io.outboxarena.shipping.domain;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ShipmentRepository extends JpaRepository<Shipment, Long> {

  Optional<Shipment> findByOrderUuid(UUID orderUuid);
}
