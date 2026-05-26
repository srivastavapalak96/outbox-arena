package io.outboxarena.order.domain;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

  Optional<Order> findByOrderUuid(UUID orderUuid);

  @EntityGraph(attributePaths = {"items"})
  @Query("select o from Order o where o.orderUuid = :orderUuid")
  Optional<Order> findWithItemsByOrderUuid(@Param("orderUuid") UUID orderUuid);
}
