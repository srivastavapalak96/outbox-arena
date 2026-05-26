package io.outboxarena.common.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OutboxRecordRepository extends JpaRepository<OutboxRecord, Long> {}
