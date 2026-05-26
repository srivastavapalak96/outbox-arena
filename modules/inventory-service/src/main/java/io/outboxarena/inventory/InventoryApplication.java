package io.outboxarena.inventory;

import io.outboxarena.common.outbox.OutboxRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackageClasses = {InventoryApplication.class, OutboxRecord.class})
@EnableJpaRepositories(basePackageClasses = {InventoryApplication.class, OutboxRecord.class})
public class InventoryApplication {

  public static void main(String[] args) {
    SpringApplication.run(InventoryApplication.class, args);
  }
}
