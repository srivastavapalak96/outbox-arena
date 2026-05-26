package io.outboxarena.order;

import io.outboxarena.common.outbox.OutboxRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"io.outboxarena.order", "io.outboxarena.common"})
@EntityScan(basePackageClasses = {OrderApplication.class, OutboxRecord.class})
@EnableJpaRepositories(basePackageClasses = {OrderApplication.class, OutboxRecord.class})
public class OrderApplication {

  public static void main(String[] args) {
    SpringApplication.run(OrderApplication.class, args);
  }
}
