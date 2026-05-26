package io.outboxarena.shipping;

import io.outboxarena.common.outbox.OutboxRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"io.outboxarena.shipping", "io.outboxarena.common"})
@EntityScan(basePackageClasses = {ShippingApplication.class, OutboxRecord.class})
@EnableJpaRepositories(basePackageClasses = {ShippingApplication.class, OutboxRecord.class})
public class ShippingApplication {

  public static void main(String[] args) {
    SpringApplication.run(ShippingApplication.class, args);
  }
}
