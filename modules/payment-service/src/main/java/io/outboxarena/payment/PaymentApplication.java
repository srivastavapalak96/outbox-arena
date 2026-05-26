package io.outboxarena.payment;

import io.outboxarena.common.outbox.OutboxRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackageClasses = {PaymentApplication.class, OutboxRecord.class})
@EnableJpaRepositories(basePackageClasses = {PaymentApplication.class, OutboxRecord.class})
public class PaymentApplication {

  public static void main(String[] args) {
    SpringApplication.run(PaymentApplication.class, args);
  }
}
