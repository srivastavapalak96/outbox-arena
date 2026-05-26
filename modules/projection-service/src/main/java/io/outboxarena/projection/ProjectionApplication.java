package io.outboxarena.projection;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Pure CDC consumer. No outbox, no business logic, no command-plane subscription. The {@code
 * outbox-arena.outbox.enabled=false} in application.yml keeps the OutboxAutoConfiguration from
 * registering its beans here.
 */
@SpringBootApplication(scanBasePackages = {"io.outboxarena.projection", "io.outboxarena.common"})
public class ProjectionApplication {

  public static void main(String[] args) {
    SpringApplication.run(ProjectionApplication.class, args);
  }
}
