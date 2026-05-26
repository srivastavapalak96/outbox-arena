package io.outboxarena.projection;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Pure CDC consumer. No outbox. No business logic. Materialises read-side projections only.
// Note: does NOT scan io.outboxarena.common.outbox -- this service has no outbox.
@SpringBootApplication
public class ProjectionApplication {

  public static void main(String[] args) {
    SpringApplication.run(ProjectionApplication.class, args);
  }
}
