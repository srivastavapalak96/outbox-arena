package io.outboxarena.projection;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Pure CDC consumer. No outbox. No business logic. Materialises read-side projections only.
@SpringBootApplication
public class ProjectionApplication {

  public static void main(String[] args) {
    SpringApplication.run(ProjectionApplication.class, args);
  }
}
