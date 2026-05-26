package io.outboxarena.notifier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Terminal-state side-effect sink. Stub for email/SMS fanout -- writes to stdout.
@SpringBootApplication
public class NotifierApplication {

  public static void main(String[] args) {
    SpringApplication.run(NotifierApplication.class, args);
  }
}
