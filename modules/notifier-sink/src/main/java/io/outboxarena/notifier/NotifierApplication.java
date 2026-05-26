package io.outboxarena.notifier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

/**
 * notifier-sink has no database. We exclude the JPA / DataSource autoconfigs that the common
 * module's transitive deps would otherwise activate. The outbox poller is disabled via
 * application.yml ({@code outbox-arena.outbox.enabled=false}) for the same reason.
 */
@SpringBootApplication(
    scanBasePackages = {"io.outboxarena.notifier"},
    exclude = {
      DataSourceAutoConfiguration.class,
      DataSourceTransactionManagerAutoConfiguration.class,
      HibernateJpaAutoConfiguration.class,
    })
public class NotifierApplication {

  public static void main(String[] args) {
    SpringApplication.run(NotifierApplication.class, args);
  }
}
