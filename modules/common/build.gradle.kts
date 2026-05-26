// Common library: outbox poller, idempotency helpers, OTel propagation, shared event DTOs.
// Plain Java library -- not a runnable Spring Boot app.

plugins {
    `java-library`
    id("io.spring.dependency-management")
    id("com.diffplug.spotless")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:3.3.4")
    }
}

dependencies {
    api("org.springframework.boot:spring-boot-starter")
    api("org.springframework.boot:spring-boot-starter-data-jpa")
    api("org.springframework.kafka:spring-kafka")
    api("org.postgresql:postgresql")
    api("org.flywaydb:flyway-core")
    api("org.flywaydb:flyway-database-postgresql")
    api("com.fasterxml.jackson.core:jackson-databind")
    api("io.micrometer:micrometer-registry-prometheus")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
}

spotless {
    java {
        googleJavaFormat("1.22.0")
        target("src/**/*.java")
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// common/ is a library, not an app -- disable Spring Boot's repackage attempt.
tasks.withType<Jar> {
    enabled = true
}
