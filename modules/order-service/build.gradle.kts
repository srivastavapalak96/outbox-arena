plugins {
    id("service-conventions")
}

dependencies {
    implementation(project(":modules:common"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.postgresql:postgresql")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.awaitility:awaitility:4.2.2")
    // Testcontainers will return in a later week once 1.21.x ships docker-java with
    // Docker 29 /info compatibility. Until then integration tests run against the
    // docker-compose Postgres on localhost:5432, gated by EA_INTEGRATION=1.
}

tasks.named<Test>("test") {
    // Pass EA_INTEGRATION through to the test JVM so @EnabledIfEnvironmentVariable picks it up.
    environment("EA_INTEGRATION", System.getenv("EA_INTEGRATION") ?: "")
}
