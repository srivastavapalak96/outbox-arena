// projection-service: CDC consumer only. No outbox, no business logic.
// Materialises denormalised order_views from Debezium streams.

plugins {
    id("service-conventions")
}

dependencies {
    implementation(project(":modules:common"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.postgresql:postgresql")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
}
