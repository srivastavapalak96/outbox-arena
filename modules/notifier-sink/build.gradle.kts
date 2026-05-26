// notifier-sink: stdout sink for terminal saga states (stub for email/SMS fanout).
// No DB, no outbox. Only Kafka consumer.

plugins {
    id("service-conventions")
}

dependencies {
    implementation(project(":modules:common"))
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.kafka:spring-kafka")
}
