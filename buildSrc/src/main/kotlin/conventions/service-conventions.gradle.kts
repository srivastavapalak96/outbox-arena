// Shared conventions for every Spring Boot service module.
// Apply with: plugins { id("conventions.service-conventions") } in modules/<service>/build.gradle.kts.

plugins {
    java
    id("org.springframework.boot")
    id("io.spring.dependency-management")
    id("com.google.cloud.tools.jib")
    id("com.diffplug.spotless")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    // Pulled by every service. Spring Boot BOM resolves versions.
    "implementation"("org.springframework.boot:spring-boot-starter-actuator")
    "implementation"("io.micrometer:micrometer-registry-prometheus")
    "implementation"("com.fasterxml.jackson.core:jackson-databind")
    "testImplementation"("org.springframework.boot:spring-boot-starter-test")
}

// Spotless: Google Java Format. Single source of truth for formatting.
spotless {
    java {
        googleJavaFormat("1.22.0")
        target("src/**/*.java")
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// Jib: multi-arch image build (arm64 + amd64) without QEMU.
// Image name resolved from the module name -- e.g. modules/order-service -> outbox-arena/order-service.
jib {
    from {
        image = "eclipse-temurin:17-jre"
        platforms {
            platform {
                architecture = "amd64"
                os = "linux"
            }
            platform {
                architecture = "arm64"
                os = "linux"
            }
        }
    }
    to {
        image = "ghcr.io/srivastavapalak96/outbox-arena/${project.name}"
        tags = setOf(project.version.toString(), "latest")
    }
    container {
        jvmFlags = listOf(
            "-XX:+UseG1GC",
            "-XX:MaxRAMPercentage=75",
            "-Djava.security.egd=file:/dev/./urandom",
        )
        ports = listOf("8080")
        labels.set(mapOf(
            "org.opencontainers.image.source" to "https://github.com/srivastavapalak96/outbox-arena",
            "org.opencontainers.image.licenses" to "Apache-2.0",
        ))
    }
}
