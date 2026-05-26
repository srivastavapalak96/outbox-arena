rootProject.name = "outbox-arena"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    // Auto-downloads JDK toolchains via the Foojay Disco API so contributors don't have to
    // pre-install Java 17. Resolver is applied at settings time, before any subproject.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

include(
    "modules:common",
    "modules:order-service",
    "modules:payment-service",
    "modules:inventory-service",
    "modules:shipping-service",
    "modules:projection-service",
    "modules:notifier-sink",
)
