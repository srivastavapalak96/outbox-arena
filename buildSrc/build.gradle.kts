plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-gradle-plugin:3.3.4")
    implementation("io.spring.gradle:dependency-management-plugin:1.1.6")
    implementation("com.google.cloud.tools:jib-gradle-plugin:3.4.4")
    implementation("com.diffplug.spotless:spotless-plugin-gradle:6.25.0")
}
