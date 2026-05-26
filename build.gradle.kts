// Root build: applied across all subprojects via conventions in buildSrc/.
// Per-module customisation lives in modules/<service>/build.gradle.kts.
// Plugins (Spring Boot, Spotless, Jib) are brought in by buildSrc as classpath deps and
// applied per-module via id("conventions.service-conventions"). We intentionally do NOT
// re-declare them here to avoid double-classpath conflicts (see: BUILDSRC + plugins DSL).

allprojects {
    group = "io.outboxarena"
    version = "0.0.1-SNAPSHOT"
}

tasks.register("printVersion") {
    doLast {
        println("outbox-arena ${project.version}")
    }
}
