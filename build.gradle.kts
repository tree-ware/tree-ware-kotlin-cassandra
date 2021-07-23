import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "org.tree-ware"
version = "1.0-SNAPSHOT"

val kotlinCoroutinesVersion = "1.5.0"

val cassandraDriverVersion = "4.8.0"

val log4j2Version = "2.14.1"

plugins {
    id("org.jetbrains.kotlin.jvm").version("1.5.21")
    id("idea")
    id("java-library")
    id("java-test-fixtures")
}

repositories {
    mavenCentral()
}

tasks.withType<KotlinCompile> {
    // Compile for Java 8 (default is Java 6)
    kotlinOptions.jvmTarget = "1.8"
}

dependencies {
    implementation(project(":tree-ware-kotlin-core"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:$kotlinCoroutinesVersion")

    api("com.datastax.oss:java-driver-core:$cassandraDriverVersion")
    api("com.datastax.oss:java-driver-query-builder:$cassandraDriverVersion")

    implementation(kotlin("stdlib"))

    implementation("org.apache.logging.log4j:log4j-api:$log4j2Version")
    implementation("org.apache.logging.log4j:log4j-core:$log4j2Version")

    testFixturesImplementation(project(":tree-ware-kotlin-core"))
    testFixturesImplementation("com.datastax.oss:java-driver-query-builder:$cassandraDriverVersion")
    testFixturesImplementation(kotlin("test"))

    testImplementation(testFixtures(project(":tree-ware-kotlin-core")))

    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4j2Version")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
