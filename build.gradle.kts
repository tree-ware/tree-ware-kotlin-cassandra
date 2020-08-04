import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "org.tree-ware"
version = "1.0-SNAPSHOT"

val kotlinVersion = "1.3.72"

val cassandraDriverVersion = "4.2.0"

val log4j2Version = "2.12.1"

val junitVersion = "5.4.2"

plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.72")
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

    implementation("com.datastax.oss:java-driver-core:$cassandraDriverVersion")
    implementation("com.datastax.oss:java-driver-query-builder:$cassandraDriverVersion")

    implementation("org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion")

    implementation("org.apache.logging.log4j:log4j-api:$log4j2Version")
    implementation("org.apache.logging.log4j:log4j-core:$log4j2Version")

    testFixturesImplementation(project(":tree-ware-kotlin-core"))
    testFixturesImplementation("com.datastax.oss:java-driver-query-builder:$cassandraDriverVersion")
    testFixturesImplementation("org.jetbrains.kotlin:kotlin-test-junit5:$kotlinVersion")

    testImplementation(testFixtures(project(":tree-ware-kotlin-core")))

    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4j2Version")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5:$kotlinVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
