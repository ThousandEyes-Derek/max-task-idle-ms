plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:3.2.1")
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("org.slf4j:slf4j-api:1.7.36")
}

application {
    mainClass.set(project.findProperty("mainClass").toString())
}
