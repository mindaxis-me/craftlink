plugins {
    java
    id("com.gradleup.shadow") version "8.3.6"
}

group = "me.mindaxis"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:1.21.4-R0.1-SNAPSHOT")
    testImplementation("io.papermc.paper:paper-api:1.21.4-R0.1-SNAPSHOT")
    // Netty is provided by Paper at runtime; compileOnly for ChannelDuplexHandler
    compileOnly("io.netty:netty-all:4.1.97.Final")
    testImplementation("io.netty:netty-all:4.1.97.Final")
    implementation("org.java-websocket:Java-WebSocket:1.5.7")
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.shadowJar {
    archiveClassifier.set("")
    relocate("org.java_websocket", "me.mindaxis.view.lib.websocket")
}

tasks.test {
    useJUnitPlatform()
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
