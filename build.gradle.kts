plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.maven.publish)
}

group = "dev.carbe"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.slf4j.api)

    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.logback.classic)
}

// Gradle 9.x: do not use java { toolchain {} } — jvmTarget is set via KotlinCompile.compilerOptions.
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
    }
}

tasks.withType<JavaCompile> {
    sourceCompatibility = "21"
    targetCompatibility = "21"
}

tasks.withType<Test> {
    useJUnitPlatform()
}

mavenPublishing {
    publishToMavenCentral(com.vanniktech.maven.publish.SonatypeHost.CENTRAL_PORTAL)
    signAllPublications()

    coordinates("dev.carbe", "lightqueue", version.toString())

    pom {
        name.set("lightqueue")
        description.set("In-memory async queue for Kotlin coroutines: workers, retry/backoff, dead-letter and overflow strategies on top of Channel.")
        url.set("https://github.com/CarbeDev/Lightqueue")
        licenses {
            license {
                name.set("MIT License")
                url.set("https://opensource.org/licenses/MIT")
            }
        }
        developers {
            developer {
                id.set("CarbeDev")
                name.set("Kevin Carbeti")
                url.set("https://github.com/CarbeDev")
            }
        }
        scm {
            url.set("https://github.com/CarbeDev/Lightqueue")
            connection.set("scm:git:git://github.com/CarbeDev/Lightqueue.git")
            developerConnection.set("scm:git:ssh://git@github.com/CarbeDev/Lightqueue.git")
        }
    }
}
