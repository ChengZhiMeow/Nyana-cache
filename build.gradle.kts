plugins {
    id("java")
}

subprojects {
    apply(plugin = "maven-publish")
}

allprojects {
    apply(plugin = "java")

    group = "net.nyana"
    version = "1.0.0"

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        compileOnly("org.jetbrains:annotations:24.0.0")

        testImplementation(platform("org.junit:junit-bom:5.10.2"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}

project(":redis") {
    afterEvaluate {
        val testSourceSet = extensions.getByType<SourceSetContainer>().named("test").get()
        val configureRedisTest: Test.() -> Unit = {
            group = "verification"
            description = "Runs Redis integration tests."
            testClassesDirs = testSourceSet.output.classesDirs
            classpath = testSourceSet.runtimeClasspath
            shouldRunAfter(tasks.named("test"))
            useJUnitPlatform()
            systemProperty("nyana.redis.tests", "true")
        }

        val redisTest = tasks.findByName("redisTest")
        if (redisTest is Test) redisTest.configureRedisTest()
        else tasks.register<Test>("redisTest", configureRedisTest)
    }
}