name := "hw"

version := "0.1"

scalaVersion := "2.12.8"

excludeDependencies += ExclusionRule("javax.ws.rs", "javax.ws.rs-api")
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1"

val kafkaVer = "2.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVer
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % kafkaVer
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
libraryDependencies += "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0.pr2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0.pr3"