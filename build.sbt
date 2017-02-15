name := "unittest-kafka"

organization in ThisBuild := "com.github.esmusssein"

version in ThisBuild := "0.1.0"

scalaVersion in ThisBuild := "2.11.8"

parallelExecution in Test := false
fork in Test := false

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.9.0.1" exclude("junit", "junit"),
  "org.apache.kafka" %% "kafka" % "0.9.0.1" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j") exclude ("junit", "junit"),
  "log4j" % "log4j" % "1.2.17",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "net.manub" %% "scalatest-embedded-kafka" % "0.6.1" exclude("junit", "junit"),
  "org.scalatest" %% "scalatest" % "2.2.1")
