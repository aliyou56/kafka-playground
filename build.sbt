// Global / onChangedBuildSource := ReloadSourceChange
name := "kafka-playground"

version := "2.13.6"

val kafkaVersion     = "2.8.0"
val circeVersion     = "0.14.1"
val scalatestVersion = "3.2.10"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka"               % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "io.circe"         %% "circe-core"          % circeVersion,
  "io.circe"         %% "circe-generic"       % circeVersion,
  "io.circe"         %% "circe-parser"        % circeVersion,
  "org.scalatest"    %% "scalatest"           % scalatestVersion,
  "org.scalatest"    %% "scalatest"           % scalatestVersion %
    "test",
  // logging
  //"org.apache.logging.log4j" % "log4j-api"       % "2.4.1",
  //"org.apache.logging.log4j" % "log4j-core"      % "2.4.1",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
  //"org.slf4j"               %% "slf4j-api"  % "1.7.5",
)

// scalacOptions ++= Seq(
//  "-Wunused:imports"
// )
// scalacOption += "-Xfatal-warnings"
