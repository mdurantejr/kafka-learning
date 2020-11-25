import sbt.internal.inc.JarUtils

name := "favourite-colour-scala"
organization := "com.durante.kafka.streams"
version := "1.0-SNAPSHOT"
scalaVersion := "2.13.4"

libraryDependencies ++=Seq(
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-log4j12" % "1.7.25"
)

javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")
scalacOptions := Seq("-target:jvm-11")
initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "11")
    sys.error("Java 11 is required for this project.")
}