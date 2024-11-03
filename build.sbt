import sbt.util

name := "CW2"
scalaVersion := "2.12.15"

scalacOptions ++= Seq("-deprecation")
resolvers ++= Resolver.sonatypeOssRepos("releases")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.3",
  "junit" % "junit" % "4.10" % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

logLevel := util.Level.Error

fork := false  // Don't fork a separate JVM
outputStrategy := Some(StdoutOutput)  // Force stdout output
connectInput := true  // Connect stdin

javaOptions ++= Seq(
  "-Xms4G",
  "-Xmx8G"
)