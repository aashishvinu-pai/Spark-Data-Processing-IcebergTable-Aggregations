name := "nyc-taxi-iceberg"
version := "1.0"
scalaVersion := "2.13.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"      % "4.1.1" % "provided",
  "org.apache.spark" %% "spark-sql"       % "4.1.1" % "provided",
  "org.apache.iceberg" % "iceberg-spark-runtime-4.0_2.13" % "1.10.1",
  "ch.qos.logback"     % "logback-classic" % "1.5.12"
)

resolvers += "Maven Central" at "https://repo.maven.apache.org/maven2"

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("org.slf4j.**" -> "shaded.slf4j.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x if x.endsWith(".class") && x.contains("scala/") => MergeStrategy.discard
  case x if x.contains("scala/")     => MergeStrategy.discard
  case _                             => MergeStrategy.first
}
