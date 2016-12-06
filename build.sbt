name := "dd"

version := "0.0.16"

scalaVersion := "2.10.6"

scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.0" % "provided"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.8.0" % "provided"
libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.6.0" % "provided"
libraryDependencies += "org.uaparser" % "uap-scala_2.10" % "0.1.0" % "provided"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
}
assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("com.fasterxml.jackson.**" -> "com.shaded.fasterxml.jackson.@1").inAll
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
