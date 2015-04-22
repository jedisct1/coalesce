
name := "Coalesce"
version := "1.0"
scalaVersion := "2.10.4"
scalacOptions += "-deprecation"
resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"
