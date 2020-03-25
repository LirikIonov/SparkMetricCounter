name := "SparkMetricCounter"

version := "1.0"
scalaVersion := "2.12.11"
sbtVersion := "1.3.3"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.4" //% "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.4" //% "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0" //% "provided"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }

/** Assembly plugin settings */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
mainClass in assembly := Some("ru.sgu.SparkLauncher")
assemblyJarName in assembly := s"${name.value}.jar"