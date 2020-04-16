name := "SparkMetricCounter"

version := "1.0"
scalaVersion := "2.11.12"
sbtVersion := "1.3.3"

ThisBuild / useCoursier := false
resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.4" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.4" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubsub" % "2.4.0" % "provided"

libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.14.0-beta" % "provided"
libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-1.9.17" % "provided"
libraryDependencies += "com.google.cloud.bigdataoss" % "bigquery-connector" % "hadoop3-0.13.17" % "provided"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Runtime

libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "com.iheart" %% "ficus" % "1.4.3"

libraryDependencies ~= { _.map(_.exclude("javax.jms", "jms")) }
libraryDependencies ~= { _.map(_.exclude("com.sun.jdmk", "jmxtools")) }
libraryDependencies ~= { _.map(_.exclude("com.sun.jmx", "jmxri")) }
libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }

assemblyShadeRules in assembly := Seq(
		ShadeRule.rename("com.google.common.**" -> "shade.com.google.common.@1").inAll
)

mainClass in assembly := Some("ru.sgu.SparkLauncher")
assemblyJarName in assembly := s"${name.value}.jar"