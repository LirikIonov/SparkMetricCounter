package ru.sgu.controller

import java.net.URL

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import ru.sgu.component.SparkComponent
import ru.sgu.config.{SparkMetricConfig, SparkMetricConfigBuilder, TypesafeConfigMerger}

class SparkController {
		implicit val sparkComponent: SparkComponent = new SparkComponent()

		def keepRunning(sc: StreamingContext, totalTime: Int): Unit = {
				sc.start()
				if (totalTime == 0) sc.awaitTermination()
				else sc.awaitTerminationOrTimeout(1000 * 60 * totalTime)
		}

		def start(args: Array[String]): Unit = {
				val confUrls: List[URL] = if (args.length > 0) args.map(u => new URL(u)).toList else List()
				val mergedConfig: Config = TypesafeConfigMerger.loadByPaths(confUrls)
				implicit val config: SparkMetricConfig = SparkMetricConfigBuilder.createUsingConfig(mergedConfig)

				implicit val sc = StreamingContext.getOrCreate(config.checkpointDir,
						() => sparkComponent.createContext(config))
				sc.sparkContext.hadoopConfiguration
						.set("fs.file.impl", classOf[com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem].getName)
				sc.sparkContext.hadoopConfiguration
						.set("fs.AbstractFileSystem.gs.impl", classOf[com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS].getName)

				implicit val session: SparkSession = sparkComponent.createSession

				new DataController().processData()
				keepRunning(sc, config.totalRunningTime)
		}
}
