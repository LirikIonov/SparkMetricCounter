package ru.sgu.controller

import java.beans.Transient

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import ru.sgu.io.PubSubDataLoader
import ru.sgu.spark.{SparkComponent, SparkMetricConfig, SparkMetricConfigBuilder}

class SparkController {
		implicit val sparkComponent: SparkComponent = new SparkComponent()

		def keepRunning(sc: StreamingContext, totalTime: Int): Unit = {
				sc.start()
				if (totalTime == 0) sc.awaitTermination()
				else sc.awaitTerminationOrTimeout(1000 * 60 * totalTime)
		}

		def start(args: Array[String]): Unit = {
				@Transient
				implicit val config: SparkMetricConfig = SparkMetricConfigBuilder.create()

				@Transient
				implicit val sc = StreamingContext.getOrCreate(config.checkpointDir,
						() => sparkComponent.createContext(config))
				sc.sparkContext.hadoopConfiguration
						.set("fs.file.impl", classOf[com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem].getName)
				sc.sparkContext.hadoopConfiguration
						.set("fs.AbstractFileSystem.gs.impl", classOf[com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS].getName)

				@Transient
				implicit val session: SparkSession = sparkComponent.createSession

				new PubSubDataLoader().startProcessingData()
				keepRunning(sc, config.totalRunningTime)
		}
}
