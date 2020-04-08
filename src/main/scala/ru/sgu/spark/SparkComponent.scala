package ru.sgu.spark

import java.beans.Transient

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import ru.sgu.SparkLauncher

class SparkComponent {
		def createContext(config: SparkMetricConfig): StreamingContext = {
				val sparkConf = new SparkConf()
						.setAppName(SparkLauncher.getClass.getName)
						.setIfMissing("spark.master", "local[*]")

				@Transient
				val sparkContext: StreamingContext = new StreamingContext(sparkConf, Seconds(config.slidingInterval))

				@Transient
				val session: SparkSession = SparkSession.builder()
						.appName(SparkLauncher.getClass.getName)
						.config(sparkConf)
						.getOrCreate()

				if (config.runMode == "cluster") {
						val checkpointDirectory = config.checkpointDir
						val yarnTags = sparkConf.get("spark.yarn.tags")
						val jobId = yarnTags.split(",").filter(_.startsWith("dataproc_job_")).head
						sparkContext.checkpoint(checkpointDirectory + '/' + jobId)
				}
				else if (config.runMode == "local") {
						sparkContext.checkpoint(config.checkpointDir)
				}
				sparkContext
		}

		def createSession(implicit config: SparkMetricConfig): SparkSession = {
				@Transient
				val sparkSession: SparkSession = SparkSession.builder()
						.appName("spark")
						.getOrCreate()
				val bucket = config.bucketName
				sparkSession.conf.set("temporaryGcsBucket", bucket)
				sparkSession
		}
}
