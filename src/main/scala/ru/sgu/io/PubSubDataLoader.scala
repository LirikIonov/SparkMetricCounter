package ru.sgu.io

import java.beans.Transient
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import ru.sgu.BikeSet
import ru.sgu.spark.SparkMetricConfig
import ru.sgu.transformer.{MetricCalculator, StreamDataModifier}

class PubSubDataLoader(implicit sc: StreamingContext, config: SparkMetricConfig, ss: SparkSession) extends Serializable {
		val dw: DataWriter = new DataWriter
		val metrics: MetricCalculator = new MetricCalculator(dw)

		val projectID: String = config.projectId
		val topic: String = config.topicName
		val subcription: String = config.subscriptionName
		val windowLength: Int = config.windowLength
		val slidingInterval: Int = config.slidingInterval

		def startProcessingData(): Unit = {
				println("Loading bike set as stream from Pub/Sub")

				@Transient
				val x = PubsubUtils.createStream(
						sc,
						projectID,
						Option(topic),
						subcription,
						SparkGCPCredentials.builder.build(),
						StorageLevel.MEMORY_AND_DISK_SER_2)
						.map(message => new String(message.getData(), StandardCharsets.UTF_8))
				StreamDataModifier.process(x, dw, metrics)
		}
}
