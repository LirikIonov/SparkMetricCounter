package ru.sgu.io

import java.beans.Transient
import java.nio.charset.StandardCharsets

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import ru.sgu.spark.SparkMetricConfig
import ru.sgu.transformer.{MetricCalculator, StreamDataModifier}

class PubSubDataLoader(implicit sc: StreamingContext, config: SparkMetricConfig, ss: SparkSession) extends Serializable with Logging {
  implicit val dw = new DataWriter
  implicit val metrics = new MetricCalculator(dw)

  val projectID = config.projectId
  val topic = config.topicName
  val subcription = config.subscriptionName
  val windowLength = config.windowLength
  val slidingInterval = config.slidingInterval

  def startProcessingData(): Unit = {
    logger.info("Loading bike set as stream from Pub/Sub")

    @Transient
    val x = PubsubUtils.createStream(
      sc,
      projectID,
      Option(topic),
      subcription,
      SparkGCPCredentials.builder.build(),
      StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))
    StreamDataModifier.process(x)
  }
}
