package ru.sgu.config

import java.time.{LocalDate, ZonedDateTime}

import net.ceedubs.ficus.Ficus._
import com.typesafe.config.Config
import ru.sgu.utils.DateTransform

object SparkMetricConfigBuilder {
		def createUsingConfig(config: Config): SparkMetricConfig = {
				//val currDate: ZonedDateTime = LocalDate.now().atStartOfDay(DateTransform.zoneId)

				SparkMetricConfig(
						projectId = config.as[String]("smc.project.id"),
						checkpointDir = config.as[String]("smc.checkpoint.dir"),
						windowLength = config.as[Option[Int]]("smc.window.length").getOrElse(100),
						slidingInterval = config.as[Option[Int]]("smc.sliding.interval").getOrElse(100),
						totalRunningTime = config.as[Option[Int]]("smc.total.running.time").getOrElse(100),

						bucketName = config.as[Option[String]]("smc.bucket.name").getOrElse("defbucket"),
						topicName = config.as[String]("smc.topic.name"),
						subscriptionName = config.as[String]("smc.subscription.name"),
						tablePrefix = config.as[String]("smc.table.prefix"),
						runMode = config.as[Option[String]]("smc.run.mode").getOrElse("cluster")
				)
		}
}
