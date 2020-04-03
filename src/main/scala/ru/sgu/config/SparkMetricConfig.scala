package ru.sgu.config

import java.time.ZonedDateTime

case class SparkMetricConfig(projectId: String,
														 checkpointDir: String,
														 windowLength: Int,
														 slidingInterval: Int,
														 totalRunningTime: Int,
														 bucketName: String,
														 topicName: String,
														 subscriptionName: String,
														 tablePrefix: String,
														 runMode: String
														) {}
