package ru.sgu.config

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
