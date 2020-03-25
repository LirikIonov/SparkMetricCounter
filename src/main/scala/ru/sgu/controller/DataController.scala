package ru.sgu.controller

import com.typesafe.scalalogging.LazyLogging
import ru.sgu.component.{BikeStatsComponent, SparkComponent}
import ru.sgu.metric.MetricCalculator

class DataController(spark: SparkComponent) extends LazyLogging {
		val bikeComponent: BikeStatsComponent = new BikeStatsComponent(spark.dataLoader, new MetricCalculator(spark.session))

		import spark.dataWriter._

		def processData(): Unit = {
				writeToGoogleCloud(bikeComponent.currentBikeStats.toDF)
				writeToBigQuery(bikeComponent.bikeMetrics.toDF)
				writeToBigQuery(bikeComponent.topRentFromAddresses.toDF)
				writeToBigQuery(bikeComponent.topRentToAddresses.toDF)
		}
}
