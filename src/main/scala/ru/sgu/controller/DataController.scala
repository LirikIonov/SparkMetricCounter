package ru.sgu.controller

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import ru.sgu.component.{BikeSetComponent, SparkComponent}
import ru.sgu.config.SparkMetricConfig
import ru.sgu.loader.PubSubDataLoader
import ru.sgu.metric.MetricCalculator
import ru.sgu.writer.DataWriter

class DataController(implicit spark: SparkComponent, sc: StreamingContext, config: SparkMetricConfig, sparkSession: SparkSession) {
		val pubSubDataLoader: PubSubDataLoader = new PubSubDataLoader()
		val metrics: MetricCalculator = new MetricCalculator
		val bikeComponent: BikeSetComponent = new BikeSetComponent(pubSubDataLoader, metrics)
		val dw: DataWriter = new DataWriter

		def processData(): Unit = {
				dw.writeToStorage(bikeComponent.currentBikeSet)
				dw.writeToBigQuery(bikeComponent.bikeMetrics, "bike_metrics")
				dw.writeToBigQuery(bikeComponent.topRentFromAddresses, "top_rent_from_addresses")
				dw.writeToBigQuery(bikeComponent.topRentToAddresses, "top_rent_to_addresses")
		}
}
