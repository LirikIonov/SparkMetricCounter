package ru.sgu.component

import ru.sgu.loader.PubSubDataLoader
import ru.sgu.metric.MetricCalculator

class BikeSetComponent(loader: PubSubDataLoader, metrics: MetricCalculator) {
		val currentBikeSet = loader.loadDataAsStream()
    val bikeMetrics = metrics.calcAggregate(currentBikeSet)
		val topRentFromAddresses = metrics.calcTopRentAddresses(currentBikeSet, "fromStationName", "from")
		val topRentToAddresses = metrics.calcTopRentAddresses(currentBikeSet, "toStationName", "to")
}
