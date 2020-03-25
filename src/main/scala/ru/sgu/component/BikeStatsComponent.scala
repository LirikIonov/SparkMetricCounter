package ru.sgu.component

import org.apache.spark.sql.{DataFrame, Dataset}
import ru.sgu.BikeStats
import ru.sgu.loader.DataLoader
import ru.sgu.logger.BaseLogger
import ru.sgu.metric.MetricCalculator

class BikeStatsComponent(loader: DataLoader, metrics: MetricCalculator) extends BaseLogger {
		lazy val currentBikeStats: Dataset[BikeStats] = log(loader.loadData(), "loading bike usage dataset...")

		lazy val bikeMetrics = log(metrics.calcAggregate(currentBikeStats), "calculating bike metrics (max, min, avg, mean, median 1, median 2)")
		lazy val topRentFromAddresses: DataFrame = log(metrics.calcTopRentAddresses(currentBikeStats, "fromStationName"), "calculating top-10 from addresses")
		lazy val topRentToAddresses: DataFrame = log(metrics.calcTopRentAddresses(currentBikeStats, "toStationName"), "calculating top-10 to addresses")
}
