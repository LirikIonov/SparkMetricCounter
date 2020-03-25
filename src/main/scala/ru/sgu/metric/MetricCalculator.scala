package ru.sgu.metric

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import ru.sgu.BikeStats
import ru.sgu.utils.DateTransform._

import scala.math.BigDecimal.RoundingMode

class MetricCalculator(session: SparkSession) {

		import org.apache.spark.sql.functions._
		import session.implicits._

		def calcAggregate(currentBikeStats: Dataset[BikeStats]): Dataset[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)] = {
				val currentTripDurs: DataFrame = currentBikeStats.map(ds => ds.tripDuration).toDF
				val columnName: String = "value"

				val maxTripDur = currentTripDurs.sort(desc(columnName)).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
				val minTripDur = currentTripDurs.sort(asc(columnName)).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
				val avgTripDur = currentTripDurs.select(mean(columnName)).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
				val medianTripDur = BigDecimal.valueOf(currentTripDurs.stat.approxQuantile(columnName, Array(0.5), 0)(0))

				val clientAgeSet: Dataset[Int] = currentBikeStats
						.map(ds => {
								val currYear: Int = strToDate(ds.startTime).getYear
								val birthYear = Option(ds.birthYear)
								birthYear match {
										case Some(_) => currYear - Integer.parseInt(birthYear.get)
										case _ => -1
								}
						})
						.filter(m => !m.equals(-1))
				val medianClientAge = BigDecimal.valueOf(clientAgeSet.stat.approxQuantile(columnName, Array(0.5), 0)(0))

				List((maxTripDur, minTripDur, avgTripDur, medianTripDur, medianClientAge)).toDS
		}

		def calcTopRentAddresses(currentBikeStats: Dataset[BikeStats], rentAddressName: String): DataFrame = {
				val count: String = "count"
				currentBikeStats.groupBy(rentAddressName)
						.count()
						.sort(desc(count))
						.limit(10)
						.toDF()
						.drop(count)
		}
}
