package ru.sgu.metric

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import ru.sgu.logger.BaseLogger
import ru.sgu.{BikeMetrics, BikeSet, TopCountries}
import ru.sgu.utils.DateTransform._

import scala.math.BigDecimal.RoundingMode

class MetricCalculator(implicit sparkSession: SparkSession) extends BaseLogger with Serializable {

		import org.apache.spark.sql.functions._
		import sparkSession.implicits._

		def calcAggregate(bikeSet: DStream[BikeSet]): DataFrame = {
				var result = sparkSession.emptyDataset[BikeMetrics]
				bikeSet.foreachRDD(bikeRdd => if (!bikeRdd.isEmpty()) {
						logger.info("Calculating bike metrics (max, min, avg, mean, median 1, median 2)...")
						val currentTripDurs: DataFrame = bikeRdd.map(ds => ds.tripDuration).toDF()
						val maxTripDur = currentTripDurs.sort(desc("value")).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
						val minTripDur = currentTripDurs.sort(asc("value")).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
						val avgTripDur = currentTripDurs.select(mean("value")).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
						val medianTripDur = getMedian(currentTripDurs.as[BigDecimal])

						val clientAgeSet: Dataset[Int] = bikeRdd
								.map(ds => {
										val currYear: Int = strToDate(ds.startTime).getYear
										val birthYear = Option(ds.birthYear)
										birthYear match {
												case Some(_) => currYear - Integer.parseInt(birthYear.get)
												case _ => -1
										}
								})
								.filter(m => !m.equals(-1))
    						.toDS
						val medianClientAge = getMedian(clientAgeSet.as[BigDecimal])

						result = result.union(List(BikeMetrics(maxTripDur, minTripDur, avgTripDur,
								medianTripDur, medianClientAge)).toDS)
				})
				result.toDF()
		}

		def calcTopRentAddresses(bikeSet: DStream[BikeSet], rentAddressName: String, addType: String): DataFrame = {
				var result: DataFrame = Seq.empty[TopCountries].toDF()
				bikeSet.foreachRDD(bikeRdd => if (!bikeRdd.isEmpty()) {
						logger.info(s"Calculating top-10 rent $addType addresses...")
						val topAddresses = bikeRdd.toDF().groupBy(rentAddressName)
								.count()
								.sort(desc("count"))
								.limit(10)
								.toDF()
								.drop("count")
						result = result.union(topAddresses)
				})
				result
		}

		private def getMedian(bikeSet: Dataset[BigDecimal]): BigDecimal = {
				BigDecimal.valueOf(bikeSet.stat.approxQuantile("value", Array(0.5), 0)(0))
		}
}
