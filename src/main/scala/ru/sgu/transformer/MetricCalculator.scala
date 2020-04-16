package ru.sgu.transformer

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import ru.sgu.io.DataWriter
import ru.sgu.utils.DateUtils._
import ru.sgu.{BikeMetrics, BikeSet}

import scala.math.BigDecimal.RoundingMode

class MetricCalculator(dw: DataWriter)(implicit ss: SparkSession) extends Serializable with Logging {

  import org.apache.spark.sql.functions._
  import ss.implicits._

  def calcAggregate(bikeRDD: RDD[BikeSet]): Unit = {
    logger.info("Calculating bike metrics (max, min, avg, mean, median 1, median 2)...")
    val currentTripDurs: DataFrame = bikeRDD
      .map(r => r.tripDuration)
      .filter(r => r != null)
      .toDF()
    //currentTripDurs.show(20)

    val maxTripDur = currentTripDurs.sort(desc("value")).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
    val minTripDur = currentTripDurs.sort(asc("value")).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
    val avgTripDur = currentTripDurs.select(mean("value")).first().getDecimal(0).setScale(2, RoundingMode.DOWN)
    val medianTripDur = getMedian(currentTripDurs.as[BigDecimal])

    val clientAgeSet: Dataset[Int] = bikeRDD
      .filter(r => r.startTime != null)
      .filter(r => r.birthYear != null)
      .map(ds => {
        val currYear: Int = strToDate(ds.startTime).getYear
        val birthYear = Option(ds.birthYear)
        birthYear match {
          case Some(_) => Option(currYear - Integer.parseInt(birthYear.get))
          case _ => None
        }
      })
      .filter(r => r.isDefined)
      .map(r => r.get)
      .toDS

    val medianClientAge = getMedian(clientAgeSet.as[BigDecimal])
    val result: RDD[BikeMetrics] = ss.sparkContext.parallelize(List(BikeMetrics(maxTripDur, minTripDur, avgTripDur,
      medianTripDur, medianClientAge)))
    //logger.info("Metric Calculator some 5 metrics"); result.toDS.show(20)
    dw.writeToBigQuery(result.toDF(), "bike_metrics")
  }

  def calcTopRentAddresses(bikeRDD: RDD[BikeSet], rentAddressName: String, addType: String, tableName: String): Unit = {
    logger.info(s"Calculating top-10 rent $addType addresses...")
    val topAddresses = bikeRDD
      .toDF()
      .filter(col(rentAddressName).isNotNull)
      .groupBy(col(rentAddressName))
      .count()
      .sort(desc("count"))
      .limit(10)
      .select(col(rentAddressName))
    //logger.info("Top10 smh: "); topAddresses.show(20)
    dw.writeToBigQuery(topAddresses, tableName)
  }

  private def getMedian(bikeSet: Dataset[BigDecimal]): BigDecimal = {
    if (bikeSet.count() == 0) 0 else BigDecimal.valueOf(bikeSet.stat.approxQuantile("value", Array(0.5), 0)(0))
  }
}
